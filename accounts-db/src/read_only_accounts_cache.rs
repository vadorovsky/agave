//! ReadOnlyAccountsCache used to store accounts, such as executable accounts,
//! which can be large, loaded many times, and rarely change.
use {
    dashmap::{mapref::entry::Entry, DashMap},
    log::*,
    rand::{seq::SliceRandom, thread_rng},
    solana_measure::{measure::Measure, measure_us},
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
        pubkey::Pubkey,
        timing::timestamp,
    },
    std::{
        mem::ManuallyDrop,
        sync::{
            atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    },
};

const CACHE_ENTRY_SIZE: usize =
    std::mem::size_of::<ReadOnlyAccountCacheEntry>() + 2 * std::mem::size_of::<ReadOnlyCacheKey>();

type ReadOnlyCacheKey = Pubkey;

#[derive(Debug)]
struct ReadOnlyAccountCacheEntry {
    account: AccountSharedData,
    /// 'slot' tracks when the 'account' is stored. This important for
    /// correctness. When 'loading' from the cache by pubkey+slot, we need to
    /// make sure that both pubkey and slot matches in the cache. Otherwise, we
    /// may return the wrong account.
    slot: Slot,
    /// lower bits of last timestamp when eviction queue was updated, in ms
    last_update_time: AtomicU32,
}

#[derive(Debug, Clone, Copy)]
pub struct ReadOnlyCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evicts: u64,
    pub load_us: u64,
    pub store_us: u64,
    pub evict_us: u64,
    pub evictor_wakeup_count_all: u64,
    pub evictor_wakeup_count_productive: u64,
}

#[derive(Default, Debug)]
struct AtomicReadOnlyCacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    evicts: AtomicU64,
    load_us: AtomicU64,
    store_us: AtomicU64,
    evict_us: AtomicU64,
    evictor_wakeup_count_all: AtomicU64,
    evictor_wakeup_count_productive: AtomicU64,
}

#[derive(Debug)]
pub struct ReadOnlyAccountsCache {
    cache: Arc<DashMap<ReadOnlyCacheKey, ReadOnlyAccountCacheEntry>>,
    _max_data_size_lo: usize,
    _max_data_size_hi: usize,
    data_size: Arc<AtomicUsize>,
    // read only cache does not update lru on read of an entry unless it has been at least this many ms since the last lru update
    ms_to_skip_lru_update: u32,

    // Performance statistics
    stats: Arc<AtomicReadOnlyCacheStats>,
    highest_slot_stored: AtomicU64,

    /// To the evictor goes the spoiled [sic]
    ///
    /// Evict from the cache in the background.
    evictor_thread_handle: ManuallyDrop<thread::JoinHandle<()>>,
    /// Flag to stop the evictor
    evictor_exit_flag: Arc<AtomicBool>,
}

impl ReadOnlyAccountsCache {
    pub fn new(
        max_data_size_lo: usize,
        max_data_size_hi: usize,
        evict_sample_size: usize,
        ms_to_skip_lru_update: u32,
    ) -> Self {
        assert!(max_data_size_lo <= max_data_size_hi);
        let cache = Arc::new(DashMap::default());
        let data_size = Arc::new(AtomicUsize::default());
        let stats = Arc::new(AtomicReadOnlyCacheStats::default());
        let evictor_exit_flag = Arc::new(AtomicBool::new(false));
        let evictor_thread_handle = Self::spawn_evictor(
            evictor_exit_flag.clone(),
            max_data_size_lo,
            max_data_size_hi,
            data_size.clone(),
            evict_sample_size,
            cache.clone(),
            stats.clone(),
        );

        Self {
            highest_slot_stored: AtomicU64::default(),
            _max_data_size_lo: max_data_size_lo,
            _max_data_size_hi: max_data_size_hi,
            cache,
            data_size,
            ms_to_skip_lru_update,
            stats,
            evictor_thread_handle: ManuallyDrop::new(evictor_thread_handle),
            evictor_exit_flag,
        }
    }

    /// true if pubkey is in cache at slot
    pub(crate) fn in_cache(&self, pubkey: &Pubkey, slot: Slot) -> bool {
        if let Some(entry) = self.cache.get(pubkey) {
            entry.slot == slot
        } else {
            false
        }
    }

    pub fn load(&self, pubkey: Pubkey, slot: Slot) -> Option<AccountSharedData> {
        let (account, load_us) = measure_us!({
            let mut found = None;
            if let Some(entry) = self.cache.get(&pubkey) {
                if entry.slot == slot {
                    let update_lru = entry.ms_since_last_update() >= self.ms_to_skip_lru_update;
                    if update_lru {
                        entry
                            .last_update_time
                            .store(ReadOnlyAccountCacheEntry::timestamp(), Ordering::Release);
                    }
                    let account = entry.account.clone();
                    drop(entry);
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    found = Some(account);
                }
            }

            if found.is_none() {
                self.stats.misses.fetch_add(1, Ordering::Relaxed);
            }
            found
        });
        self.stats.load_us.fetch_add(load_us, Ordering::Relaxed);
        account
    }

    fn account_size(account: &AccountSharedData) -> usize {
        CACHE_ENTRY_SIZE + account.data().len()
    }

    pub fn store(&self, pubkey: Pubkey, slot: Slot, account: AccountSharedData) {
        let measure_store = Measure::start("");
        self.highest_slot_stored.fetch_max(slot, Ordering::Release);
        let account_size = Self::account_size(&account);
        self.data_size.fetch_add(account_size, Ordering::Relaxed);
        match self.cache.entry(pubkey) {
            Entry::Vacant(entry) => {
                entry.insert(ReadOnlyAccountCacheEntry::new(account, slot));
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                let account_size = Self::account_size(&entry.account);
                self.data_size.fetch_sub(account_size, Ordering::Relaxed);
                entry.account = account;
                entry.slot = slot;
            }
        };
        let store_us = measure_store.end_as_us();
        self.stats.store_us.fetch_add(store_us, Ordering::Relaxed);
    }

    /// true if any pubkeys could have ever been stored into the cache at `slot`
    pub(crate) fn can_slot_be_in_cache(&self, slot: Slot) -> bool {
        self.highest_slot_stored.load(Ordering::Acquire) >= slot
    }

    /// remove entry if it exists.
    /// Assume the entry does not exist for performance.
    pub(crate) fn remove_assume_not_present(&self, pubkey: Pubkey) -> Option<AccountSharedData> {
        // get read lock first to see if the entry exists
        _ = self.cache.get(&pubkey)?;
        self.remove(pubkey)
    }

    pub(crate) fn remove(&self, pubkey: Pubkey) -> Option<AccountSharedData> {
        Self::do_remove(&pubkey, &self.cache, &self.data_size)
    }

    /// Removes `key` from the cache, if present, and returns the removed account
    fn do_remove(
        key: &ReadOnlyCacheKey,
        cache: &DashMap<ReadOnlyCacheKey, ReadOnlyAccountCacheEntry>,
        data_size: &AtomicUsize,
    ) -> Option<AccountSharedData> {
        let (_, entry) = cache.remove(key)?;
        let account_size = Self::account_size(&entry.account);
        data_size.fetch_sub(account_size, Ordering::Relaxed);
        Some(entry.account)
    }

    pub(crate) fn cache_len(&self) -> usize {
        self.cache.len()
    }

    pub(crate) fn data_size(&self) -> usize {
        self.data_size.load(Ordering::Relaxed)
    }

    pub(crate) fn get_and_reset_stats(&self) -> ReadOnlyCacheStats {
        let hits = self.stats.hits.swap(0, Ordering::Relaxed);
        let misses = self.stats.misses.swap(0, Ordering::Relaxed);
        let evicts = self.stats.evicts.swap(0, Ordering::Relaxed);
        let load_us = self.stats.load_us.swap(0, Ordering::Relaxed);
        let store_us = self.stats.store_us.swap(0, Ordering::Relaxed);
        let evict_us = self.stats.evict_us.swap(0, Ordering::Relaxed);
        let evictor_wakeup_count_all = self
            .stats
            .evictor_wakeup_count_all
            .swap(0, Ordering::Relaxed);
        let evictor_wakeup_count_productive = self
            .stats
            .evictor_wakeup_count_productive
            .swap(0, Ordering::Relaxed);

        ReadOnlyCacheStats {
            hits,
            misses,
            evicts,
            load_us,
            store_us,
            evict_us,
            evictor_wakeup_count_all,
            evictor_wakeup_count_productive,
        }
    }

    /// Spawns the background thread to handle evictions
    fn spawn_evictor(
        exit: Arc<AtomicBool>,
        max_data_size_lo: usize,
        max_data_size_hi: usize,
        data_size: Arc<AtomicUsize>,
        evict_sample_size: usize,
        cache: Arc<DashMap<ReadOnlyCacheKey, ReadOnlyAccountCacheEntry>>,
        stats: Arc<AtomicReadOnlyCacheStats>,
    ) -> thread::JoinHandle<()> {
        thread::Builder::new()
            .name("solAcctReadCache".to_string())
            .spawn(move || {
                info!("AccountsReadCacheEvictor has started");
                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    // We shouldn't need to evict often, so sleep to reduce the frequency.
                    // 100 ms is already four times per slot, which should be plenty.
                    thread::sleep(Duration::from_millis(100));
                    stats
                        .evictor_wakeup_count_all
                        .fetch_add(1, Ordering::Relaxed);

                    if data_size.load(Ordering::Relaxed) <= max_data_size_hi {
                        continue;
                    }
                    stats
                        .evictor_wakeup_count_productive
                        .fetch_add(1, Ordering::Relaxed);

                    let (num_evicts, evict_us) = measure_us!(Self::evict(
                        max_data_size_lo,
                        &data_size,
                        evict_sample_size,
                        &cache,
                    ));
                    stats.evicts.fetch_add(num_evicts, Ordering::Relaxed);
                    stats.evict_us.fetch_add(evict_us, Ordering::Relaxed);
                }
                info!("AccountsReadCacheEvictor has stopped");
            })
            .expect("spawn accounts read cache evictor thread")
    }

    /// Evicts entries until the cache's size is <= `target_data_size`.
    ///
    /// The algorithm used here is a sampled LRU eviction, which means picking
    /// random `evict_sample_size` elements and picking the oldest one among
    /// them. While this mechanism doesn't ensure 100% accuracy, it results in
    /// better performance.
    ///
    /// Because the cache is a sharded `DashMap`, where each shard has its own
    /// `RWLock`, pick these elements just from one random shard.
    fn evict(
        target_data_size: usize,
        data_size: &AtomicUsize,
        evict_sample_size: usize,
        cache: &DashMap<ReadOnlyCacheKey, ReadOnlyAccountCacheEntry>,
    ) -> u64 {
        let mut rng = thread_rng();

        let mut num_evicts = 0;
        while data_size.load(Ordering::Relaxed) > target_data_size {
            let shard = cache
                .shards()
                .choose(&mut rng)
                // PANICS: It's practically impossible to end up with 0
                // shards in the dashmap. The default number of shards is
                // determined here:
                // https://github.com/xacrimon/dashmap/blob/v.5.5.3/src/lib.rs#L66-L71
                // Which relies on:
                //
                // Which would return 0 only if there are 0 CPUs.
                .unwrap();
            let (key, account_size) = {
                let shard = shard.read();
                let (key, entry) = match shard
                    .iter()
                    .take(evict_sample_size)
                    .min_by_key(|(_, entry)| entry.get().last_update_time.load(Ordering::Relaxed))
                {
                    Some(entry) => entry,
                    // This can only happen if the shard is empty. In that case,
                    // continue looping.
                    None => continue,
                };
                // Make it owned, to release the read lock.
                (key.to_owned(), entry.get().account.data().len())
            };

            shard.write().remove_entry(&key);
            let account_size = CACHE_ENTRY_SIZE + account_size;
            data_size.fetch_sub(account_size, Ordering::Relaxed);
            num_evicts += 1;
        }
        num_evicts
    }
}

impl Drop for ReadOnlyAccountsCache {
    fn drop(&mut self) {
        self.evictor_exit_flag.store(true, Ordering::Relaxed);
        // SAFETY: We are dropping, so we will never use `evictor_thread_handle` again.
        let evictor_thread_handle = unsafe { ManuallyDrop::take(&mut self.evictor_thread_handle) };
        evictor_thread_handle
            .join()
            .expect("join accounts read cache evictor thread");
    }
}

impl ReadOnlyAccountCacheEntry {
    fn new(account: AccountSharedData, slot: Slot) -> Self {
        Self {
            account,
            slot,
            last_update_time: AtomicU32::new(Self::timestamp()),
        }
    }

    /// lower bits of current timestamp. We don't need higher bits and u32 packs with Index u32 in `ReadOnlyAccountCacheEntry`
    fn timestamp() -> u32 {
        timestamp() as u32
    }

    /// ms since `last_update_time` timestamp
    fn ms_since_last_update(&self) -> u32 {
        Self::timestamp().wrapping_sub(self.last_update_time.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::{seq::IteratorRandom, Rng, SeedableRng},
        rand_chacha::ChaChaRng,
        solana_sdk::account::{accounts_equal, Account, WritableAccount},
        std::{
            collections::HashMap,
            iter::repeat_with,
            sync::Arc,
            time::{Duration, Instant},
        },
        test_case::test_matrix,
    };

    impl ReadOnlyAccountsCache {
        // Evict entries, but in the foreground
        //
        // Evicting in the background is non-deterministic w.r.t. when the evictor runs,
        // which can make asserting invariants difficult in tests.
        fn evict_in_foreground(&self, evict_sample_size: usize) {
            #[allow(clippy::used_underscore_binding)]
            let target_data_size = self._max_data_size_lo;
            Self::evict(
                target_data_size,
                &self.data_size,
                evict_sample_size,
                &self.cache,
            );
        }

        /// reset the read only accounts cache
        #[cfg(feature = "dev-context-only-utils")]
        pub fn reset_for_tests(&self) {
            self.cache.clear();
            self.data_size.store(0, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_accountsdb_sizeof() {
        // size_of(arc(x)) does not return the size of x
        assert!(std::mem::size_of::<Arc<u64>>() == std::mem::size_of::<Arc<u8>>());
        assert!(std::mem::size_of::<Arc<u64>>() == std::mem::size_of::<Arc<[u8; 32]>>());
    }

    #[test]
    fn test_read_only_accounts_cache_deterministic() {
        solana_logger::setup();
        let per_account_size = CACHE_ENTRY_SIZE;
        let data_size = 100;
        let max = data_size + per_account_size;
        // Larger sample size doesn't make sense for the small amount of
        // accounts we're testing here.
        let evict_sample_size = 1;
        let cache = ReadOnlyAccountsCache::new(
            max,
            usize::MAX, // <-- do not evict in the background
            evict_sample_size,
            READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS,
        );
        let slot = 0;
        assert!(cache.load(Pubkey::default(), slot).is_none());
        assert_eq!(0, cache.cache_len());
        assert_eq!(0, cache.data_size());
        cache.remove(Pubkey::default()); // assert no panic
        let key1 = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        let key3 = Pubkey::new_unique();
        let account1 = AccountSharedData::from(Account {
            data: vec![0; data_size],
            ..Account::default()
        });
        let mut account2 = account1.clone();
        account2.checked_add_lamports(1).unwrap(); // so they compare differently
        let mut account3 = account1.clone();
        account3.checked_add_lamports(4).unwrap(); // so they compare differently

        // Store `account1`.
        cache.store(key1, slot, account1.clone());
        cache.evict_in_foreground(evict_sample_size);
        assert_eq!(100 + per_account_size, cache.data_size());
        // Due to sampled LRU eviction, we are not 100% sure whether it's the
        // `account1` which got evicted. What we know for sure is that the
        // cache should contain just one element.
        assert_eq!(1, cache.cache_len());
        let entries: Vec<_> = cache
            .cache
            .iter()
            .map(|entry| (entry.key().to_owned(), entry.value().account.to_owned()))
            .collect();
        assert_eq!(entries.len(), 1);
        let (loaded_key, loaded_account) = entries.first().unwrap();
        if *loaded_key == key1 {
            assert!(accounts_equal(loaded_account, &account1));
            // Pass a wrong slot and check that load fails.
            assert!(cache.load(key2, slot + 1).is_none());
        } else if *loaded_key == key2 {
            assert!(accounts_equal(loaded_account, &account2));
            // Pass a wrong slot and check that load fails.
            assert!(cache.load(key2, slot + 1).is_none());
        } else {
            panic!("Unexpected key: {loaded_key:?}");
        }

        // Insert `account1` again for slot+1, and assert only one entry for
        // `key1` is in the cache.
        cache.store(key1, slot + 1, account1.clone());
        assert_eq!(1, cache.cache_len());

        // Store `acocunt2`.
        cache.store(key2, slot, account2.clone());
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account2));
        cache.evict_in_foreground(evict_sample_size);
        assert_eq!(100 + per_account_size, cache.data_size());
        // Due to sampled LRU eviction, we are not 100% sure whether it's the
        // `account1` which got evicted. What we know for sure is that the
        // cache should contain just one element.
        assert_eq!(1, cache.cache_len());
        let entries: Vec<_> = cache
            .cache
            .iter()
            .map(|entry| (entry.key().to_owned(), entry.value().account.to_owned()))
            .collect();
        assert_eq!(entries.len(), 1);
        let (loaded_key, loaded_account) = entries.first().unwrap();
        if *loaded_key == key1 {
            assert!(accounts_equal(loaded_account, &account1));
        } else if *loaded_key == key2 {
            assert!(accounts_equal(loaded_account, &account2));
        } else {
            panic!("Unexpected key: {loaded_key:?}");
        }

        // Overwrite `key2` with `account1`.
        cache.store(key2, slot, account1.clone());
        cache.evict_in_foreground(evict_sample_size);
        assert_eq!(100 + per_account_size, cache.data_size());
        // Regardless of which key was evicted, both `key1` and `key2` should
        // hold `account1`.
        assert_eq!(1, cache.cache_len());
        let entries: Vec<_> = cache
            .cache
            .iter()
            .map(|entry| (entry.key().to_owned(), entry.value().account.to_owned()))
            .collect();
        assert_eq!(entries.len(), 1);
        let (_, loaded_account) = entries.first().unwrap();
        assert!(accounts_equal(loaded_account, &account1));

        cache.remove(key1);
        cache.remove(key2);
        assert_eq!(0, cache.data_size());
        assert_eq!(0, cache.cache_len());

        // Can store 2 items, 3rd item kicks the oldest item out.
        let max = (data_size + per_account_size) * 2;
        let cache = ReadOnlyAccountsCache::new(
            max,
            usize::MAX, // <-- do not evict in the background
            evict_sample_size,
            READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS,
        );
        cache.store(key1, slot, account1.clone());
        cache.evict_in_foreground(evict_sample_size);
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(key1, slot).unwrap(), &account1));
        assert_eq!(1, cache.cache_len());
        cache.store(key2, slot, account2.clone());
        cache.evict_in_foreground(evict_sample_size);
        assert_eq!(max, cache.data_size());
        assert!(accounts_equal(&cache.load(key1, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account2));
        assert_eq!(2, cache.cache_len());
        cache.store(key2, slot, account1.clone()); // overwrite key2 with account1
        cache.evict_in_foreground(evict_sample_size);
        assert_eq!(max, cache.data_size());
        assert!(accounts_equal(&cache.load(key1, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account1));
        assert_eq!(2, cache.cache_len());
        cache.store(key3, slot, account3.clone());
        cache.evict_in_foreground(evict_sample_size);
        assert_eq!(max, cache.data_size());

        assert_eq!(2, cache.cache_len());
        let entries: Vec<_> = cache
            .cache
            .iter()
            .map(|entry| (entry.key().to_owned(), entry.value().account.to_owned()))
            .collect();
        assert_eq!(entries.len(), 2);
        for (loaded_key, loaded_account) in entries {
            if loaded_key == key1 {
                assert!(accounts_equal(&loaded_account, &account1));
            } else if loaded_key == key2 {
                // `key2` was overwritten with `account1`.
                assert!(accounts_equal(&loaded_account, &account1));
            } else if loaded_key == key3 {
                assert!(accounts_equal(&loaded_account, &account3));
            } else {
                panic!("Unexpected key: {loaded_key:?}");
            }
        }
    }

    /// tests like to deterministically update lru always
    const READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS: u32 = 0;

    #[test_matrix([1, 2, 4, 8, 10, 16, 32])]
    fn test_read_only_accounts_cache_random(evict_sample_size: usize) {
        const SEED: [u8; 32] = [0xdb; 32];
        const DATA_SIZE: usize = 19;
        const MAX_CACHE_SIZE: usize = 17 * (CACHE_ENTRY_SIZE + DATA_SIZE);
        let mut rng = ChaChaRng::from_seed(SEED);
        let cache = ReadOnlyAccountsCache::new(
            MAX_CACHE_SIZE,
            usize::MAX, // <-- do not evict in the background
            evict_sample_size,
            READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS,
        );
        let slots: Vec<Slot> = repeat_with(|| rng.gen_range(0..1000)).take(5).collect();
        let pubkeys: Vec<Pubkey> = repeat_with(|| {
            let mut arr = [0u8; 32];
            rng.fill(&mut arr[..]);
            Pubkey::new_from_array(arr)
        })
        .take(35)
        .collect();
        let mut hash_map = HashMap::<ReadOnlyCacheKey, (AccountSharedData, Slot, usize)>::new();
        for ix in 0..1000 {
            if rng.gen_bool(0.1) {
                let element = cache.cache.iter().choose(&mut rng).unwrap();
                let (pubkey, entry) = element.pair();
                let slot = entry.slot;
                let account = cache.load(*pubkey, slot).unwrap();
                let (other, other_slot, index) = hash_map.get_mut(pubkey).unwrap();
                assert_eq!(account, *other);
                assert_eq!(slot, *other_slot);
                *index = ix;
            } else {
                let mut data = vec![0u8; DATA_SIZE];
                rng.fill(&mut data[..]);
                let account = AccountSharedData::from(Account {
                    lamports: rng.gen(),
                    data,
                    executable: rng.gen(),
                    rent_epoch: rng.gen(),
                    owner: Pubkey::default(),
                });
                let slot = *slots.choose(&mut rng).unwrap();
                let pubkey = *pubkeys.choose(&mut rng).unwrap();
                hash_map.insert(pubkey, (account.clone(), slot, ix));
                cache.store(pubkey, slot, account);
                cache.evict_in_foreground(evict_sample_size);
            }
        }
        assert_eq!(cache.cache_len(), 17);
        assert_eq!(hash_map.len(), 35);
        // Ensure that all the cache entries hold information consistent with
        // what we accumulated in the local hash map.
        // Note that the opposite assertion (checking that all entries from the
        // local hash map exist in the cache) wouldn't work, because of sampled
        // LRU eviction.
        for entry in cache.cache.iter() {
            let pubkey = entry.key();
            let ReadOnlyAccountCacheEntry { account, slot, .. } = entry.value();

            let (local_account, local_slot, _) = hash_map
                .get(pubkey)
                .expect("account to be present in the map");
            assert_eq!(account, local_account);
            assert_eq!(slot, local_slot);
        }
    }

    #[test_matrix([1, 2, 4, 8, 10, 16, 32])]
    fn test_evict_in_background(evict_sample_size: usize) {
        const ACCOUNT_DATA_SIZE: usize = 200;
        const MAX_ENTRIES: usize = 7;
        const MAX_CACHE_SIZE: usize = MAX_ENTRIES * (CACHE_ENTRY_SIZE + ACCOUNT_DATA_SIZE);
        let cache = ReadOnlyAccountsCache::new(
            MAX_CACHE_SIZE,
            MAX_CACHE_SIZE,
            evict_sample_size,
            READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS,
        );

        for i in 0..MAX_ENTRIES {
            let pubkey = Pubkey::new_unique();
            let account = AccountSharedData::new(i as u64, ACCOUNT_DATA_SIZE, &Pubkey::default());
            cache.store(pubkey, i as Slot, account);
        }
        // we haven't exceeded the max cache size yet, so no evictions should've happened
        assert_eq!(cache.cache_len(), MAX_ENTRIES);
        assert_eq!(cache.data_size(), MAX_CACHE_SIZE);
        assert_eq!(cache.stats.evicts.load(Ordering::Relaxed), 0);

        // store another account to trigger evictions
        let slot = MAX_ENTRIES as Slot;
        let pubkey = Pubkey::new_unique();
        let account = AccountSharedData::new(42, ACCOUNT_DATA_SIZE, &Pubkey::default());
        cache.store(pubkey, slot, account.clone());

        // wait for the evictor to run...
        let timer = Instant::now();
        while cache.stats.evicts.load(Ordering::Relaxed) == 0 {
            assert!(
                timer.elapsed() < Duration::from_secs(5),
                "timed out waiting for the evictor to run",
            );
            thread::sleep(Duration::from_millis(1));
        }

        // ...now ensure the cache size is right
        assert_eq!(cache.cache_len(), MAX_ENTRIES);
        assert_eq!(cache.data_size(), MAX_CACHE_SIZE);
    }
}
