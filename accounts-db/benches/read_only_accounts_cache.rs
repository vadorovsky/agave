#![feature(test)]

extern crate test;

use {
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion},
    rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng},
    solana_accounts_db::{
        accounts_db::AccountsDb, read_only_accounts_cache::ReadOnlyAccountsCache,
    },
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::Builder,
        time::{Duration, Instant},
    },
};
mod utils;

/// Numbers of reader and writer threads to bench.
const NUM_READERS_WRITERS: &[usize] = &[
    8,
    16,
    // These parameters are likely to freeze your computer, if it has less than
    // 32 cores.
    // 32, 64, 128, 256, 512, 1024,
];

fn bench_read_only_accounts_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_only_accounts_cache");
    let slot = 0;

    for num_readers_writers in NUM_READERS_WRITERS {
        let cache = Arc::new(ReadOnlyAccountsCache::new(
            AccountsDb::DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_LO,
            AccountsDb::DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_HI,
            AccountsDb::READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE,
        ));

        // Prepare initial accounts, but make sure to not fill up the cache.
        let accounts: Vec<_> = utils::accounts(1000).collect();
        let pubkeys: Vec<_> = accounts
            .iter()
            .map(|(pubkey, _)| pubkey.to_owned())
            .collect();

        // Spawn the reader threads in the background. They are reading the
        // reading the initially inserted accounts.
        let stop_threads = Arc::new(AtomicBool::new(false));
        let reader_handles = (0..*num_readers_writers).map(|i| {
            let stop_threads = Arc::clone(&stop_threads);
            let cache = Arc::clone(&cache);
            let pubkeys = pubkeys.clone();

            Builder::new()
                .name(format!("reader{i:02}"))
                .spawn({
                    move || {
                        // Continuously read random accounts.
                        let mut rng = SmallRng::seed_from_u64(i as u64);
                        while !stop_threads.load(Ordering::Relaxed) {
                            let pubkey = pubkeys.choose(&mut rng).unwrap();
                            test::black_box(cache.load(*pubkey, slot));
                        }
                    }
                })
                .unwrap()
        });

        // Spawn the writer threads in the background. Remove the stored items
        // immediately to not cause eviction.
        let slot = 1;
        let writer_handles = (0..*num_readers_writers).map(|i| {
            let stop_threads = Arc::clone(&stop_threads);
            let cache = Arc::clone(&cache);
            let accounts: Vec<_> = utils::accounts(1000).collect();

            Builder::new()
                .name(format!("writer{i:02}"))
                .spawn({
                    move || {
                        // Continuously write to already existing pubkeys.
                        let mut rng = SmallRng::seed_from_u64(100_u64.saturating_add(i as u64));
                        while !stop_threads.load(Ordering::Relaxed) {
                            let (pubkey, account) = accounts.choose(&mut rng).unwrap();
                            test::black_box(cache.store(*pubkey, slot, account.clone()));
                            test::black_box(cache.remove(*pubkey));
                        }
                    }
                })
                .unwrap()
        });

        // Benchmark the performance of loading and storing accounts in an
        // initially empty cache.
        group.bench_function(
            BenchmarkId::new("read_only_accounts_cache_store", num_readers_writers),
            |b| {
                b.iter_custom(|iters| {
                    let accounts = utils::accounts(iters as usize);
                    let mut total_time = Duration::new(0, 0);

                    for (pubkey, account) in accounts {
                        // Measure only stores.
                        let start = Instant::now();
                        test::black_box(cache.store(pubkey, slot, account));
                        total_time += start.elapsed();

                        // Remove the key to avoid overfilling the cache and
                        // evictions.
                        cache.remove(pubkey);
                    }
                    total_time
                })
            },
        );
        group.bench_function(
            BenchmarkId::new("read_only_accounts_cache_load", num_readers_writers),
            |b| {
                b.iter_custom(|iters| {
                    let accounts: Vec<_> = utils::accounts(iters as usize).collect();

                    for (pubkey, account) in accounts.iter() {
                        test::black_box(cache.store(*pubkey, slot, account.clone()));
                    }

                    let start = Instant::now();
                    for (pubkey, _) in accounts {
                        test::black_box(cache.load(pubkey, slot));
                    }

                    start.elapsed()
                })
            },
        );

        stop_threads.store(true, Ordering::Relaxed);
        for reader_handle in reader_handles {
            reader_handle.join().unwrap();
        }
        for writer_handle in writer_handles {
            writer_handle.join().unwrap();
        }
    }
}

/// Benchmarks the read-only cache eviction mechanism. It does so by performing
/// multithreaded reads and writes on a full cache. Each write triggers
/// eviction. Background reads add more contention.
fn bench_read_only_accounts_cache_eviction(c: &mut Criterion) {
    /// Number of accounts to use in the benchmark. That's the maximum number
    /// of evictions observed on mainnet validators.
    const NUM_ACCOUNTS_BENCHED: usize = 1000;

    let mut group = c.benchmark_group("read_only_accounts_cache_eviction");

    for num_readers_writers in NUM_READERS_WRITERS {
        let cache = Arc::new(ReadOnlyAccountsCache::new(
            AccountsDb::DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_LO,
            AccountsDb::DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_HI,
            AccountsDb::READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE,
        ));

        // Prepare initial accounts, enough of the to fill up the cache.
        let accounts: Vec<_> =
            utils::accounts_with_size_limit(AccountsDb::DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_HI)
                .collect();
        let pubkeys: Vec<_> = accounts
            .iter()
            .map(|(pubkey, _)| pubkey.to_owned())
            .collect();

        // Fill up the cache.
        let slot = 0;
        for (pubkey, account) in accounts.iter() {
            cache.store(*pubkey, slot, account.clone());
        }

        // Spawn the reader threads in the background. They are reading the
        // reading the initially inserted accounts.
        let stop_threads = Arc::new(AtomicBool::new(false));
        let reader_handles = (0..*num_readers_writers).map(|i| {
            let stop_threads = Arc::clone(&stop_threads);
            let cache = Arc::clone(&cache);
            let pubkeys = pubkeys.clone();

            Builder::new()
                .name(format!("reader{i:02}"))
                .spawn({
                    move || {
                        // Continuously read random accounts.
                        let mut rng = SmallRng::seed_from_u64(i as u64);
                        while !stop_threads.load(Ordering::Relaxed) {
                            let pubkey = pubkeys.choose(&mut rng).unwrap();
                            test::black_box(cache.load(*pubkey, slot));
                        }
                    }
                })
                .unwrap()
        });

        // Spawn the writer threads in the background. Prepare the accounts
        // with the same public keys and sizes as the initial ones. The
        // intention is a constant overwrite in background for additional
        // contention.
        let slot = 1;
        let writer_handles = (0..*num_readers_writers).map(|i| {
            let stop_threads = Arc::clone(&stop_threads);
            let cache = Arc::clone(&cache);
            let accounts = accounts.clone();

            Builder::new()
                .name(format!("writer{i:02}"))
                .spawn({
                    move || {
                        // Continuously write to already existing pubkeys.
                        let mut rng = SmallRng::seed_from_u64(100_u64.saturating_add(i as u64));
                        while !stop_threads.load(Ordering::Relaxed) {
                            let (pubkey, account) = accounts.choose(&mut rng).unwrap();
                            cache.store(*pubkey, slot, account.clone());
                        }
                    }
                })
                .unwrap()
        });

        // Benchmark the performance of loading and storing accounts in a
        // cache that is fully populated. This triggers eviction for each
        // write operation. Background threads introduce contention.
        group.bench_function(
            BenchmarkId::new(
                "read_only_accounts_cache_eviction_load",
                num_readers_writers,
            ),
            |b| {
                b.iter_custom(|iters| {
                    let mut rng = SmallRng::seed_from_u64(1);
                    let mut total_time = Duration::new(0, 0);

                    for _ in 0..iters {
                        let pubkey = pubkeys.choose(&mut rng).unwrap().to_owned();

                        let start = Instant::now();
                        test::black_box(cache.load(pubkey, slot));
                        total_time += start.elapsed();
                    }

                    total_time
                })
            },
        );
        group.bench_function(
            BenchmarkId::new(
                "read_only_accounts_cache_eviction_store",
                num_readers_writers,
            ),
            |b| {
                b.iter_custom(|iters| {
                    let accounts = utils::accounts(iters as usize);

                    let start = Instant::now();
                    for (pubkey, account) in accounts {
                        test::black_box(cache.store(pubkey, slot, account));
                    }

                    start.elapsed()
                })
            },
        );

        stop_threads.store(true, Ordering::Relaxed);
        for reader_handle in reader_handles {
            reader_handle.join().unwrap();
        }
        for writer_handle in writer_handles {
            writer_handle.join().unwrap();
        }
    }
}

criterion_group!(
    benches,
    bench_read_only_accounts_cache,
    bench_read_only_accounts_cache_eviction
);
criterion_main!(benches);
