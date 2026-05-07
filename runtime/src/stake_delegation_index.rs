use {
    crate::stake_account,
    imbl::HashMap as ImblHashMap,
    rayon::{
        iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator},
    },
    solana_pubkey::Pubkey,
    solana_stake_interface::state::Delegation,
    std::{
        collections::HashMap,
        sync::{Arc, RwLock, RwLockReadGuard},
    },
};

type StakeAccount = stake_account::StakeAccount<Delegation>;
pub(crate) type StakeDelegationForkId = u64;

#[derive(Clone, Debug)]
struct RootEntry {
    stake_pubkey: Pubkey,
    stake_account: Arc<StakeAccount>,
}

#[derive(Default, Debug)]
struct ForkDelta {
    changes: HashMap<Pubkey, Option<Arc<StakeAccount>>>,
}

#[derive(Default, Debug)]
struct StakeDelegationIndexInner {
    root_entries: Vec<Option<RootEntry>>,
    root_positions: HashMap<Pubkey, usize>,
    forks: HashMap<StakeDelegationForkId, ForkDelta>,
    next_fork_id: StakeDelegationForkId,
}

#[derive(Default, Debug)]
pub(crate) struct StakeDelegationIndex {
    inner: RwLock<StakeDelegationIndexInner>,
}

#[derive(Clone, Debug)]
struct FrontierEntry {
    stake_pubkey: Pubkey,
    stake_account: Arc<StakeAccount>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FrontierStakeDelegations {
    entries: Vec<FrontierEntry>,
}

#[derive(Debug)]
pub(crate) struct FrontierQuery<'a> {
    inner: RwLockReadGuard<'a, StakeDelegationIndexInner>,
    visible_root_indices: Vec<usize>,
    overlay: HashMap<Pubkey, Option<Arc<StakeAccount>>>,
    overlay_only_inserts: Vec<FrontierEntry>,
}

impl FrontierStakeDelegations {
    pub(crate) fn ordered_pubkeys(&self) -> impl Iterator<Item = &Pubkey> {
        self.entries.iter().map(|entry| &entry.stake_pubkey)
    }
}

impl<'a> FrontierQuery<'a> {
    pub(crate) fn len(&self) -> usize {
        self.visible_root_indices.len() + self.overlay_only_inserts.len()
    }

    pub(crate) fn get(&'a self, stake_pubkey: &Pubkey) -> Option<&'a StakeAccount> {
        match self.overlay.get(stake_pubkey) {
            Some(Some(stake_account)) => Some(stake_account.as_ref()),
            Some(None) => None,
            None => self
                .inner
                .root_positions
                .get(stake_pubkey)
                .and_then(|root_index| self.inner.root_entries[*root_index].as_ref())
                .map(|root_entry| root_entry.stake_account.as_ref()),
        }
    }

    pub(crate) fn par_iter(
        &'a self,
    ) -> impl IndexedParallelIterator<Item = (&'a Pubkey, &'a StakeAccount)> {
        self.visible_root_indices
            .par_iter()
            .map(move |root_index| {
                let root_entry = self.inner.root_entries[*root_index]
                    .as_ref()
                    .expect("visible rooted frontier entry must exist");
                let stake_account = self
                    .overlay
                    .get(&root_entry.stake_pubkey)
                    .and_then(|stake_account| stake_account.as_ref())
                    .map_or(root_entry.stake_account.as_ref(), Arc::as_ref);
                (&root_entry.stake_pubkey, stake_account)
            })
            .chain(
                self.overlay_only_inserts
                    .par_iter()
                    .map(|entry| (&entry.stake_pubkey, entry.stake_account.as_ref())),
            )
    }
}

impl StakeDelegationIndex {
    pub(crate) fn new(stake_delegations: &ImblHashMap<Pubkey, StakeAccount>) -> Self {
        let mut root_entries = Vec::with_capacity(stake_delegations.len());
        let mut root_positions = HashMap::with_capacity(stake_delegations.len());
        for (stake_pubkey, stake_account) in stake_delegations.iter() {
            root_positions.insert(*stake_pubkey, root_entries.len());
            root_entries.push(Some(RootEntry {
                stake_pubkey: *stake_pubkey,
                stake_account: Arc::new(stake_account.clone()),
            }));
        }
        Self {
            inner: RwLock::new(StakeDelegationIndexInner {
                root_entries,
                root_positions,
                forks: HashMap::new(),
                next_fork_id: 1,
            }),
        }
    }

    pub(crate) fn allocate_fork(&self) -> StakeDelegationForkId {
        let mut inner = self.inner.write().unwrap();
        let fork_id = inner.next_fork_id;
        inner.next_fork_id = inner.next_fork_id.saturating_add(1);
        inner.forks.entry(fork_id).or_default();
        fork_id
    }

    pub(crate) fn release_fork(&self, fork_id: StakeDelegationForkId) {
        self.inner.write().unwrap().forks.remove(&fork_id);
    }

    pub(crate) fn upsert_root(&self, stake_pubkey: Pubkey, stake_account: StakeAccount) {
        let mut inner = self.inner.write().unwrap();
        let stake_account = Arc::new(stake_account);
        if let Some(index) = inner.root_positions.get(&stake_pubkey).copied() {
            inner.root_entries[index] = Some(RootEntry {
                stake_pubkey,
                stake_account,
            });
        } else {
            let index = inner.root_entries.len();
            inner.root_positions.insert(stake_pubkey, index);
            inner.root_entries.push(Some(RootEntry {
                stake_pubkey,
                stake_account,
            }));
        }
    }

    pub(crate) fn remove_root(&self, stake_pubkey: &Pubkey) {
        let mut inner = self.inner.write().unwrap();
        if let Some(index) = inner.root_positions.remove(stake_pubkey) {
            inner.root_entries[index] = None;
        }
    }

    pub(crate) fn upsert_fork(
        &self,
        fork_id: StakeDelegationForkId,
        stake_pubkey: Pubkey,
        stake_account: StakeAccount,
    ) {
        self.inner
            .write()
            .unwrap()
            .forks
            .entry(fork_id)
            .or_default()
            .changes
            .insert(stake_pubkey, Some(Arc::new(stake_account)));
    }

    pub(crate) fn remove_fork(&self, fork_id: StakeDelegationForkId, stake_pubkey: &Pubkey) {
        self.inner
            .write()
            .unwrap()
            .forks
            .entry(fork_id)
            .or_default()
            .changes
            .insert(*stake_pubkey, None);
    }

    pub(crate) fn frontier_snapshot(
        &self,
        fork_ids_in_ancestor_order: &[StakeDelegationForkId],
    ) -> FrontierStakeDelegations {
        let inner = self.inner.read().unwrap();
        let mut overlay = HashMap::new();
        for fork_id in fork_ids_in_ancestor_order {
            if let Some(delta) = inner.forks.get(fork_id) {
                for (stake_pubkey, stake_account) in delta.changes.iter() {
                    overlay.insert(*stake_pubkey, stake_account.clone());
                }
            }
        }

        let mut entries =
            Vec::with_capacity(inner.root_positions.len().saturating_add(overlay.len()));
        for root_entry in inner.root_entries.iter().flatten() {
            match overlay.remove(&root_entry.stake_pubkey) {
                Some(Some(stake_account)) => entries.push(FrontierEntry {
                    stake_pubkey: root_entry.stake_pubkey,
                    stake_account,
                }),
                Some(None) => {}
                None => entries.push(FrontierEntry {
                    stake_pubkey: root_entry.stake_pubkey,
                    stake_account: Arc::clone(&root_entry.stake_account),
                }),
            }
        }

        let mut inserts = overlay
            .into_iter()
            .filter_map(|(stake_pubkey, maybe_stake_account)| {
                if inner.root_positions.contains_key(&stake_pubkey) {
                    return None;
                }
                maybe_stake_account.map(|stake_account| FrontierEntry {
                    stake_pubkey,
                    stake_account,
                })
            })
            .collect::<Vec<_>>();
        inserts.sort_unstable_by_key(|entry| entry.stake_pubkey);
        entries.extend(inserts);

        FrontierStakeDelegations { entries }
    }

    pub(crate) fn frontier_query(
        &self,
        fork_ids_in_ancestor_order: &[StakeDelegationForkId],
    ) -> FrontierQuery<'_> {
        let inner = self.inner.read().unwrap();
        let mut overlay = HashMap::new();
        for fork_id in fork_ids_in_ancestor_order {
            if let Some(delta) = inner.forks.get(fork_id) {
                for (stake_pubkey, stake_account) in delta.changes.iter() {
                    overlay.insert(*stake_pubkey, stake_account.clone());
                }
            }
        }

        let mut visible_root_indices = Vec::with_capacity(inner.root_positions.len());
        for (root_index, root_entry) in inner.root_entries.iter().enumerate() {
            let Some(root_entry) = root_entry.as_ref() else {
                continue;
            };
            if matches!(overlay.get(&root_entry.stake_pubkey), Some(None)) {
                continue;
            }
            visible_root_indices.push(root_index);
        }

        let mut overlay_only_inserts = overlay
            .iter()
            .filter_map(|(stake_pubkey, maybe_stake_account)| {
                if inner.root_positions.contains_key(stake_pubkey) {
                    return None;
                }
                maybe_stake_account.as_ref().map(|stake_account| FrontierEntry {
                    stake_pubkey: *stake_pubkey,
                    stake_account: Arc::clone(stake_account),
                })
            })
            .collect::<Vec<_>>();
        overlay_only_inserts.sort_unstable_by_key(|entry| entry.stake_pubkey);

        FrontierQuery {
            inner,
            visible_root_indices,
            overlay,
            overlay_only_inserts,
        }
    }

    pub(crate) fn apply_rooted_forks(&self, fork_ids_in_ancestor_order: &[StakeDelegationForkId]) {
        let mut inner = self.inner.write().unwrap();
        for fork_id in fork_ids_in_ancestor_order {
            let Some(delta) = inner.forks.remove(fork_id) else {
                continue;
            };
            for (stake_pubkey, maybe_stake_account) in delta.changes {
                match maybe_stake_account {
                    Some(stake_account) => {
                        if let Some(index) = inner.root_positions.get(&stake_pubkey).copied() {
                            inner.root_entries[index] = Some(RootEntry {
                                stake_pubkey,
                                stake_account,
                            });
                        } else {
                            let index = inner.root_entries.len();
                            inner.root_positions.insert(stake_pubkey, index);
                            inner.root_entries.push(Some(RootEntry {
                                stake_pubkey,
                                stake_account,
                            }));
                        }
                    }
                    None => {
                        if let Some(index) = inner.root_positions.remove(&stake_pubkey) {
                            inner.root_entries[index] = None;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::stakes::tests::create_stake_account,
        rayon::ThreadPoolBuilder,
        solana_pubkey::new_rand,
        solana_rent::Rent,
    };

    #[test]
    fn test_frontier_snapshot_overlays_root_and_deltas() {
        let rent = Rent::default();
        let vote_pubkey_a = new_rand();
        let vote_pubkey_b = new_rand();
        let stake_pubkey_a = new_rand();
        let stake_pubkey_b = new_rand();
        let stake_pubkey_c = new_rand();

        let root_account_a = create_stake_account(10, &vote_pubkey_a, &stake_pubkey_a, &rent);
        let root_account_b = create_stake_account(20, &vote_pubkey_b, &stake_pubkey_b, &rent);

        let root = ImblHashMap::from_iter([
            (stake_pubkey_a, StakeAccount::try_from(root_account_a).unwrap()),
            (stake_pubkey_b, StakeAccount::try_from(root_account_b).unwrap()),
        ]);
        let index = StakeDelegationIndex::new(&root);

        let fork = index.allocate_fork();
        let updated_account_a = create_stake_account(30, &vote_pubkey_b, &stake_pubkey_a, &rent);
        let inserted_account_c = create_stake_account(40, &vote_pubkey_a, &stake_pubkey_c, &rent);
        index.upsert_fork(
            fork,
            stake_pubkey_a,
            StakeAccount::try_from(updated_account_a).unwrap(),
        );
        index.remove_fork(fork, &stake_pubkey_b);
        index.upsert_fork(
            fork,
            stake_pubkey_c,
            StakeAccount::try_from(inserted_account_c).unwrap(),
        );

        let snapshot = index.frontier_snapshot(&[fork]);
        let stake_delegations = snapshot
            .entries
            .iter()
            .map(|entry| (&entry.stake_pubkey, entry.stake_account.as_ref()))
            .collect::<Vec<_>>();
        assert_eq!(stake_delegations.len(), 2);
        assert!(stake_delegations.iter().any(|(pubkey, account)| {
            **pubkey == stake_pubkey_a && account.delegation().stake == 30
        }));
        assert!(stake_delegations.iter().any(|(pubkey, account)| {
            **pubkey == stake_pubkey_c && account.delegation().stake == 40
        }));
    }

    #[test]
    fn test_frontier_query_matches_snapshot_order() {
        let rent = Rent::default();
        let vote_pubkey_a = new_rand();
        let vote_pubkey_b = new_rand();
        let stake_pubkey_a = new_rand();
        let stake_pubkey_b = new_rand();
        let stake_pubkey_c = new_rand();

        let root_account_a = create_stake_account(10, &vote_pubkey_a, &stake_pubkey_a, &rent);
        let root_account_b = create_stake_account(20, &vote_pubkey_b, &stake_pubkey_b, &rent);

        let root = ImblHashMap::from_iter([
            (stake_pubkey_a, StakeAccount::try_from(root_account_a).unwrap()),
            (stake_pubkey_b, StakeAccount::try_from(root_account_b).unwrap()),
        ]);
        let index = StakeDelegationIndex::new(&root);

        let fork = index.allocate_fork();
        let updated_account_a = create_stake_account(30, &vote_pubkey_b, &stake_pubkey_a, &rent);
        let inserted_account_c = create_stake_account(40, &vote_pubkey_a, &stake_pubkey_c, &rent);
        index.upsert_fork(
            fork,
            stake_pubkey_a,
            StakeAccount::try_from(updated_account_a).unwrap(),
        );
        index.remove_fork(fork, &stake_pubkey_b);
        index.upsert_fork(
            fork,
            stake_pubkey_c,
            StakeAccount::try_from(inserted_account_c).unwrap(),
        );

        let snapshot = index.frontier_snapshot(&[fork]);
        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let frontier_query = index.frontier_query(&[fork]);
        let query_entries = thread_pool.install(|| frontier_query.par_iter().collect::<Vec<_>>());
        let snapshot_entries = snapshot
            .entries
            .iter()
            .map(|entry| (&entry.stake_pubkey, entry.stake_account.as_ref()))
            .collect::<Vec<_>>();
        assert_eq!(query_entries, snapshot_entries);
    }
}
