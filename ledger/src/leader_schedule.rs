use {
    rand::distributions::{Distribution, WeightedIndex},
    rand_chacha::{rand_core::SeedableRng, ChaChaRng},
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    std::{collections::HashMap, convert::identity, ops::Index, sync::Arc},
};

mod identity_keyed;
mod vote_keyed;
pub use {
    identity_keyed::LeaderSchedule as IdentityKeyedLeaderSchedule,
    vote_keyed::LeaderSchedule as VoteKeyedLeaderSchedule,
};

// Used for testing
#[derive(Clone, Debug)]
pub struct FixedSchedule {
    pub leader_schedule: Arc<LeaderSchedule>,
}

/// Stake-weighted leader schedule for one epoch.
pub type LeaderSchedule = Box<dyn LeaderScheduleVariant>;

pub trait LeaderScheduleVariant:
    std::fmt::Debug + Send + Sync + Index<u64, Output = Pubkey>
{
    fn get_slot_leaders(&self) -> Box<dyn Iterator<Item = &Pubkey> + '_>;
    fn get_unrepeated_slot_leaders(&self) -> &[Pubkey];
    fn get_leader_slots_map(&self) -> &HashMap<Pubkey, Vec<usize>>;
    fn num_slots(&self) -> usize;

    /// Get the vote account address for the given epoch slot index. This is
    /// guaranteed to be Some if the leader schedule is keyed by vote account
    fn get_vote_key_at_slot_index(&self, _epoch_slot_index: usize) -> Option<&Pubkey> {
        None
    }

    fn get_leader_upcoming_slots(
        &self,
        pubkey: &Pubkey,
        offset: usize, // Starting index.
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        let index = self.get_leader_slots_map().get(pubkey);
        let num_slots = self.num_slots();

        match index {
            Some(index) if !index.is_empty() => {
                let size = index.len();
                let start_offset = index
                    .binary_search(&(offset % num_slots))
                    .unwrap_or_else(identity)
                    + offset / num_slots * size;
                // The modular arithmetic here and above replicate Index implementation
                // for LeaderSchedule, where the schedule keeps repeating endlessly.
                // The '%' returns where in a cycle we are and the '/' returns how many
                // times the schedule is repeated.
                Box::new(
                    (start_offset..=usize::MAX)
                        .map(move |k| index[k % size] + k / size * num_slots),
                )
            }
            _ => {
                // Empty iterator for pubkeys not in schedule
                #[allow(clippy::reversed_empty_ranges)]
                Box::new((1..=0).map(|_| 0))
            }
        }
    }
}

// Note: passing in zero keyed stakes will cause a panic.
fn stake_weighted_slot_leaders(
    mut keyed_stakes: Vec<(&Pubkey, u64)>,
    epoch: Epoch,
    len: u64,
    repeat: u64,
) -> Vec<Pubkey> {
    debug_assert!(
        len % repeat == 0,
        "expected `len` {len} to be divisible by `repeat` {repeat}"
    );
    sort_stakes(&mut keyed_stakes);
    let (keys, stakes): (Vec<_>, Vec<_>) = keyed_stakes.into_iter().unzip();
    let weighted_index = WeightedIndex::new(stakes).unwrap();
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    let rng = &mut ChaChaRng::from_seed(seed);
    (0..len / repeat)
        .map(|_| keys[weighted_index.sample(rng)])
        .collect()
}

fn sort_stakes(stakes: &mut Vec<(&Pubkey, u64)>) {
    // Sort first by stake. If stakes are the same, sort by pubkey to ensure a
    // deterministic result.
    // Note: Use unstable sort, because we dedup right after to remove the equal elements.
    stakes.sort_unstable_by(|(l_pubkey, l_stake), (r_pubkey, r_stake)| {
        if r_stake == l_stake {
            r_pubkey.cmp(l_pubkey)
        } else {
            r_stake.cmp(l_stake)
        }
    });

    // Now that it's sorted, we can do an O(n) dedup.
    stakes.dedup();
}

#[cfg(test)]
mod tests {
    use {super::*, itertools::Itertools, rand::Rng, std::iter::repeat_with};

    #[test]
    fn test_get_leader_upcoming_slots() {
        const NUM_SLOTS: usize = 97;
        let mut rng = rand::thread_rng();
        let pubkeys: Vec<_> = repeat_with(Pubkey::new_unique).take(4).collect();
        let schedule: Vec<_> = repeat_with(|| pubkeys[rng.gen_range(0..3)])
            .take(19)
            .collect();
        let schedule = IdentityKeyedLeaderSchedule::new_from_schedule(schedule);
        let leaders = (0..NUM_SLOTS)
            .map(|i| (schedule[i as u64], i))
            .into_group_map();
        for pubkey in &pubkeys {
            let index = leaders.get(pubkey).cloned().unwrap_or_default();
            for offset in 0..NUM_SLOTS {
                let schedule: Vec<_> = schedule
                    .get_leader_upcoming_slots(pubkey, offset)
                    .take_while(|s| *s < NUM_SLOTS)
                    .collect();
                let index: Vec<_> = index.iter().copied().skip_while(|s| *s < offset).collect();
                assert_eq!(schedule, index);
            }
        }
    }

    #[test]
    fn test_sort_stakes_basic() {
        let pubkey0 = solana_pubkey::new_rand();
        let pubkey1 = solana_pubkey::new_rand();
        let mut stakes = vec![(&pubkey0, 1), (&pubkey1, 2)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(&pubkey1, 2), (&pubkey0, 1)]);
    }

    #[test]
    fn test_sort_stakes_with_dup() {
        let pubkey0 = solana_pubkey::new_rand();
        let pubkey1 = solana_pubkey::new_rand();
        let mut stakes = vec![(&pubkey0, 1), (&pubkey1, 2), (&pubkey0, 1)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(&pubkey1, 2), (&pubkey0, 1)]);
    }

    #[test]
    fn test_sort_stakes_with_equal_stakes() {
        let pubkey0 = Pubkey::default();
        let pubkey1 = solana_pubkey::new_rand();
        let mut stakes = vec![(&pubkey0, 1), (&pubkey1, 1)];
        sort_stakes(&mut stakes);
        assert_eq!(stakes, vec![(&pubkey1, 1), (&pubkey0, 1)]);
    }
}
