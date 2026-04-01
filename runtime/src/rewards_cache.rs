use {
    crate::{
        inflation_rewards::points::{DelegatedVoteState, calculate_points},
        stake_account::StakeAccount,
        stakes::Stakes,
    },
    dashmap::{DashMap, DashSet},
    solana_account::ReadableAccount,
    solana_clock::Epoch,
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    solana_sdk_ids::stake as stake_program,
    solana_stake_interface::{stake_history::StakeHistory, state::Delegation},
    solana_vote::vote_account::VoteAccount,
    solana_vote_interface::state::VoteStateVersions,
};

#[derive(Debug, Default)]
pub struct RewardsCache {
    rewarded_epoch: Epoch,
    by_vote: DashMap<Pubkey, VoteRewardsCacheEntry, PubkeyHasherBuilder>,
    by_stake: DashMap<Pubkey, StakeRewardPoints, PubkeyHasherBuilder>,
}

#[derive(Debug, Default)]
pub struct VoteRewardsCacheEntry {
    pub stake_pubkeys: DashSet<Pubkey, PubkeyHasherBuilder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StakeRewardPoints {
    pub voter_pubkey: Pubkey,
    pub points: u128,
}

impl RewardsCache {
    pub fn new(rewarded_epoch: Epoch) -> Self {
        Self {
            rewarded_epoch,
            by_vote: DashMap::with_hasher(PubkeyHasherBuilder::default()),
            by_stake: DashMap::with_hasher(PubkeyHasherBuilder::default()),
        }
    }

    pub fn rewarded_epoch(&self) -> Epoch {
        self.rewarded_epoch
    }

    pub fn reset_for_epoch(&mut self, rewarded_epoch: Epoch) {
        *self = Self::new(rewarded_epoch);
    }

    pub fn stake_points(&self, stake_pubkey: &Pubkey) -> Option<StakeRewardPoints> {
        self.by_stake.get(stake_pubkey).map(|entry| *entry)
    }

    pub fn sum_points_for_stake_pubkeys<'a>(
        &self,
        stake_pubkeys: impl IntoIterator<Item = &'a Pubkey>,
    ) -> u128 {
        stake_pubkeys
            .into_iter()
            .map(|stake_pubkey| {
                self.stake_points(stake_pubkey)
                    .map(|entry| entry.points)
                    .unwrap_or(0)
            })
            .sum()
    }

    pub fn affected_stakes_for_vote(&self, vote_pubkey: &Pubkey) -> Vec<Pubkey> {
        self.by_vote
            .get(vote_pubkey)
            .map(|entry| {
                entry
                    .stake_pubkeys
                    .iter()
                    .map(|stake_pubkey| *stake_pubkey)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn rebuild_from_stakes(
        rewarded_epoch: Epoch,
        stakes: &Stakes<StakeAccount<Delegation>>,
        new_rate_activation_epoch: Option<Epoch>,
    ) -> Self {
        let cache = Self::new(rewarded_epoch);
        for stake_pubkey in stakes.stake_delegations().keys() {
            cache.refresh_stake_delegation(
                stake_pubkey,
                stakes,
                rewarded_epoch,
                new_rate_activation_epoch,
            );
        }
        cache
    }

    pub fn check_and_store(
        &self,
        pubkey: &Pubkey,
        account: &impl ReadableAccount,
        stakes: &Stakes<StakeAccount<Delegation>>,
        rewarded_epoch: Epoch,
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        if account.lamports() == 0 {
            self.remove_stake_delegation(pubkey);
            self.refresh_vote_account(pubkey, stakes, rewarded_epoch, new_rate_activation_epoch);
            return;
        }

        let owner = account.owner();
        if *owner == stake_program::id() {
            self.refresh_stake_delegation(
                pubkey,
                stakes,
                rewarded_epoch,
                new_rate_activation_epoch,
            );
        } else if solana_vote_program::check_id(owner)
            && VoteStateVersions::is_correct_size_and_initialized(account.data())
        {
            self.refresh_vote_account(pubkey, stakes, rewarded_epoch, new_rate_activation_epoch);
        }
    }

    fn remove_stake_delegation(&self, stake_pubkey: &Pubkey) -> Option<StakeRewardPoints> {
        let (_, old) = self.by_stake.remove(stake_pubkey)?;
        if let Some(vote_entry) = self.by_vote.get(&old.voter_pubkey) {
            vote_entry.stake_pubkeys.remove(stake_pubkey);
            let remove_vote_entry = vote_entry.stake_pubkeys.is_empty();
            drop(vote_entry);
            if remove_vote_entry {
                self.by_vote.remove(&old.voter_pubkey);
            }
        }
        Some(old)
    }

    pub fn upsert_stake_delegation_points(
        &self,
        stake_pubkey: Pubkey,
        voter_pubkey: Pubkey,
        points: u128,
    ) {
        let _ = self.remove_stake_delegation(&stake_pubkey);
        if points == 0 {
            return;
        }

        self.by_stake.insert(
            stake_pubkey,
            StakeRewardPoints {
                voter_pubkey,
                points,
            },
        );
        self.by_vote
            .entry(voter_pubkey)
            .or_insert_with(VoteRewardsCacheEntry::default)
            .stake_pubkeys
            .insert(stake_pubkey);
    }

    fn refresh_stake_delegation(
        &self,
        stake_pubkey: &Pubkey,
        stakes: &Stakes<StakeAccount<Delegation>>,
        rewarded_epoch: Epoch,
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        let Some(stake_account) = stakes.stake_delegations().get(stake_pubkey) else {
            self.remove_stake_delegation(stake_pubkey);
            return;
        };

        let voter_pubkey = stake_account.delegation().voter_pubkey;
        let points = stakes
            .vote_accounts()
            .get(&voter_pubkey)
            .map(|vote_account| {
                calculate_stake_reward_points_from_vote_account(
                    stake_account,
                    &vote_account,
                    stakes.history(),
                    new_rate_activation_epoch,
                )
            })
            .unwrap_or(0);

        self.upsert_stake_delegation_points(*stake_pubkey, voter_pubkey, points);
    }

    fn refresh_vote_account(
        &self,
        vote_pubkey: &Pubkey,
        stakes: &Stakes<StakeAccount<Delegation>>,
        rewarded_epoch: Epoch,
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        let affected_stakes = self.affected_stakes_for_vote(vote_pubkey);
        if affected_stakes.is_empty() {
            return;
        }

        for stake_pubkey in affected_stakes {
            self.refresh_stake_delegation(
                &stake_pubkey,
                stakes,
                rewarded_epoch,
                new_rate_activation_epoch,
            );
        }
    }
}

fn calculate_stake_reward_points_from_vote_account(
    stake_account: &StakeAccount<Delegation>,
    vote_account: &VoteAccount,
    stake_history: &StakeHistory,
    new_rate_activation_epoch: Option<Epoch>,
) -> u128 {
    calculate_points(
        stake_account.stake_state(),
        DelegatedVoteState::from(vote_account.vote_state_view()),
        stake_history,
        new_rate_activation_epoch,
    )
    .unwrap_or(0)
}
