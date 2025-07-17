use {
    super::{
        epoch_rewards_hasher::hash_rewards_into_partitions, Bank,
        CalculateRewardsAndDistributeVoteRewardsResult, CalculateValidatorRewardsResult,
        EpochRewardCalculateParamInfo, PartitionedRewardsCalculation, PartitionedStakeReward,
        StakeRewardCalculation, VoteRewardsAccounts, REWARD_CALCULATION_NUM_BLOCKS,
    },
    crate::{
        bank::{
            PrevEpochInflationRewards, RewardCalcTracer, RewardCalculationEvent, RewardsMetrics,
            VoteReward,
        },
        inflation_rewards::{
            points::{calculate_points, PointValue},
            redeem_rewards,
        },
        stake_account::StakeAccount,
        stakes::Stakes,
    },
    ahash::RandomState,
    log::{debug, info},
    rayon::{
        iter::{IntoParallelRefIterator, ParallelIterator},
        ThreadPool,
    },
    solana_account::ReadableAccount,
    solana_clock::{Epoch, Slot},
    solana_measure::measure_us,
    solana_pubkey::Pubkey,
    solana_stake_interface::state::Delegation,
    solana_sysvar::epoch_rewards::EpochRewards,
    solana_vote::vote_account::VoteAccount,
    solana_vote_program::vote_state::VoteStateVersions,
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
            Arc,
        },
    },
};

impl Bank {
    /// Begin the process of calculating and distributing rewards.
    /// This process can take multiple slots.
    pub(in crate::bank) fn begin_partitioned_rewards(
        &mut self,
        reward_calc_tracer: Option<impl Fn(&RewardCalculationEvent) + Send + Sync>,
        thread_pool: &ThreadPool,
        parent_epoch: Epoch,
        parent_slot: Slot,
        parent_block_height: u64,
        rewards_metrics: &mut RewardsMetrics,
    ) {
        let CalculateRewardsAndDistributeVoteRewardsResult {
            distributed_rewards,
            point_value,
            stake_rewards,
        } = self.calculate_rewards_and_distribute_vote_rewards(
            parent_epoch,
            reward_calc_tracer,
            thread_pool,
            rewards_metrics,
        );

        let slot = self.slot();
        let distribution_starting_block_height =
            self.block_height() + REWARD_CALCULATION_NUM_BLOCKS;

        let num_partitions = self.get_reward_distribution_num_blocks(&stake_rewards);

        self.set_epoch_reward_status_calculation(distribution_starting_block_height, stake_rewards);

        self.create_epoch_rewards_sysvar(
            distributed_rewards,
            distribution_starting_block_height,
            num_partitions,
            point_value,
        );

        datapoint_info!(
            "epoch-rewards-status-update",
            ("start_slot", slot, i64),
            ("calculation_block_height", self.block_height(), i64),
            ("active", 1, i64),
            ("parent_slot", parent_slot, i64),
            ("parent_block_height", parent_block_height, i64),
        );
    }

    // Calculate rewards from previous epoch and distribute vote rewards
    fn calculate_rewards_and_distribute_vote_rewards(
        &self,
        prev_epoch: Epoch,
        reward_calc_tracer: Option<impl Fn(&RewardCalculationEvent) + Send + Sync>,
        thread_pool: &ThreadPool,
        metrics: &mut RewardsMetrics,
    ) -> CalculateRewardsAndDistributeVoteRewardsResult {
        // We hold the lock here for the epoch rewards calculation cache to prevent
        // rewards computation across multiple forks simultaneously. This aligns with
        // how banks are currently created- all banks are created sequentially.
        // As such, this lock does not actually introduce contention because bank
        // creation (and therefore reward calculation) is always done sequentially.
        //
        // However, if we plan to support creating banks in parallel in the future, this logic
        // would need to change to allow rewards computation on multiple forks concurrently.
        // That said, there's still a compelling reason to keep this lock even in a parallel
        // bank creation model: we want to avoid calculating rewards multiple times for the same
        // parent bank hash. This lock ensures that.
        //
        // Creating bank for multiple forks in parallel would also introduce contention for compute resources,
        // potentially slowing down the performance of both forks. This, in turn, could delay
        // vote propagation and consensus for the leading fork—the one most likely to become rooted.
        //
        // Therefore, it seems beneficial to continue processing forks sequentially at epoch
        // boundaries: acquire the lock for the first fork, compute rewards, and let other forks
        // wait until the computation is complete.
        let mut epoch_rewards_calculation_cache =
            self.epoch_rewards_calculation_cache.lock().unwrap();
        let rewards_calculation = epoch_rewards_calculation_cache
            .entry(self.parent_hash)
            .or_insert_with(|| {
                let calculation = self.calculate_rewards_for_partitioning(
                    prev_epoch,
                    reward_calc_tracer,
                    thread_pool,
                    metrics,
                );
                info!(
                    "calculated rewards for epoch: {}, parent_slot: {}, parent_hash: {}",
                    self.epoch, self.parent_slot, self.parent_hash
                );
                Arc::new(calculation)
            })
            .clone();
        drop(epoch_rewards_calculation_cache);

        let PartitionedRewardsCalculation {
            vote_account_rewards,
            stake_rewards,
            validator_rate,
            foundation_rate,
            prev_epoch_duration_in_years,
            capitalization,
            point_value,
        } = rewards_calculation.as_ref();

        let total_vote_rewards = vote_account_rewards.total_vote_rewards_lamports;
        self.store_vote_accounts_partitioned(vote_account_rewards, metrics);

        // update reward history of JUST vote_rewards, stake_rewards is vec![] here
        self.update_reward_history(vec![], &vote_account_rewards.rewards[..]);

        let StakeRewardCalculation {
            stake_rewards,
            total_stake_rewards_lamports,
        } = stake_rewards;

        // verify that we didn't pay any more than we expected to
        assert!(point_value.rewards >= total_vote_rewards + total_stake_rewards_lamports);
        info!(
            "distributed vote rewards: {} out of {}, remaining {}",
            total_vote_rewards, point_value.rewards, total_stake_rewards_lamports
        );

        let (num_stake_accounts, num_vote_accounts) = {
            let stakes = self.stakes_cache.stakes();
            (
                stakes.stake_delegations().len(),
                stakes.vote_accounts().len(),
            )
        };
        self.capitalization.fetch_add(total_vote_rewards, Relaxed);

        let active_stake = if let Some(stake_history_entry) =
            self.stakes_cache.stakes().history().get(prev_epoch)
        {
            stake_history_entry.effective
        } else {
            0
        };

        datapoint_info!(
            "epoch_rewards",
            ("slot", self.slot, i64),
            ("epoch", prev_epoch, i64),
            ("validator_rate", *validator_rate, f64),
            ("foundation_rate", *foundation_rate, f64),
            (
                "epoch_duration_in_years",
                *prev_epoch_duration_in_years,
                f64
            ),
            ("validator_rewards", total_vote_rewards, i64),
            ("active_stake", active_stake, i64),
            ("pre_capitalization", *capitalization, i64),
            ("post_capitalization", self.capitalization(), i64),
            ("num_stake_accounts", num_stake_accounts, i64),
            ("num_vote_accounts", num_vote_accounts, i64),
        );

        CalculateRewardsAndDistributeVoteRewardsResult {
            distributed_rewards: total_vote_rewards,
            point_value: point_value.clone(),
            stake_rewards: Arc::clone(stake_rewards),
        }
    }

    fn store_vote_accounts_partitioned(
        &self,
        vote_account_rewards: &VoteRewardsAccounts,
        metrics: &RewardsMetrics,
    ) {
        let (_, measure_us) = measure_us!({
            self.store_accounts((self.slot(), &vote_account_rewards.accounts_to_store[..]));
        });

        metrics
            .store_vote_accounts_us
            .fetch_add(measure_us, Relaxed);
    }

    /// Calculate rewards from previous epoch to prepare for partitioned distribution.
    pub(super) fn calculate_rewards_for_partitioning(
        &self,
        prev_epoch: Epoch,
        reward_calc_tracer: Option<impl Fn(&RewardCalculationEvent) + Send + Sync>,
        thread_pool: &ThreadPool,
        metrics: &mut RewardsMetrics,
    ) -> PartitionedRewardsCalculation {
        let capitalization = self.capitalization();
        let PrevEpochInflationRewards {
            validator_rewards,
            prev_epoch_duration_in_years,
            validator_rate,
            foundation_rate,
        } = self.calculate_previous_epoch_inflation_rewards(capitalization, prev_epoch);

        let CalculateValidatorRewardsResult {
            vote_rewards_accounts: vote_account_rewards,
            stake_reward_calculation: stake_rewards,
            point_value,
        } = self
            .calculate_validator_rewards(
                prev_epoch,
                validator_rewards,
                reward_calc_tracer,
                thread_pool,
                metrics,
            )
            .unwrap_or_default();

        PartitionedRewardsCalculation {
            vote_account_rewards,
            stake_rewards,
            validator_rate,
            foundation_rate,
            prev_epoch_duration_in_years,
            capitalization,
            point_value,
        }
    }

    /// Calculate epoch reward and return vote and stake rewards.
    fn calculate_validator_rewards(
        &self,
        rewarded_epoch: Epoch,
        rewards: u64,
        reward_calc_tracer: Option<impl RewardCalcTracer>,
        thread_pool: &ThreadPool,
        metrics: &mut RewardsMetrics,
    ) -> Option<CalculateValidatorRewardsResult> {
        let stakes = self.stakes_cache.stakes();
        let reward_calculate_param = self.get_epoch_reward_calculate_param_info(&stakes);

        self.calculate_reward_points_partitioned(
            &reward_calculate_param,
            rewards,
            thread_pool,
            metrics,
        )
        .map(|point_value| {
            let (vote_rewards_accounts, stake_reward_calculation) = self
                .calculate_stake_vote_rewards(
                    &reward_calculate_param,
                    rewarded_epoch,
                    point_value.clone(),
                    reward_calc_tracer,
                    metrics,
                );
            CalculateValidatorRewardsResult {
                vote_rewards_accounts,
                stake_reward_calculation,
                point_value,
            }
        })
    }

    /// calculate and return some reward calc info to avoid recalculation across functions
    fn get_epoch_reward_calculate_param_info<'a>(
        &'a self,
        stakes: &'a Stakes<StakeAccount<Delegation>>,
    ) -> EpochRewardCalculateParamInfo<'a> {
        // Use `stakes` for stake-related info
        let stake_history = stakes.history().clone();
        let stake_delegations = self.filter_stake_delegations(stakes);

        // Use `EpochStakes` for vote accounts
        let leader_schedule_epoch = self.epoch_schedule().get_leader_schedule_epoch(self.slot());
        let cached_vote_accounts = self.epoch_stakes(leader_schedule_epoch)
            .expect("calculation should always run after Bank::update_epoch_stakes(leader_schedule_epoch)")
            .stakes()
            .vote_accounts();

        EpochRewardCalculateParamInfo {
            stake_history,
            stake_delegations,
            cached_vote_accounts,
        }
    }

    /// Calculates epoch rewards for stake/vote accounts
    /// Returns vote rewards, stake rewards, and the sum of all stake rewards in lamports
    fn calculate_stake_vote_rewards(
        &self,
        reward_calculate_params: &EpochRewardCalculateParamInfo,
        rewarded_epoch: Epoch,
        point_value: PointValue,
        reward_calc_tracer: Option<impl RewardCalcTracer>,
        metrics: &mut RewardsMetrics,
    ) -> (VoteRewardsAccounts, StakeRewardCalculation) {
        let EpochRewardCalculateParamInfo {
            stake_history,
            stake_delegations,
            cached_vote_accounts,
            ..
        } = reward_calculate_params;

        let new_warmup_cooldown_rate_epoch = self.new_warmup_cooldown_rate_epoch();

        let vote_account_rewards_len = Arc::new(AtomicUsize::default());
        let total_stake_rewards = Arc::new(AtomicU64::default());
        let reward_calc_tracer = Arc::new(reward_calc_tracer);
        const ASSERT_STAKE_CACHE: bool = false; // Turn this on to assert that all vote accounts are in the cache

        let num_workers = num_cpus::get();

        // Collections gathered in the pool.
        let mut stake_rewards =
            vec![Vec::with_capacity(stake_delegations.len() / num_workers); num_workers];
        let mut vote_account_rewards: Vec<_> = (0..num_workers)
            .map(|_| HashMap::with_hasher(RandomState::new()))
            .collect();

        // Paired references of the collections above.
        let mut worker_states: Vec<_> = stake_rewards
            .iter_mut()
            .zip(vote_account_rewards.iter_mut())
            .collect();

        let measure_stake_rewards_us =
            crate::thread_pool::scope_with("solStkRwrds", &mut worker_states, |s| {
                let (_, measure_stake_rewards_us) =
                    measure_us!(for (stake_pubkey, stake_account) in stake_delegations {
                        let point_value = point_value.clone();
                        let vote_account_rewards_len = Arc::clone(&vote_account_rewards_len);
                        let total_stake_rewards = Arc::clone(&total_stake_rewards);
                        let reward_calc_tracer = Arc::clone(&reward_calc_tracer);

                        s.spawn(move |(stake_rewards, vote_account_rewards)| {
                            // curry closure to add the contextual stake_pubkey
                            let reward_calc_tracer =
                                reward_calc_tracer.as_ref().as_ref().map(|outer| {
                                    // inner
                                    move |inner_event: &_| {
                                        outer(&RewardCalculationEvent::Staking(
                                            stake_pubkey,
                                            inner_event,
                                        ))
                                    }
                                });

                            let stake_pubkey = **stake_pubkey;
                            let vote_pubkey = stake_account.delegation().voter_pubkey;
                            let vote_account_from_cache = cached_vote_accounts.get(&vote_pubkey);
                            if ASSERT_STAKE_CACHE && vote_account_from_cache.is_none() {
                                let account_from_db =
                                    self.get_account_with_fixed_root(&vote_pubkey);
                                if let Some(account_from_db) = account_from_db {
                                    if VoteStateVersions::is_correct_size_and_initialized(
                                        account_from_db.data(),
                                    ) && VoteAccount::try_from(account_from_db.clone()).is_ok()
                                    {
                                        panic!(
                                        "Vote account {} not found in cache, but found in db: {:?}",
                                        vote_pubkey, account_from_db
                                    );
                                    }
                                }
                            }
                            let Some(vote_account) = vote_account_from_cache else {
                                return;
                            };
                            let vote_state_view = vote_account.vote_state_view();
                            let mut stake_state = *stake_account.stake_state();

                            let redeemed = redeem_rewards(
                                rewarded_epoch,
                                &mut stake_state,
                                vote_state_view,
                                &point_value,
                                stake_history,
                                reward_calc_tracer.as_ref(),
                                new_warmup_cooldown_rate_epoch,
                            );

                            if let Ok((stakers_reward, voters_reward)) = redeemed {
                                let commission = vote_state_view.commission();

                                // track voter rewards
                                let voters_reward_entry =
                                    vote_account_rewards.entry(vote_pubkey).or_insert_with(|| {
                                        vote_account_rewards_len.fetch_add(1, Relaxed);
                                        VoteReward {
                                            commission,
                                            vote_account: vote_account.into(),
                                            vote_rewards: 0,
                                        }
                                    });

                                voters_reward_entry.vote_rewards = voters_reward_entry
                                    .vote_rewards
                                    .saturating_add(voters_reward);

                                total_stake_rewards.fetch_add(stakers_reward, Relaxed);

                                // Safe to unwrap because all stake_delegations are type
                                // StakeAccount<Delegation>, which will always only wrap
                                // a `StakeStateV2::Stake` variant.
                                let stake = stake_state.stake().unwrap();
                                stake_rewards.push(PartitionedStakeReward {
                                    stake_pubkey,
                                    stake_reward: stakers_reward,
                                    stake,
                                    commission,
                                });
                            } else {
                                debug!(
                                    "redeem_rewards() failed for {}: {:?}",
                                    stake_pubkey, redeemed
                                );
                            }
                        });
                    });
                measure_stake_rewards_us
            });

        let vote_account_rewards_len = vote_account_rewards_len.load(Relaxed);

        let stake_rewards = stake_rewards.into_iter().flatten().collect();
        let (vote_rewards, measure_vote_rewards_us) = measure_us!(
            Self::calc_vote_accounts_to_store(vote_account_rewards, vote_account_rewards_len)
        );

        metrics.redeem_rewards_us += measure_stake_rewards_us + measure_vote_rewards_us;

        (
            vote_rewards,
            StakeRewardCalculation {
                stake_rewards: Arc::new(stake_rewards),
                total_stake_rewards_lamports: total_stake_rewards.load(Relaxed),
            },
        )
    }

    /// Calculates epoch reward points from stake/vote accounts.
    /// Returns reward lamports and points for the epoch or none if points == 0.
    fn calculate_reward_points_partitioned(
        &self,
        reward_calculate_params: &EpochRewardCalculateParamInfo,
        rewards: u64,
        thread_pool: &ThreadPool,
        metrics: &RewardsMetrics,
    ) -> Option<PointValue> {
        let EpochRewardCalculateParamInfo {
            stake_history,
            stake_delegations,
            cached_vote_accounts,
            ..
        } = reward_calculate_params;

        let solana_vote_program: Pubkey = solana_vote_program::id();
        let new_warmup_cooldown_rate_epoch = self.new_warmup_cooldown_rate_epoch();
        let (points, measure_us) = measure_us!(thread_pool.install(|| {
            stake_delegations
                .par_iter()
                .map(|(_stake_pubkey, stake_account)| {
                    let vote_pubkey = stake_account.delegation().voter_pubkey;

                    let Some(vote_account) = cached_vote_accounts.get(&vote_pubkey) else {
                        return 0;
                    };
                    if vote_account.owner() != &solana_vote_program {
                        return 0;
                    }

                    calculate_points(
                        stake_account.stake_state(),
                        vote_account.vote_state_view(),
                        stake_history,
                        new_warmup_cooldown_rate_epoch,
                    )
                    .unwrap_or(0)
                })
                .sum::<u128>()
        }));
        metrics.calculate_points_us.fetch_add(measure_us, Relaxed);

        (points > 0).then_some(PointValue { rewards, points })
    }

    /// If rewards are active, recalculates partitioned stake rewards and stores
    /// a new Bank::epoch_reward_status. This method assumes that vote rewards
    /// have already been calculated and delivered, and *only* recalculates
    /// stake rewards
    pub(in crate::bank) fn recalculate_partitioned_rewards(
        &mut self,
        reward_calc_tracer: Option<impl RewardCalcTracer>,
    ) {
        let epoch_rewards_sysvar = self.get_epoch_rewards_sysvar();
        if epoch_rewards_sysvar.active {
            let (stake_rewards, partition_indices) =
                self.recalculate_stake_rewards(&epoch_rewards_sysvar, reward_calc_tracer);
            self.set_epoch_reward_status_distribution(
                epoch_rewards_sysvar.distribution_starting_block_height,
                stake_rewards,
                partition_indices,
            );
        }
    }

    /// Returns a vector of partitioned stake rewards. StakeRewards are
    /// recalculated from an active EpochRewards sysvar, vote accounts from
    /// EpochStakes, and stake accounts from StakesCache.
    fn recalculate_stake_rewards(
        &self,
        epoch_rewards_sysvar: &EpochRewards,
        reward_calc_tracer: Option<impl RewardCalcTracer>,
    ) -> (Arc<Vec<PartitionedStakeReward>>, Vec<Vec<usize>>) {
        assert!(epoch_rewards_sysvar.active);
        // If rewards are active, the rewarded epoch is always the immediately
        // preceding epoch.
        let rewarded_epoch = self.epoch().saturating_sub(1);

        let point_value = PointValue {
            rewards: epoch_rewards_sysvar.total_rewards,
            points: epoch_rewards_sysvar.total_points,
        };

        let stakes = self.stakes_cache.stakes();
        let reward_calculate_param = self.get_epoch_reward_calculate_param_info(&stakes);

        // On recalculation, only the `StakeRewardCalculation::stake_rewards`
        // field is relevant. It is assumed that vote-account rewards have
        // already been calculated and delivered, while
        // `StakeRewardCalculation::total_rewards` only reflects rewards that
        // have not yet been distributed.
        let (_, StakeRewardCalculation { stake_rewards, .. }) = self.calculate_stake_vote_rewards(
            &reward_calculate_param,
            rewarded_epoch,
            point_value,
            reward_calc_tracer,
            &mut RewardsMetrics::default(), // This is required, but not reporting anything at the moment
        );
        drop(stakes);
        let partition_indices = hash_rewards_into_partitions(
            &stake_rewards,
            &epoch_rewards_sysvar.parent_blockhash,
            epoch_rewards_sysvar.num_partitions as usize,
        );
        (stake_rewards, partition_indices)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            bank::{
                null_tracer,
                partitioned_epoch_rewards::{
                    tests::{
                        build_partitioned_stake_rewards, create_default_reward_bank,
                        create_reward_bank, create_reward_bank_with_specific_stakes, RewardBank,
                        SLOTS_PER_EPOCH,
                    },
                    EpochRewardPhase, EpochRewardStatus, PartitionedStakeRewards,
                    StartBlockHeightAndPartitionedRewards,
                },
                tests::create_genesis_config,
                RewardInfo, VoteReward,
            },
            stake_account::StakeAccount,
            stakes::Stakes,
        },
        rayon::ThreadPoolBuilder,
        solana_account::{accounts_equal, state_traits::StateMut, ReadableAccount},
        solana_native_token::{sol_to_lamports, LAMPORTS_PER_SOL},
        solana_reward_info::RewardType,
        solana_stake_interface::state::{Delegation, StakeStateV2},
        solana_vote_interface::state::VoteState,
        std::sync::{Arc, RwLockReadGuard},
    };

    #[test]
    fn test_store_vote_accounts_partitioned() {
        let (genesis_config, _mint_keypair) = create_genesis_config(1_000_000 * LAMPORTS_PER_SOL);
        let bank = Bank::new_for_tests(&genesis_config);

        let expected_vote_rewards_num = 100;

        let vote_rewards = (0..expected_vote_rewards_num)
            .map(|_| (Pubkey::new_unique(), VoteReward::new_random()))
            .collect::<Vec<_>>();

        let mut vote_rewards_account = VoteRewardsAccounts::default();
        vote_rewards
            .iter()
            .for_each(|(vote_key, vote_reward_info)| {
                let info = RewardInfo {
                    reward_type: RewardType::Voting,
                    lamports: vote_reward_info.vote_rewards as i64,
                    post_balance: vote_reward_info.vote_rewards,
                    commission: Some(vote_reward_info.commission),
                };
                vote_rewards_account.rewards.push((*vote_key, info));
                vote_rewards_account
                    .accounts_to_store
                    .push((*vote_key, vote_reward_info.vote_account.clone()));
                vote_rewards_account.total_vote_rewards_lamports += vote_reward_info.vote_rewards;
            });

        let metrics = RewardsMetrics::default();

        let total_vote_rewards = vote_rewards_account.total_vote_rewards_lamports;
        bank.store_vote_accounts_partitioned(&vote_rewards_account, &metrics);
        assert_eq!(
            expected_vote_rewards_num,
            vote_rewards_account.accounts_to_store.len()
        );
        assert_eq!(
            vote_rewards
                .iter()
                .map(|(_, vote_reward_info)| vote_reward_info.vote_rewards)
                .sum::<u64>(),
            total_vote_rewards
        );

        // load accounts to make sure they were stored correctly
        vote_rewards
            .iter()
            .for_each(|(vote_key, vote_reward_info)| {
                let loaded_account = bank
                    .load_slow_with_fixed_root(&bank.ancestors, vote_key)
                    .unwrap();
                assert!(accounts_equal(
                    &loaded_account.0,
                    &vote_reward_info.vote_account
                ));
            });
    }

    #[test]
    fn test_store_vote_accounts_partitioned_empty() {
        let (genesis_config, _mint_keypair) = create_genesis_config(1_000_000 * LAMPORTS_PER_SOL);
        let bank = Bank::new_for_tests(&genesis_config);

        let expected = 0;
        let vote_rewards = VoteRewardsAccounts::default();
        let metrics = RewardsMetrics::default();
        let total_vote_rewards = vote_rewards.total_vote_rewards_lamports;

        bank.store_vote_accounts_partitioned(&vote_rewards, &metrics);
        assert_eq!(expected, vote_rewards.accounts_to_store.len());
        assert_eq!(0, total_vote_rewards);
    }

    #[test]
    /// Test rewards computation and partitioned rewards distribution at the epoch boundary
    fn test_rewards_computation() {
        solana_logger::setup();

        let expected_num_delegations = 100;
        let bank = create_default_reward_bank(expected_num_delegations, SLOTS_PER_EPOCH)
            .0
            .bank;

        // Calculate rewards
        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let mut rewards_metrics = RewardsMetrics::default();
        let expected_rewards = 100_000_000_000;

        let calculated_rewards = bank.calculate_validator_rewards(
            1,
            expected_rewards,
            null_tracer(),
            &thread_pool,
            &mut rewards_metrics,
        );

        let vote_rewards = &calculated_rewards.as_ref().unwrap().vote_rewards_accounts;
        let stake_rewards = &calculated_rewards
            .as_ref()
            .unwrap()
            .stake_reward_calculation;

        let total_vote_rewards: u64 = vote_rewards
            .rewards
            .iter()
            .map(|reward| reward.1.lamports)
            .sum::<i64>() as u64;

        // assert that total rewards matches the sum of vote rewards and stake rewards
        assert_eq!(
            stake_rewards.total_stake_rewards_lamports + total_vote_rewards,
            expected_rewards
        );

        // assert that number of stake rewards matches
        assert_eq!(stake_rewards.stake_rewards.len(), expected_num_delegations);
    }

    #[test]
    fn test_rewards_point_calculation() {
        solana_logger::setup();

        let expected_num_delegations = 100;
        let RewardBank { bank, .. } =
            create_default_reward_bank(expected_num_delegations, SLOTS_PER_EPOCH).0;

        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let rewards_metrics = RewardsMetrics::default();
        let expected_rewards = 100_000_000_000;

        let stakes: RwLockReadGuard<Stakes<StakeAccount<Delegation>>> = bank.stakes_cache.stakes();
        let reward_calculate_param = bank.get_epoch_reward_calculate_param_info(&stakes);

        let point_value = bank.calculate_reward_points_partitioned(
            &reward_calculate_param,
            expected_rewards,
            &thread_pool,
            &rewards_metrics,
        );

        assert!(point_value.is_some());
        assert_eq!(point_value.as_ref().unwrap().rewards, expected_rewards);
        assert_eq!(point_value.as_ref().unwrap().points, 8400000000000);
    }

    #[test]
    fn test_rewards_point_calculation_empty() {
        solana_logger::setup();

        // bank with no rewards to distribute
        let (genesis_config, _mint_keypair) = create_genesis_config(sol_to_lamports(1.0));
        let bank = Bank::new_for_tests(&genesis_config);

        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let rewards_metrics: RewardsMetrics = RewardsMetrics::default();
        let expected_rewards = 100_000_000_000;
        let stakes: RwLockReadGuard<Stakes<StakeAccount<Delegation>>> = bank.stakes_cache.stakes();
        let reward_calculate_param = bank.get_epoch_reward_calculate_param_info(&stakes);

        let point_value = bank.calculate_reward_points_partitioned(
            &reward_calculate_param,
            expected_rewards,
            &thread_pool,
            &rewards_metrics,
        );

        assert!(point_value.is_none());
    }

    #[test]
    fn test_calculate_stake_vote_rewards() {
        solana_logger::setup();

        let expected_num_delegations = 1;
        let RewardBank {
            bank,
            voters,
            stakers,
        } = create_default_reward_bank(expected_num_delegations, SLOTS_PER_EPOCH).0;

        let vote_pubkey = voters.first().unwrap();
        let stake_pubkey = *stakers.first().unwrap();
        let stake_account = bank
            .load_slow_with_fixed_root(&bank.ancestors, &stake_pubkey)
            .unwrap()
            .0;

        let mut rewards_metrics = RewardsMetrics::default();

        let point_value = PointValue {
            rewards: 100000, // lamports to split
            points: 1000,    // over these points
        };
        let tracer = |_event: &RewardCalculationEvent| {};
        let reward_calc_tracer = Some(tracer);
        let rewarded_epoch = bank.epoch();
        let stakes: RwLockReadGuard<Stakes<StakeAccount<Delegation>>> = bank.stakes_cache.stakes();
        let reward_calculate_param = bank.get_epoch_reward_calculate_param_info(&stakes);
        let (vote_rewards_accounts, stake_reward_calculation) = bank.calculate_stake_vote_rewards(
            &reward_calculate_param,
            rewarded_epoch,
            point_value,
            reward_calc_tracer,
            &mut rewards_metrics,
        );

        let vote_account = bank
            .load_slow_with_fixed_root(&bank.ancestors, vote_pubkey)
            .unwrap()
            .0;
        let vote_state = VoteState::deserialize(vote_account.data()).unwrap();

        assert_eq!(
            vote_rewards_accounts.rewards.len(),
            vote_rewards_accounts.accounts_to_store.len()
        );
        assert_eq!(vote_rewards_accounts.rewards.len(), 1);
        let rewards = &vote_rewards_accounts.rewards[0];
        let account = &vote_rewards_accounts.accounts_to_store[0].1;
        let vote_rewards = 0;
        let commission = vote_state.commission;
        assert_eq!(account.lamports(), vote_account.lamports());
        assert!(accounts_equal(account, &vote_account));
        assert_eq!(
            rewards.1,
            RewardInfo {
                reward_type: RewardType::Voting,
                lamports: vote_rewards as i64,
                post_balance: vote_account.lamports(),
                commission: Some(commission),
            }
        );
        assert_eq!(&rewards.0, vote_pubkey);

        assert_eq!(stake_reward_calculation.stake_rewards.len(), 1);
        let expected_reward = {
            let stake_reward = 8_400_000_000_000;
            let stake_state: StakeStateV2 = stake_account.state().unwrap();
            let mut stake = stake_state.stake().unwrap();
            stake.credits_observed = vote_state.credits();
            stake.delegation.stake += stake_reward;
            PartitionedStakeReward {
                stake,
                stake_pubkey,
                stake_reward,
                commission,
            }
        };
        assert_eq!(stake_reward_calculation.stake_rewards[0], expected_reward);
    }

    fn compare_stake_rewards(
        expected_stake_rewards: &[PartitionedStakeRewards],
        received_stake_rewards: &[PartitionedStakeRewards],
    ) {
        for (i, partition) in received_stake_rewards.iter().enumerate() {
            let expected_partition = &expected_stake_rewards[i];
            assert_eq!(partition.len(), expected_partition.len());
            for reward in partition {
                assert!(expected_partition.iter().any(|x| x == reward));
            }
        }
    }

    #[test]
    fn test_recalculate_stake_rewards() {
        let expected_num_delegations = 4;
        let num_rewards_per_block = 2;
        // Distribute 4 rewards over 2 blocks
        let (RewardBank { bank, .. }, _) = create_reward_bank(
            expected_num_delegations,
            num_rewards_per_block,
            SLOTS_PER_EPOCH,
        );
        let rewarded_epoch = bank.epoch();

        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let mut rewards_metrics = RewardsMetrics::default();
        let PartitionedRewardsCalculation {
            stake_rewards:
                StakeRewardCalculation {
                    stake_rewards: expected_stake_rewards,
                    ..
                },
            ..
        } = bank.calculate_rewards_for_partitioning(
            rewarded_epoch,
            null_tracer(),
            &thread_pool,
            &mut rewards_metrics,
        );

        let epoch_rewards_sysvar = bank.get_epoch_rewards_sysvar();
        let (recalculated_rewards, recalculated_partition_indices) =
            bank.recalculate_stake_rewards(&epoch_rewards_sysvar, null_tracer());

        let recalculated_rewards =
            build_partitioned_stake_rewards(&recalculated_rewards, &recalculated_partition_indices);

        let expected_partition_indices = hash_rewards_into_partitions(
            &expected_stake_rewards,
            &epoch_rewards_sysvar.parent_blockhash,
            epoch_rewards_sysvar.num_partitions as usize,
        );

        let expected_stake_rewards_partitioned =
            build_partitioned_stake_rewards(&expected_stake_rewards, &expected_partition_indices);

        assert_eq!(
            expected_stake_rewards_partitioned.len(),
            recalculated_rewards.len()
        );
        compare_stake_rewards(&expected_stake_rewards_partitioned, &recalculated_rewards);

        // Advance to first distribution block, ie. child block of the epoch
        // boundary; slot is advanced 2 to demonstrate that distribution works
        // on block-height, not slot
        let new_slot = bank.slot() + 2;
        let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), new_slot));

        let epoch_rewards_sysvar = bank.get_epoch_rewards_sysvar();
        let (recalculated_rewards, recalculated_partition_indices) =
            bank.recalculate_stake_rewards(&epoch_rewards_sysvar, null_tracer());

        // Note that recalculated rewards are **NOT** the same as expected
        // rewards, which were calculated before any distribution. This is
        // because "Recalculated rewards" doesn't include already distributed
        // stake rewards. Therefore, the partition_indices are different too.
        // However, the actual rewards for the remaining partitions should be
        // the same. The following code use the test helper function to build
        // the partitioned stake rewards for the remaining partitions and verify
        // that they are the same.
        let recalculated_rewards =
            build_partitioned_stake_rewards(&recalculated_rewards, &recalculated_partition_indices);
        assert_eq!(
            expected_stake_rewards_partitioned.len(),
            recalculated_rewards.len()
        );
        // First partition has already been distributed, so recalculation
        // returns 0 rewards
        assert_eq!(recalculated_rewards[0].len(), 0);
        let starting_index = (bank.block_height() + 1
            - epoch_rewards_sysvar.distribution_starting_block_height)
            as usize;
        compare_stake_rewards(
            &expected_stake_rewards_partitioned[starting_index..],
            &recalculated_rewards[starting_index..],
        );

        // Advance to last distribution slot
        let new_slot = bank.slot() + 1;
        let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), new_slot));

        let epoch_rewards_sysvar = bank.get_epoch_rewards_sysvar();
        assert!(!epoch_rewards_sysvar.active);
        // Recalculation would panic, tested separately
    }

    #[test]
    #[should_panic]
    fn test_recalculate_stake_rewards_distribution_complete() {
        let expected_num_delegations = 2;
        let num_rewards_per_block = 2;
        // Distribute 2 rewards over 1 block
        let (RewardBank { bank, .. }, _) = create_reward_bank(
            expected_num_delegations,
            num_rewards_per_block,
            SLOTS_PER_EPOCH,
        );
        let rewarded_epoch = bank.epoch();

        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let mut rewards_metrics = RewardsMetrics::default();
        let PartitionedRewardsCalculation {
            stake_rewards:
                StakeRewardCalculation {
                    stake_rewards: expected_stake_rewards,
                    ..
                },
            ..
        } = bank.calculate_rewards_for_partitioning(
            rewarded_epoch,
            null_tracer(),
            &thread_pool,
            &mut rewards_metrics,
        );

        let epoch_rewards_sysvar = bank.get_epoch_rewards_sysvar();
        let expected_partition_indices = hash_rewards_into_partitions(
            &expected_stake_rewards,
            &epoch_rewards_sysvar.parent_blockhash,
            epoch_rewards_sysvar.num_partitions as usize,
        );
        let expected_stake_rewards =
            build_partitioned_stake_rewards(&expected_stake_rewards, &expected_partition_indices);

        let (recalculated_rewards, recalculated_partition_indices) =
            bank.recalculate_stake_rewards(&epoch_rewards_sysvar, null_tracer());
        let recalculated_rewards =
            build_partitioned_stake_rewards(&recalculated_rewards, &recalculated_partition_indices);

        assert_eq!(expected_stake_rewards.len(), recalculated_rewards.len());
        compare_stake_rewards(&expected_stake_rewards, &recalculated_rewards);

        // Advance to first distribution slot
        let new_slot = bank.slot() + 1;
        let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), new_slot));

        let epoch_rewards_sysvar = bank.get_epoch_rewards_sysvar();
        assert!(!epoch_rewards_sysvar.active);
        // Should panic
        let _recalculated_rewards =
            bank.recalculate_stake_rewards(&epoch_rewards_sysvar, null_tracer());
    }

    #[test]
    fn test_recalculate_partitioned_rewards() {
        let expected_num_delegations = 3;
        let num_rewards_per_block = 2;
        // Distribute 4 rewards over 2 blocks
        let mut stakes = vec![2_000_000_000; expected_num_delegations];
        // Add stake large enough to be affected by total-rewards discrepancy
        stakes.push(40_000_000_000);
        let (RewardBank { bank, .. }, _) = create_reward_bank_with_specific_stakes(
            stakes,
            num_rewards_per_block,
            SLOTS_PER_EPOCH - 1,
        );
        let rewarded_epoch = bank.epoch();

        // Advance to next epoch boundary to update EpochStakes Kludgy because
        // mutable Bank methods require the bank not be Arc-wrapped.
        let new_slot = bank.slot() + 1;
        let mut bank = Bank::new_from_parent(bank, &Pubkey::default(), new_slot);
        let expected_starting_block_height = bank.block_height() + 1;

        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let mut rewards_metrics = RewardsMetrics::default();
        let PartitionedRewardsCalculation {
            stake_rewards:
                StakeRewardCalculation {
                    stake_rewards: expected_stake_rewards,
                    ..
                },
            point_value,
            ..
        } = bank.calculate_rewards_for_partitioning(
            rewarded_epoch,
            null_tracer(),
            &thread_pool,
            &mut rewards_metrics,
        );

        bank.recalculate_partitioned_rewards(null_tracer());
        let EpochRewardStatus::Active(EpochRewardPhase::Distribution(
            StartBlockHeightAndPartitionedRewards {
                distribution_starting_block_height,
                all_stake_rewards: ref recalculated_rewards,
                ref partition_indices,
            },
        )) = bank.epoch_reward_status
        else {
            panic!("{:?} not active", bank.epoch_reward_status);
        };
        assert_eq!(
            expected_starting_block_height,
            distribution_starting_block_height
        );

        let recalculated_rewards =
            build_partitioned_stake_rewards(recalculated_rewards, partition_indices);

        let epoch_rewards_sysvar = bank.get_epoch_rewards_sysvar();
        let expected_partition_indices = hash_rewards_into_partitions(
            &expected_stake_rewards,
            &epoch_rewards_sysvar.parent_blockhash,
            epoch_rewards_sysvar.num_partitions as usize,
        );
        let expected_stake_rewards =
            build_partitioned_stake_rewards(&expected_stake_rewards, &expected_partition_indices);

        assert_eq!(expected_stake_rewards.len(), recalculated_rewards.len());
        compare_stake_rewards(&expected_stake_rewards, &recalculated_rewards);

        let sysvar = bank.get_epoch_rewards_sysvar();
        assert_eq!(point_value.rewards, sysvar.total_rewards);

        // Advance to first distribution slot
        let mut bank =
            Bank::new_from_parent(Arc::new(bank), &Pubkey::default(), SLOTS_PER_EPOCH + 1);

        bank.recalculate_partitioned_rewards(null_tracer());
        let EpochRewardStatus::Active(EpochRewardPhase::Distribution(
            StartBlockHeightAndPartitionedRewards {
                distribution_starting_block_height,
                all_stake_rewards: ref recalculated_rewards,
                ref partition_indices,
            },
        )) = bank.epoch_reward_status
        else {
            panic!("{:?} not active", bank.epoch_reward_status);
        };

        // Note that recalculated rewards are **NOT** the same as expected
        // rewards, which were calculated before any distribution. This is
        // because "Recalculated rewards" doesn't include already distributed
        // stake rewards. Therefore, the partition_indices are different too.
        // However, the actual rewards for the remaining partitions should be
        // the same. The following code use the test helper function to build
        // the partitioned stake rewards for the remaining partitions and verify
        // that they are the same.
        let recalculated_rewards =
            build_partitioned_stake_rewards(recalculated_rewards, partition_indices);
        assert_eq!(
            expected_starting_block_height,
            distribution_starting_block_height
        );
        assert_eq!(expected_stake_rewards.len(), recalculated_rewards.len());
        // First partition has already been distributed, so recalculation
        // returns 0 rewards
        assert_eq!(recalculated_rewards[0].len(), 0);
        let epoch_rewards_sysvar = bank.get_epoch_rewards_sysvar();
        let starting_index = (bank.block_height() + 1
            - epoch_rewards_sysvar.distribution_starting_block_height)
            as usize;
        compare_stake_rewards(
            &expected_stake_rewards[starting_index..],
            &recalculated_rewards[starting_index..],
        );

        // Advance to last distribution slot
        let mut bank =
            Bank::new_from_parent(Arc::new(bank), &Pubkey::default(), SLOTS_PER_EPOCH + 2);

        bank.recalculate_partitioned_rewards(null_tracer());
        assert_eq!(bank.epoch_reward_status, EpochRewardStatus::Inactive);
    }
}
