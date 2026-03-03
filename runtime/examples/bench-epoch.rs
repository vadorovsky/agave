use {
    clap::{App, Arg, ArgMatches, SubCommand},
    solana_account::{Account, AccountSharedData, ReadableAccount, state_traits::StateMut},
    solana_native_token::LAMPORTS_PER_SOL,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank,
        genesis_utils::{
            GenesisConfigInfo, ValidatorVoteKeypairs, create_genesis_config_with_vote_accounts,
        },
    },
    solana_sdk_ids::stake as stake_program,
    solana_signer::Signer,
    solana_stake_interface::{
        stake_flags::StakeFlags,
        state::{Delegation, Meta, Stake, StakeStateV2},
    },
    solana_sysvar::epoch_rewards::{self, EpochRewards},
    solana_vote_interface::state::{MAX_LOCKOUT_HISTORY, VoteStateV4, VoteStateVersions},
    solana_vote_program::vote_state::process_slot_vote_unchecked,
    std::{hint::black_box, sync::Arc, time::Instant},
};

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

const CMD_TURNOVER: &str = "turnover";
const CMD_REWARDS_PERIOD: &str = "rewards-period";
const ARG_VOTE_ACCOUNTS: &str = "vote-accounts";
const ARG_STAKE_ACCOUNTS: &str = "stake-accounts";

const DELEGATED_STAKE_LAMPORTS: u64 = 1_000 * LAMPORTS_PER_SOL;
const VALIDATOR_STAKE_LAMPORTS: u64 = 1_000 * LAMPORTS_PER_SOL;
const GENESIS_MINT_LAMPORTS: u64 = 1_000_000 * LAMPORTS_PER_SOL;
const SYNTHETIC_VOTE_SLOTS: u64 = (MAX_LOCKOUT_HISTORY as u64) + 42;

fn create_stake_account(vote_pubkey: &Pubkey, rent_exempt_reserve: u64) -> Account {
    let total_lamports = rent_exempt_reserve + DELEGATED_STAKE_LAMPORTS;

    let meta = Meta {
        rent_exempt_reserve,
        ..Meta::default()
    };

    let delegation = Delegation {
        voter_pubkey: *vote_pubkey,
        stake: DELEGATED_STAKE_LAMPORTS,
        ..Delegation::default()
    };

    let stake = Stake {
        delegation,
        credits_observed: 0,
    };

    let stake_state = StakeStateV2::Stake(meta, stake, StakeFlags::empty());

    let mut account = AccountSharedData::new(
        total_lamports,
        StakeStateV2::size_of(),
        &stake_program::id(),
    );
    account.set_state(&stake_state).unwrap();
    Account::from(account)
}

fn populate_vote_accounts(bank: &Bank, vote_pubkeys: Vec<Pubkey>) {
    for vote_pubkey in vote_pubkeys.into_iter() {
        let mut vote_account = bank.get_account(&vote_pubkey).unwrap();

        let mut vote_state = VoteStateV4::deserialize(vote_account.data(), &vote_pubkey).unwrap();

        for i in 0..SYNTHETIC_VOTE_SLOTS {
            process_slot_vote_unchecked(&mut vote_state, i);
        }

        let versioned = VoteStateVersions::V4(Box::new(vote_state));
        vote_account.set_state(&versioned).unwrap();

        bank.store_account(&vote_pubkey, &vote_account);
    }
}

fn setup_bank(vote_accounts: usize, stake_accounts: usize) -> Arc<Bank> {
    let validators = (0..vote_accounts)
        .map(|_| ValidatorVoteKeypairs::new_rand())
        .collect::<Vec<_>>();

    let GenesisConfigInfo {
        mut genesis_config, ..
    } = create_genesis_config_with_vote_accounts(
        GENESIS_MINT_LAMPORTS,
        &validators.iter().collect::<Vec<_>>(),
        vec![VALIDATOR_STAKE_LAMPORTS; vote_accounts],
    );

    let vote_pubkeys = validators
        .iter()
        .map(|v| v.vote_keypair.pubkey())
        .collect::<Vec<_>>();

    let stakes_per_vote = stake_accounts / vote_accounts;
    let stake_rent_exempt_reserve = genesis_config.rent.minimum_balance(StakeStateV2::size_of());

    for vote_pubkey in vote_pubkeys.iter() {
        let stake_account = create_stake_account(vote_pubkey, stake_rent_exempt_reserve);

        for _ in 0..stakes_per_vote {
            let stake_pubkey = Pubkey::new_unique();
            genesis_config
                .accounts
                .insert(stake_pubkey, stake_account.clone());
        }
    }

    let initial_bank = Arc::new(Bank::new_for_tests(&genesis_config));

    populate_vote_accounts(&initial_bank, vote_pubkeys);

    let last_slot_in_epoch = initial_bank.get_slots_in_epoch(0).checked_sub(1).unwrap();

    Arc::new(Bank::new_from_parent(
        initial_bank,
        &Pubkey::default(),
        last_slot_in_epoch,
    ))
}

fn bench_turnover(matches: &ArgMatches<'_>) {
    let vote_accounts: usize = matches
        .value_of(ARG_VOTE_ACCOUNTS)
        .unwrap()
        .parse()
        .unwrap();
    let stake_accounts: usize = matches
        .value_of(ARG_STAKE_ACCOUNTS)
        .unwrap()
        .parse()
        .unwrap();
    println!(
        "Benchmarking epoch turnover with {vote_accounts} vote accounts and {stake_accounts} stake accounts"
    );

    let bank_now = Instant::now();
    let initial_bank = setup_bank(vote_accounts, stake_accounts);
    println!("Bank setup: {:?}", bank_now.elapsed());
    let first_epoch_slot = initial_bank.slot() + 1;

    let now = Instant::now();
    let bank = Bank::new_from_parent(initial_bank.clone(), &Pubkey::default(), first_epoch_slot);
    black_box(bank);
    println!("Epoch turnover: {:?}", now.elapsed());
}

fn bench_rewards_period(matches: &ArgMatches<'_>) {
    let vote_accounts: usize = matches
        .value_of(ARG_VOTE_ACCOUNTS)
        .unwrap()
        .parse()
        .unwrap();
    let stake_accounts: usize = matches
        .value_of(ARG_STAKE_ACCOUNTS)
        .unwrap()
        .parse()
        .unwrap();
    println!(
        "Benchmarking rewards period with {vote_accounts} vote accounts and {stake_accounts} stake accounts"
    );

    let bank_now = Instant::now();
    let initial_bank = setup_bank(vote_accounts, stake_accounts);
    println!("Bank setup: {:?}", bank_now.elapsed());
    let first_epoch_slot = initial_bank.slot() + 1;

    let bank = Arc::new(Bank::new_from_parent(
        initial_bank,
        &Pubkey::default(),
        first_epoch_slot,
    ));

    let rewards_steps = bank
        .get_account(&epoch_rewards::id())
        .and_then(|account| bincode::deserialize::<EpochRewards>(account.data()).ok())
        .unwrap()
        .num_partitions;

    let final_rewards_slot = first_epoch_slot + rewards_steps;

    let now = Instant::now();
    let mut bank = bank.clone();
    for slot in (first_epoch_slot + 1)..=final_rewards_slot {
        bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), slot));
    }
    black_box(bank);
    println!("Rewards period: {:?}", now.elapsed());
}

fn main() {
    let version = solana_version::version!();
    let mut app = App::new("bench-epoch")
        .about("benchmark for epoch turnover and rewards period")
        .version(version)
        .subcommand(
            SubCommand::with_name(CMD_TURNOVER)
                .about("")
                .arg(
                    Arg::with_name(ARG_VOTE_ACCOUNTS)
                        .long(ARG_VOTE_ACCOUNTS)
                        .takes_value(true)
                        .default_value("1000")
                        .help("Number of vote accounts"),
                )
                .arg(
                    Arg::with_name(ARG_STAKE_ACCOUNTS)
                        .long(ARG_STAKE_ACCOUNTS)
                        .takes_value(true)
                        .default_value("1000000")
                        .help("Number of stake accounts"),
                ),
        )
        .subcommand(
            SubCommand::with_name(CMD_REWARDS_PERIOD)
                .about("")
                .arg(
                    Arg::with_name("vote-accounts")
                        .long("vote-accounts")
                        .takes_value(true)
                        .value_name("VOTE_ACCOUNTS")
                        .default_value("1000")
                        .help("Number of vote accounts"),
                )
                .arg(
                    Arg::with_name("stake-accounts")
                        .long("stake-accounts")
                        .takes_value(true)
                        .value_name("STAKE_ACCOUNTS")
                        .default_value("1000000")
                        .help("Number of stake accounts"),
                ),
        );

    let matches = app.clone().get_matches();

    let subcommand = matches.subcommand();
    match subcommand {
        (CMD_TURNOVER, Some(matches)) => bench_turnover(matches),
        (CMD_REWARDS_PERIOD, Some(matches)) => bench_rewards_period(matches),
        _ => {
            app.print_help().unwrap();
        }
    };
}
