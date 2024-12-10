use {
    rand::{
        distributions::{Distribution, WeightedIndex},
        Rng, SeedableRng,
    },
    rand_chacha::ChaChaRng,
    solana_accounts_db::append_vec::SCAN_BUFFER_SIZE_WITHOUT_DATA,
    solana_sdk::{
        account::AccountSharedData, pubkey::Pubkey, rent::Rent,
        rent_collector::RENT_EXEMPT_RENT_EPOCH, system_instruction::MAX_PERMITTED_DATA_LENGTH,
    },
    std::iter,
};

/// Sizes of accounts.
///
/// - No data.
/// - 165 bytes (a token account).
/// - 200 bytes (a stake account).
/// - 256 kibibytes (pathological case for the scan buffer).
/// - 10 mebibytes (the max size for an account).
const DATA_SIZES: &[usize] = &[
    0,
    165,
    200,
    SCAN_BUFFER_SIZE_WITHOUT_DATA,
    MAX_PERMITTED_DATA_LENGTH as usize,
];
/// Distribution of the account sizes:
///
/// - 3% of accounts have no data.
/// - 75% of accounts are 165 bytes (a token account).
/// - 20% of accounts are 200 bytes (a stake account).
/// - 1% of accounts are 256 kibibytes (pathological case for the scan buffer).
/// - 1% of accounts are 10 mebibytes (the max size for an account).
const WEIGHTS: &[usize] = &[3, 75, 20, 1, 1];

/// Returns an iterator with storable accounts.
pub fn accounts(accounts_count: usize) -> impl Iterator<Item = (Pubkey, AccountSharedData)> {
    let distribution = WeightedIndex::new(WEIGHTS).unwrap();
    let mut rng = ChaChaRng::seed_from_u64(accounts_count as u64);
    let rent = Rent::default();

    iter::repeat_with(move || {
        let index = distribution.sample(&mut rng);
        let data_size = DATA_SIZES[index];
        let owner: [u8; 32] = rng.gen();
        let owner = Pubkey::new_from_array(owner);
        (
            owner.clone(),
            AccountSharedData::new_rent_epoch(
                rent.minimum_balance(data_size),
                data_size,
                &owner,
                RENT_EXEMPT_RENT_EPOCH,
            ),
        )
    })
}

#[allow(dead_code)]
pub fn accounts_with_size_limit(
    size_limit: usize,
) -> impl Iterator<Item = (Pubkey, AccountSharedData)> {
    let distribution = WeightedIndex::new(WEIGHTS).unwrap();
    let mut rng = ChaChaRng::seed_from_u64(255);
    let rent = Rent::default();
    let mut sum = 0_usize;

    iter::from_fn(move || {
        let index = distribution.sample(&mut rng);
        let data_size = DATA_SIZES[index];
        sum = sum.saturating_add(data_size);
        if sum >= size_limit {
            None
        } else {
            let owner = Pubkey::new_unique();

            Some((
                owner.clone(),
                AccountSharedData::new_rent_epoch(
                    rent.minimum_balance(data_size),
                    data_size,
                    &owner,
                    RENT_EXEMPT_RENT_EPOCH,
                ),
            ))
        }
    })
}
