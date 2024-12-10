#![allow(clippy::arithmetic_side_effects)]
use {
    criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput},
    solana_accounts_db::{
        accounts_file::StorageAccess,
        append_vec::{self, AppendVec},
        tiered_storage::{
            file::TieredReadableFile,
            hot::{HotStorageReader, HotStorageWriter},
        },
    },
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
        pubkey::Pubkey,
        rent_collector::RENT_EXEMPT_RENT_EPOCH,
    },
    std::mem::ManuallyDrop,
};

mod utils;

const ACCOUNTS_COUNTS: [usize; 4] = [
    1,      // the smallest count; will bench overhead
    100,    // number of accounts written per slot on mnb (with *no* rent rewrites)
    1_000,  // number of accounts written slot on mnb (with rent rewrites)
    10_000, // reasonable largest number of accounts written per slot
];

fn bench_write_accounts_file(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_accounts_file");

    // most accounts on mnb are 165-200 bytes, so use that here too
    let space = 200;
    let lamports = 2_282_880; // the rent-exempt amount for 200 bytes of data
    let temp_dir = tempfile::tempdir().unwrap();

    for accounts_count in ACCOUNTS_COUNTS {
        group.throughput(Throughput::Elements(accounts_count as u64));

        let accounts: Vec<_> = std::iter::repeat_with(|| {
            (
                Pubkey::new_unique(),
                AccountSharedData::new_rent_epoch(
                    lamports,
                    space,
                    &Pubkey::new_unique(),
                    RENT_EXEMPT_RENT_EPOCH,
                ),
            )
        })
        .take(accounts_count)
        .collect();
        let accounts_refs: Vec<_> = accounts
            .iter()
            .map(|(pubkey, account)| (pubkey, account))
            .collect();
        let storable_accounts = (Slot::MAX, accounts_refs.as_slice());

        group.bench_function(BenchmarkId::new("append_vec", accounts_count), |b| {
            b.iter_batched_ref(
                || {
                    let path = temp_dir.path().join(format!("append_vec_{accounts_count}"));
                    let file_size = accounts.len() * (space + append_vec::STORE_META_OVERHEAD);
                    AppendVec::new(path, true, file_size)
                },
                |append_vec| {
                    let res = append_vec.append_accounts(&storable_accounts, 0).unwrap();
                    let accounts_written_count = res.offsets.len();
                    assert_eq!(accounts_written_count, accounts_count);
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(BenchmarkId::new("hot_storage", accounts_count), |b| {
            b.iter_batched_ref(
                || {
                    let path = temp_dir
                        .path()
                        .join(format!("hot_storage_{accounts_count}"));
                    _ = std::fs::remove_file(&path);
                    HotStorageWriter::new(path).unwrap()
                },
                |hot_storage| {
                    let res = hot_storage.write_accounts(&storable_accounts, 0).unwrap();
                    let accounts_written_count = res.offsets.len();
                    assert_eq!(accounts_written_count, accounts_count);
                    // Purposely do not call hot_storage.flush() here, since it will impact the
                    // bench.  Flushing will be handled by Drop, which is *not* timed (and that's
                    // what we want).
                },
                BatchSize::SmallInput,
            );
        });
    }
}

fn bench_scan_pubkeys(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_pubkeys");
    let temp_dir = tempfile::tempdir().unwrap();

    for accounts_count in ACCOUNTS_COUNTS {
        group.throughput(Throughput::Elements(accounts_count as u64));

        let storable_accounts: Vec<_> = utils::accounts(accounts_count).collect();

        // create an append vec file
        let append_vec_path = temp_dir.path().join(format!("append_vec_{accounts_count}"));
        _ = std::fs::remove_file(&append_vec_path);
        let file_size = storable_accounts
            .iter()
            .map(|(_, account)| append_vec::aligned_stored_size(account.data().len()))
            .sum();
        let append_vec = AppendVec::new(append_vec_path, true, file_size);
        let stored_accounts_info = append_vec
            .append_accounts(&(Slot::MAX, storable_accounts.as_slice()), 0)
            .unwrap();
        assert_eq!(stored_accounts_info.offsets.len(), accounts_count);
        append_vec.flush().unwrap();
        // Open append vecs for reading here, outside of the bench function, so we don't open lots
        // of file handles and run out/crash.  We also need to *not* remove the backing file in
        // these new append vecs because that would cause double-free (or triple-free here).
        // Wrap the append vecs in ManuallyDrop to *not* remove the backing file on drop.
        let append_vec_mmap = ManuallyDrop::new(
            AppendVec::new_from_file(append_vec.path(), append_vec.len(), StorageAccess::Mmap)
                .unwrap()
                .0,
        );
        let append_vec_file = ManuallyDrop::new(
            AppendVec::new_from_file(append_vec.path(), append_vec.len(), StorageAccess::File)
                .unwrap()
                .0,
        );

        // create a hot storage file
        let hot_storage_path = temp_dir
            .path()
            .join(format!("hot_storage_{accounts_count}"));
        _ = std::fs::remove_file(&hot_storage_path);
        let mut hot_storage_writer = HotStorageWriter::new(&hot_storage_path).unwrap();
        let stored_accounts_info = hot_storage_writer
            .write_accounts(&(Slot::MAX, storable_accounts.as_slice()), 0)
            .unwrap();
        assert_eq!(stored_accounts_info.offsets.len(), accounts_count);
        hot_storage_writer.flush().unwrap();
        // Similar to the append vec case above, open the hot storage for reading here.
        let hot_storage_file = TieredReadableFile::new(&hot_storage_path).unwrap();
        let hot_storage_reader = HotStorageReader::new(hot_storage_file).unwrap();

        group.bench_function(BenchmarkId::new("append_vec_mmap", accounts_count), |b| {
            b.iter(|| {
                let mut count = 0;
                append_vec_mmap.scan_pubkeys(|_| count += 1);
                assert_eq!(count, accounts_count);
            });
        });
        group.bench_function(BenchmarkId::new("append_vec_file", accounts_count), |b| {
            b.iter(|| {
                let mut count = 0;
                append_vec_file.scan_pubkeys(|_| count += 1);
                assert_eq!(count, accounts_count);
            });
        });
        group.bench_function(BenchmarkId::new("hot_storage", accounts_count), |b| {
            b.iter(|| {
                let mut count = 0;
                hot_storage_reader.scan_pubkeys(|_| count += 1).unwrap();
                assert_eq!(count, accounts_count);
            });
        });
    }
}

criterion_group!(benches, bench_write_accounts_file, bench_scan_pubkeys);
criterion_main!(benches);
