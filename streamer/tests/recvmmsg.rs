#![cfg(target_os = "linux")]

use {
    solana_net_utils::bind_to_localhost,
    solana_streamer::{
        packet::{PacketMutBatch, PACKET_DATA_SIZE},
        recvmmsg::*,
    },
    std::time::Instant,
};

#[test]
pub fn test_recv_mmsg_batch_size() {
    let reader = bind_to_localhost().expect("bind");
    let addr = reader.local_addr().unwrap();
    let sender = bind_to_localhost().expect("bind");

    const TEST_BATCH_SIZE: usize = 64;
    let sent = TEST_BATCH_SIZE;

    let mut elapsed_in_max_batch = 0;
    let mut num_max_batches = 0;
    (0..1000).for_each(|_| {
        for _ in 0..sent {
            let data = [0; PACKET_DATA_SIZE];
            sender.send_to(&data[..], addr).unwrap();
        }
        let mut packets = PacketMutBatch::with_len(TEST_BATCH_SIZE);
        let now = Instant::now();
        let recv = recv_mmsg(&reader, &mut packets[..]).unwrap();
        elapsed_in_max_batch += now.elapsed().as_nanos();
        if recv == TEST_BATCH_SIZE {
            num_max_batches += 1;
        }
    });
    assert!(num_max_batches > 990);

    let mut elapsed_in_small_batch = 0;
    (0..1000).for_each(|_| {
        for _ in 0..sent {
            let data = [0; PACKET_DATA_SIZE];
            sender.send_to(&data[..], addr).unwrap();
        }
        let mut packets = PacketMutBatch::with_len(4);
        let mut recv = 0;
        let now = Instant::now();
        while let Ok(num) = recv_mmsg(&reader, &mut packets[..]) {
            recv += num;
            if recv >= TEST_BATCH_SIZE {
                break;
            }
            packets = PacketMutBatch::with_len(4);
        }
        elapsed_in_small_batch += now.elapsed().as_nanos();
        assert_eq!(TEST_BATCH_SIZE, recv);
    });

    assert!(elapsed_in_max_batch <= elapsed_in_small_batch);
}
