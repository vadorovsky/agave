use {
    bytes::{BufMut, Bytes, BytesMut},
    solana_ledger::{
        blockstore::Blockstore,
        shred::{Nonce, SIZE_OF_NONCE},
    },
    solana_perf::packet::{Meta, Packet},
    solana_sdk::clock::Slot,
    std::net::SocketAddr,
};

pub fn repair_response_packet(
    blockstore: &Blockstore,
    slot: Slot,
    shred_index: u64,
    dest: &SocketAddr,
    nonce: Nonce,
) -> Option<Packet> {
    let shred = blockstore
        .get_data_shred(slot, shred_index)
        .expect("Blockstore could not get data shred");
    shred
        .map(|shred| repair_response_packet_from_bytes(shred, dest, nonce))
        .unwrap_or(None)
}

pub fn repair_response_packet_from_bytes(
    bytes: Vec<u8>,
    dest: &SocketAddr,
    nonce: Nonce,
) -> Option<Packet> {
    let mut meta = Meta::default();
    meta.set_socket_addr(dest);
    let mut buffer = BytesMut::from(Bytes::from(bytes.into_boxed_slice()));
    buffer.reserve(SIZE_OF_NONCE);
    let mut wr = buffer.writer();
    bincode::serialize_into(&mut wr, &nonce).expect("Buffer not large enough to fit nonce");
    let buffer = wr.into_inner().freeze();
    let packet = Packet::new(buffer, meta);
    Some(packet)
}

#[cfg(test)]
mod test {
    use {
        super::*,
        solana_ledger::{
            shred::{Payload, Shred, ShredFlags},
            sigverify_shreds::{verify_shred_cpu, LruCache},
        },
        solana_perf::packet::PacketFlags,
        solana_sdk::signature::{Keypair, Signer},
        std::{
            collections::HashMap,
            net::{IpAddr, Ipv4Addr},
            sync::RwLock,
        },
    };

    fn run_test_sigverify_shred_cpu_repair(slot: Slot) {
        solana_logger::setup();
        let cache = RwLock::new(LruCache::new(/*capacity:*/ 128));
        let mut shred = Shred::new_from_data(
            slot,
            0xc0de,
            0xdead,
            &[1, 2, 3, 4],
            ShredFlags::LAST_SHRED_IN_SLOT,
            0,
            0,
            0xc0de,
        );
        assert_eq!(shred.slot(), slot);
        let keypair = Keypair::new();
        shred.sign(&keypair);
        trace!("signature {}", shred.signature());
        let nonce = 9;
        let mut packet = repair_response_packet_from_bytes(
            Payload::unwrap_or_clone(shred.into_payload()),
            &SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            nonce,
        )
        .unwrap();
        packet.meta_mut().flags |= PacketFlags::REPAIR;

        let leader_slots = HashMap::from([(slot, keypair.pubkey())]);
        assert!(verify_shred_cpu(&packet, &leader_slots, &cache));

        let wrong_keypair = Keypair::new();
        let leader_slots = HashMap::from([(slot, wrong_keypair.pubkey())]);
        assert!(!verify_shred_cpu(&packet, &leader_slots, &cache));

        let leader_slots = HashMap::new();
        assert!(!verify_shred_cpu(&packet, &leader_slots, &cache));
    }

    #[test]
    fn test_sigverify_shred_cpu_repair() {
        run_test_sigverify_shred_cpu_repair(0xdead_c0de);
    }
}
