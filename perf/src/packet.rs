//! The `packet` module defines data structures and methods to pull data from the network.
pub use solana_packet::{
    self, BufMut, Meta, Packet, PacketArray, PacketFlags, PacketMut, PacketRead, PACKET_DATA_SIZE,
};
use {
    crate::{cuda_runtime::PinnedVec, recycler::Recycler},
    bincode::config::Options,
    // rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator},
    serde::{de::DeserializeOwned, Serialize},
    std::{
        io::Read,
        net::SocketAddr,
        ops::{Index, IndexMut},
        slice::{Iter, IterMut, SliceIndex},
    },
};

pub const NUM_PACKETS: usize = 1024 * 8;

pub const PACKETS_PER_BATCH: usize = 64;
pub const NUM_RCVMMSGS: usize = 64;

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug, Default, Clone)]
pub struct PacketArrayBatch {
    packets: PinnedVec<PacketArray>,
}

pub type PacketBatchRecycler = Recycler<PinnedVec<PacketArray>>;

impl PacketArrayBatch {
    pub fn new(packets: Vec<Packet>) -> Self {
        let mut batch = Self::with_capacity(packets.len());
        for packet in packets {
            let packet: PacketArray = packet.into();
            batch.push(packet);
        }
        batch
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let packets = PinnedVec::with_capacity(capacity);
        Self { packets }
    }

    pub fn new_pinned_with_capacity(capacity: usize) -> Self {
        let mut batch = Self::with_capacity(capacity);
        batch.packets.reserve_and_pin(capacity);
        batch
    }

    pub fn new_unpinned_with_recycler(
        recycler: &PacketBatchRecycler,
        capacity: usize,
        name: &'static str,
    ) -> Self {
        let mut packets = recycler.allocate(name);
        packets.reserve(capacity);
        Self { packets }
    }

    pub fn new_with_recycler(
        recycler: &PacketBatchRecycler,
        capacity: usize,
        name: &'static str,
    ) -> Self {
        let mut packets = recycler.allocate(name);
        packets.reserve_and_pin(capacity);
        Self { packets }
    }

    pub fn new_with_recycler_data(
        recycler: &PacketBatchRecycler,
        name: &'static str,
        packets: Vec<Packet>,
    ) -> Self {
        let mut batch = Self::new_with_recycler(recycler, packets.len(), name);
        for packet in packets {
            let packet: PacketArray = packet.into();
            batch.push(packet);
        }
        batch
    }

    // TODO: Evaluate a need of this method.
    pub fn new_unpinned_with_recycler_data_and_dests<T: serde::Serialize>(
        recycler: &PacketBatchRecycler,
        name: &'static str,
        dests_and_data: &[(SocketAddr, T)],
    ) -> Self {
        let mut batch = Self::new_unpinned_with_recycler(recycler, dests_and_data.len(), name);

        for (addr, data) in dests_and_data.iter() {
            let packet = if !addr.ip().is_unspecified() && addr.port() != 0 {
                match Packet::from_data(Some(addr), data) {
                    Ok(packet) => packet,
                    Err(e) => {
                        // TODO: This should never happen. Instead the caller should
                        // break the payload into smaller messages, and here any errors
                        // should be propagated.
                        error!("Couldn't write to packet {:?}. Data skipped.", e);
                        let mut packet = Packet::default();
                        packet.meta_mut().set_discard(true);
                        packet
                    }
                }
            } else {
                trace!("Dropping packet, as destination is unknown");
                let mut packet = Packet::default();
                packet.meta_mut().set_discard(true);
                packet
            };
            let packet: PacketArray = packet.into();
            batch.push(packet);
        }
        batch
    }

    pub fn new_unpinned_with_recycler_data(
        recycler: &PacketBatchRecycler,
        name: &'static str,
        packets: Vec<Packet>,
    ) -> Self {
        let mut batch = Self::new_unpinned_with_recycler(recycler, packets.len(), name);
        for packet in packets {
            let packet: PacketArray = packet.into();
            batch.push(packet);
        }
        batch
    }

    pub fn resize(&mut self, new_len: usize, value: PacketArray) {
        self.packets.resize(new_len, value)
    }

    pub fn truncate(&mut self, len: usize) {
        self.packets.truncate(len);
    }

    pub fn push(&mut self, packet: PacketArray) {
        self.packets.push(packet);
    }

    // TODO: Is this needed?
    // pub fn set_addr(&mut self, addr: &SocketAddr) {
    //     for p in self.iter_mut() {
    //         p.meta_mut().set_socket_addr(addr);
    //     }
    // }

    pub fn len(&self) -> usize {
        self.packets.len()
    }

    pub fn capacity(&self) -> usize {
        self.packets.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    pub fn as_ptr(&self) -> *const PacketArray {
        self.packets.as_ptr()
    }

    pub fn iter(&self) -> Iter<'_, PacketArray> {
        self.packets.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, PacketArray> {
        self.packets.iter_mut()
    }

    /// See Vector::set_len() for more details
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to [`self.capacity`].
    /// - The elements at `old_len..new_len` must be initialized. Packet data
    ///   will likely be overwritten when populating the packet, but the meta
    ///   should specifically be initialized to known values.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        self.packets.set_len(new_len);
    }
}

impl<I: SliceIndex<[PacketArray]>> Index<I> for PacketArrayBatch {
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        &self.packets[index]
    }
}

impl<I: SliceIndex<[PacketArray]>> IndexMut<I> for PacketArrayBatch {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.packets[index]
    }
}

impl<'a> IntoIterator for &'a PacketArrayBatch {
    type Item = &'a PacketArray;
    type IntoIter = Iter<'a, PacketArray>;

    fn into_iter(self) -> Self::IntoIter {
        self.packets.into_iter()
    }
}

// impl<'a> IntoParallelIterator for &'a PacketArrayBatch {
//     type Iter = rayon::slice::Iter<'a, PacketArray>;
//     type Item = &'a Packet;
//     fn into_par_iter(self) -> Self::Iter {
//         self.packets.par_iter()
//     }
// }
//
// impl<'a> IntoParallelIterator for &'a mut PacketArrayBatch {
//     type Iter = rayon::slice::IterMut<'a, PacketArray>;
//     type Item = &'a mut Packet;
//     fn into_par_iter(self) -> Self::Iter {
//         self.packets.par_iter_mut()
//     }
// }

// impl From<PacketArrayBatch> for Vec<Packet> {
//     fn from(batch: PacketArrayBatch) -> Self {
//         batch
//             .packets
//             .into_iter()
//             .map(|packet| (*packet).into())
//             .collect()
//     }
// }

pub fn to_packet_batches<T>(items: &[T], chunk_size: usize) -> Vec<Vec<Packet>>
where
    T: Serialize,
{
    items
        .chunks(chunk_size)
        .map(|batch_items| {
            let mut batch = Vec::with_capacity(batch_items.len());
            for item in batch_items.iter() {
                let packet = Packet::from_data(None, item).expect("serialize request");
                batch.push(packet);
            }
            batch
        })
        .collect()
}

#[cfg(test)]
fn to_packet_batches_for_tests<T: Serialize>(items: &[T]) -> Vec<Vec<Packet>> {
    to_packet_batches(items, NUM_PACKETS)
}

pub fn deserialize_from_with_limit<R, T>(reader: R) -> bincode::Result<T>
where
    R: Read,
    T: DeserializeOwned,
{
    // with_limit causes pre-allocation size to be limited
    // to prevent against memory exhaustion attacks.
    bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(reader)
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_hash::Hash, solana_keypair::Keypair, solana_signer::Signer,
        solana_system_transaction::transfer,
    };

    #[test]
    fn test_to_packet_batches() {
        let keypair = Keypair::new();
        let hash = Hash::new_from_array([1; 32]);
        let tx = transfer(&keypair, &keypair.pubkey(), 1, hash);
        let rv = to_packet_batches_for_tests(&[tx.clone(); 1]);
        assert_eq!(rv.len(), 1);
        assert_eq!(rv[0].len(), 1);

        #[allow(clippy::useless_vec)]
        let rv = to_packet_batches_for_tests(&vec![tx.clone(); NUM_PACKETS]);
        assert_eq!(rv.len(), 1);
        assert_eq!(rv[0].len(), NUM_PACKETS);

        #[allow(clippy::useless_vec)]
        let rv = to_packet_batches_for_tests(&vec![tx; NUM_PACKETS + 1]);
        assert_eq!(rv.len(), 2);
        assert_eq!(rv[0].len(), NUM_PACKETS);
        assert_eq!(rv[1].len(), 1);
    }

    #[test]
    fn test_to_packets_pinning() {
        let recycler = PacketBatchRecycler::default();
        for i in 0..2 {
            let _first_packets = PacketArrayBatch::new_with_recycler(&recycler, i + 1, "first one");
        }
    }
}
