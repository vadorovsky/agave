//! The `packet` module defines data structures and methods to pull data from the network.
pub use solana_packet::{self, Meta, Packet as PinnedPacket, PacketFlags, PACKET_DATA_SIZE};
use {
    crate::{cuda_runtime::PinnedVec, recycler::Recycler},
    bincode::config::Options,
    bytes::{BufMut, Bytes, BytesMut},
    rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator},
    serde::{de::DeserializeOwned, Deserialize, Serialize},
    std::{
        borrow::Borrow,
        io::Read,
        net::SocketAddr,
        ops::{Deref, DerefMut, Index, IndexMut},
        slice::{Iter, IterMut, SliceIndex},
    },
};

pub const NUM_PACKETS: usize = 1024 * 8;

pub const PACKETS_PER_BATCH: usize = 64;
pub const NUM_RCVMMSGS: usize = 64;

/// Read data and metadata from a packet.
pub trait PacketRead {
    fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>;
    fn meta(&self) -> &Meta;
    fn meta_mut(&mut self) -> &mut Meta;
    fn size(&self) -> usize;

    fn deserialize_slice<T, I>(&self, index: I) -> bincode::Result<T>
    where
        T: serde::de::DeserializeOwned,
        I: SliceIndex<[u8], Output = [u8]>,
    {
        let bytes = self.data(index).ok_or(bincode::ErrorKind::SizeLimit)?;
        bincode::options()
            .with_limit(PACKET_DATA_SIZE as u64)
            .with_fixint_encoding()
            .reject_trailing_bytes()
            .deserialize(bytes)
    }
}

#[repr(C)]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Packet {
    Pinned(PinnedPacket),
    Tpu(TpuPacket),
}

impl Default for Packet {
    fn default() -> Self {
        Self::Pinned(PinnedPacket::default())
    }
}

impl PacketRead for Packet {
    fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        match self {
            Self::Pinned(packet) => packet.data(index),
            Self::Tpu(packet) => packet.data(index),
        }
    }

    #[inline]
    fn meta(&self) -> &Meta {
        match self {
            Self::Pinned(packet) => packet.meta(),
            Self::Tpu(packet) => packet.meta(),
        }
    }

    #[inline]
    fn meta_mut(&mut self) -> &mut Meta {
        match self {
            Self::Pinned(packet) => packet.meta_mut(),
            Self::Tpu(packet) => packet.meta_mut(),
        }
    }

    #[inline]
    fn size(&self) -> usize {
        match self {
            Self::Pinned(packet) => packet.size(),
            Self::Tpu(packet) => packet.size(),
        }
    }
}

impl PacketRead for PinnedPacket {
    fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        self.data(index)
    }

    #[inline]
    fn meta(&self) -> &Meta {
        self.meta()
    }

    #[inline]
    fn meta_mut(&mut self) -> &mut Meta {
        self.meta_mut()
    }

    #[inline]
    fn size(&self) -> usize {
        self.meta().size
    }
}

/// Representation of a packet used in TPU.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TpuPacket {
    buffer: Bytes,
    meta: Meta,
}

impl TpuPacket {
    pub fn new(buffer: Bytes, meta: Meta) -> Packet {
        Packet::Tpu(Self { buffer, meta })
    }

    pub fn from_data<T>(dest: Option<&SocketAddr>, data: T) -> bincode::Result<Packet>
    where
        T: solana_packet::Encode,
    {
        let buffer = BytesMut::with_capacity(PACKET_DATA_SIZE);
        let mut writer = buffer.writer();
        data.encode(&mut writer)?;
        let buffer = writer.into_inner();
        let buffer = buffer.freeze();

        let mut meta = Meta::default();
        // We don't use the `size` field of `Meta` in `TpuPacket`/`TpuPacketMut`.
        // Instead, we rely on the length of the buffers. There is no need for
        // tracking the size manually.
        // However, removing the `size` field from `Meta` would break ABI and the
        // `Packet` struct. Maintaining multiple `Meta` implementations would add
        // more maintenance burden.
        meta.size = usize::MAX;
        if let Some(dest) = dest {
            meta.set_socket_addr(dest);
        }

        Ok(Packet::Tpu(Self { buffer, meta }))
    }
}

impl PacketRead for TpuPacket {
    fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        if self.meta.discard() {
            None
        } else {
            self.buffer.get(index)
        }
    }

    #[inline]
    fn meta(&self) -> &Meta {
        &self.meta
    }

    #[inline]
    fn meta_mut(&mut self) -> &mut Meta {
        &mut self.meta
    }

    #[inline]
    fn size(&self) -> usize {
        self.buffer.len()
    }
}

#[derive(Clone)]
pub enum PacketBatch {
    Pinned(PinnedPacketBatch),
    Unpinned(UnpinnedPacketBatch),
}

impl PacketBatch {
    pub fn as_ptr(&self) -> *const Packet {
        match self {
            Self::Pinned(batch) => batch.as_ptr(),
            Self::Unpinned(batch) => batch.as_ptr(),
        }
    }
}

impl Deref for PacketBatch {
    type Target = Vec<Packet>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Pinned(batch) => batch.packets.deref(),
            Self::Unpinned(batch) => batch.deref(),
        }
    }
}

impl DerefMut for PacketBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Pinned(batch) => batch.packets.deref_mut(),
            Self::Unpinned(batch) => batch.deref_mut(),
        }
    }
}

impl<'a> IntoParallelIterator for &'a PacketBatch {
    type Iter = rayon::slice::Iter<'a, Packet>;
    type Item = &'a Packet;

    fn into_par_iter(self) -> Self::Iter {
        self.deref().into_par_iter()
    }
}

impl<'a> IntoParallelIterator for &'a mut PacketBatch {
    type Iter = rayon::slice::IterMut<'a, Packet>;
    type Item = &'a mut Packet;

    fn into_par_iter(self) -> Self::Iter {
        self.deref_mut().into_par_iter()
    }
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PinnedPacketBatch {
    packets: PinnedVec<Packet>,
}

pub type PacketBatchRecycler = Recycler<PinnedVec<Packet>>;

impl PinnedPacketBatch {
    pub fn new(packets: Vec<Packet>) -> PacketBatch {
        let packets = PinnedVec::from_vec(packets);
        PacketBatch::Pinned(Self { packets })
    }

    pub fn with_capacity(capacity: usize) -> PacketBatch {
        let packets = PinnedVec::with_capacity(capacity);
        PacketBatch::Pinned(Self { packets })
    }

    pub fn new_pinned_with_capacity(capacity: usize) -> PacketBatch {
        let mut packets = PinnedVec::with_capacity(capacity);
        packets.reserve_and_pin(capacity);
        PacketBatch::Pinned(Self { packets })
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
        mut packets: Vec<Packet>,
    ) -> Self {
        let mut batch = Self::new_with_recycler(recycler, packets.len(), name);
        batch.packets.append(&mut packets);
        batch
    }

    pub fn new_unpinned_with_recycler_data_and_dests<S, T>(
        recycler: &PacketBatchRecycler,
        name: &'static str,
        dests_and_data: impl IntoIterator<Item = (S, T), IntoIter: ExactSizeIterator>,
    ) -> Self
    where
        S: Borrow<SocketAddr>,
        T: solana_packet::Encode,
    {
        let dests_and_data = dests_and_data.into_iter();
        let mut batch = Self::new_unpinned_with_recycler(recycler, dests_and_data.len(), name);

        for (addr, data) in dests_and_data {
            let addr = addr.borrow();
            if !addr.ip().is_unspecified() && addr.port() != 0 {
                let packet = match PinnedPacket::from_data(Some(addr), data) {
                    Ok(packet) => packet,
                    Err(e) => {
                        error!("Couldn't write to packet {:?}. Data skipped.", e);
                        let mut packet = PinnedPacket::default();
                        packet.meta_mut().set_discard(true);
                        packet
                    }
                };
                batch.push(packet);
            } else {
                trace!("Dropping packet, as destination is unknown");
                let mut packet = PinnedPacket::default();
                packet.meta_mut().set_discard(true);
                batch.push(packet);
            }
        }
        batch
    }

    pub fn new_unpinned_with_recycler_data(
        recycler: &PacketBatchRecycler,
        name: &'static str,
        mut packets: Vec<Packet>,
    ) -> Self {
        let mut batch = Self::new_unpinned_with_recycler(recycler, packets.len(), name);
        batch.packets.append(&mut packets);
        batch
    }

    pub fn resize(&mut self, new_len: usize, value: PinnedPacket) {
        self.packets.resize(new_len, Packet::Pinned(value))
    }

    pub fn truncate(&mut self, len: usize) {
        self.packets.truncate(len);
    }

    pub fn push(&mut self, packet: PinnedPacket) {
        self.packets.push(Packet::Pinned(packet));
    }

    pub fn set_addr(&mut self, addr: &SocketAddr) {
        for p in self.iter_mut() {
            p.meta_mut().set_socket_addr(addr);
        }
    }

    pub fn len(&self) -> usize {
        self.packets.len()
    }

    pub fn capacity(&self) -> usize {
        self.packets.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    pub fn as_ptr(&self) -> *const Packet {
        self.packets.as_ptr()
    }

    pub fn iter(&self) -> Iter<'_, Packet> {
        self.packets.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, Packet> {
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

impl<I: SliceIndex<[Packet]>> Index<I> for PinnedPacketBatch {
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        &self.packets[index]
    }
}

impl<I: SliceIndex<[Packet]>> IndexMut<I> for PinnedPacketBatch {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.packets[index]
    }
}

impl<'a> IntoIterator for &'a PinnedPacketBatch {
    type Item = &'a Packet;
    type IntoIter = Iter<'a, Packet>;

    fn into_iter(self) -> Self::IntoIter {
        self.packets.iter()
    }
}

impl<'a> IntoParallelIterator for &'a PinnedPacketBatch {
    type Iter = rayon::slice::Iter<'a, Packet>;
    type Item = &'a Packet;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter()
    }
}

impl<'a> IntoParallelIterator for &'a mut PinnedPacketBatch {
    type Iter = rayon::slice::IterMut<'a, Packet>;
    type Item = &'a mut Packet;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter_mut()
    }
}

impl From<PinnedPacketBatch> for Vec<Packet> {
    fn from(batch: PinnedPacketBatch) -> Self {
        batch.packets.into()
    }
}

pub fn to_packet_batches<T: Serialize>(items: &[T], chunk_size: usize) -> Vec<PacketBatch> {
    items
        .chunks(chunk_size)
        .map(|batch_items| {
            let mut batch = PinnedPacketBatch::with_capacity(batch_items.len());
            for item in batch_items.iter() {
                let packet =
                    Packet::Pinned(PinnedPacket::from_data(None, item).expect("serialize request"));
                batch.push(packet);
            }
            batch
        })
        .collect()
}

#[cfg(test)]
fn to_packet_batches_for_tests<T: Serialize>(items: &[T]) -> Vec<PacketBatch> {
    to_packet_batches(items, NUM_PACKETS)
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UnpinnedPacketBatch {
    packets: Vec<Packet>,
}

impl UnpinnedPacketBatch {
    pub fn with_capacity(capacity: usize) -> PacketBatch {
        let packets = Vec::with_capacity(capacity);
        PacketBatch::Unpinned(Self { packets })
    }
}

impl Deref for UnpinnedPacketBatch {
    type Target = Vec<Packet>;

    fn deref(&self) -> &Self::Target {
        &self.packets
    }
}

impl DerefMut for UnpinnedPacketBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.packets
    }
}

impl FromIterator<Packet> for UnpinnedPacketBatch {
    fn from_iter<T: IntoIterator<Item = Packet>>(iter: T) -> Self {
        let packets = Vec::from_iter(iter);
        Self { packets }
    }
}

impl<'a> IntoIterator for &'a UnpinnedPacketBatch {
    type Item = &'a Packet;
    type IntoIter = Iter<'a, Packet>;

    fn into_iter(self) -> Self::IntoIter {
        self.packets.iter()
    }
}

impl<'a> IntoParallelIterator for &'a UnpinnedPacketBatch {
    type Iter = rayon::slice::Iter<'a, Packet>;
    type Item = &'a Packet;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter()
    }
}

impl<'a> IntoParallelIterator for &'a mut UnpinnedPacketBatch {
    type Iter = rayon::slice::IterMut<'a, Packet>;
    type Item = &'a mut Packet;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter_mut()
    }
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
            let _first_packets =
                PinnedPacketBatch::new_with_recycler(&recycler, i + 1, "first one");
        }
    }
}
