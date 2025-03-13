//! The `packet` module defines data structures and methods to pull data from the network.
pub use solana_packet::{self, Meta, Packet, PacketFlags, PACKET_DATA_SIZE};
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

#[derive(Clone, Debug)]
pub enum GenericPacket {
    Packet(Packet),
    TpuPacket(TpuPacket),
}

impl PacketRead for GenericPacket {
    fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        match self {
            Self::Packet(packet) => packet.data(index),
            Self::TpuPacket(packet) => packet.data(index),
        }
    }

    fn meta(&self) -> &Meta {
        match self {
            Self::Packet(packet) => packet.meta(),
            Self::TpuPacket(packet) => packet.meta(),
        }
    }

    fn meta_mut(&mut self) -> &mut Meta {
        match self {
            Self::Packet(packet) => packet.meta_mut(),
            Self::TpuPacket(packet) => packet.meta_mut(),
        }
    }

    fn size(&self) -> usize {
        match self {
            Self::Packet(packet) => packet.size(),
            Self::TpuPacket(packet) => packet.size(),
        }
    }
}

impl PacketRead for Packet {
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

    fn size(&self) -> usize {
        self.meta().size
    }
}

/// Creates a [`BytesMut`] buffer and [`Meta`] from the given serializable
/// `data`.
fn from_data<T>(dest: Option<&SocketAddr>, data: T) -> bincode::Result<(BytesMut, Meta)>
where
    T: solana_packet::Encode,
{
    let buffer = BytesMut::with_capacity(PACKET_DATA_SIZE);
    let mut writer = buffer.writer();
    data.encode(&mut writer)?;
    let buffer = writer.into_inner();
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
    Ok((buffer, meta))
}

/// Representation of a packet used in the TPU.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TpuPacket {
    buffer: Bytes,
    meta: Meta,
}

impl TpuPacket {
    pub fn new(buffer: Bytes, meta: Meta) -> Self {
        Self { buffer, meta }
    }

    pub fn from_data<T>(dest: Option<&SocketAddr>, data: T) -> bincode::Result<Self>
    where
        T: solana_packet::Encode,
    {
        let (buffer, meta) = from_data(dest, data)?;
        let buffer = buffer.freeze();
        Ok(Self { buffer, meta })
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TpuPacketBatch {
    packets: Vec<TpuPacket>,
}

impl Deref for TpuPacketBatch {
    type Target = Vec<TpuPacket>;

    fn deref(&self) -> &Self::Target {
        &self.packets
    }
}

#[derive(Debug)]
pub enum GenericPacketBatch {
    PacketBatch(PacketBatch),
    TpuPacketBatch(TpuPacketBatch),
}

impl GenericPacketBatch {
    pub fn iter(&self) -> impl Iterator<Item = GenericPacket> {
        match self {
            Self::PacketBatch(packet_batch) => packet_batch
                .iter()
                .map(|packet| &GenericPacket::Packet(*packet)),
            Self::TpuPacketBatch(packet_batch) => packet_batch
                .iter()
                .map(|packet| &GenericPacket::TpuPacket(*packet)),
        }
    }
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PacketBatch {
    packets: PinnedVec<Packet>,
}

pub type PacketBatchRecycler = Recycler<PinnedVec<Packet>>;

impl PacketBatch {
    pub fn new(packets: Vec<Packet>) -> Self {
        let packets = PinnedVec::from_vec(packets);
        Self { packets }
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
        batch
            .packets
            .resize(dests_and_data.len(), Packet::default());

        for ((addr, data), packet) in dests_and_data.zip(batch.packets.iter_mut()) {
            let addr = addr.borrow();
            if !addr.ip().is_unspecified() && addr.port() != 0 {
                if let Err(e) = Packet::populate_packet(packet, Some(addr), &data) {
                    // TODO: This should never happen. Instead the caller should
                    // break the payload into smaller messages, and here any errors
                    // should be propagated.
                    error!("Couldn't write to packet {:?}. Data skipped.", e);
                    packet.meta_mut().set_discard(true);
                }
            } else {
                trace!("Dropping packet, as destination is unknown");
                packet.meta_mut().set_discard(true);
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

    pub fn resize(&mut self, new_len: usize, value: Packet) {
        self.packets.resize(new_len, value)
    }

    pub fn truncate(&mut self, len: usize) {
        self.packets.truncate(len);
    }

    pub fn push(&mut self, packet: Packet) {
        self.packets.push(packet);
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

impl Deref for PacketBatch {
    type Target = [Packet];

    fn deref(&self) -> &Self::Target {
        &self.packets
    }
}

impl DerefMut for PacketBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.packets
    }
}

impl<I: SliceIndex<[Packet]>> Index<I> for PacketBatch {
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        &self.packets[index]
    }
}

impl<I: SliceIndex<[Packet]>> IndexMut<I> for PacketBatch {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.packets[index]
    }
}

impl<'a> IntoIterator for &'a PacketBatch {
    type Item = &'a Packet;
    type IntoIter = Iter<'a, Packet>;

    fn into_iter(self) -> Self::IntoIter {
        self.packets.iter()
    }
}

impl<'a> IntoParallelIterator for &'a PacketBatch {
    type Iter = rayon::slice::Iter<'a, Packet>;
    type Item = &'a Packet;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter()
    }
}

impl<'a> IntoParallelIterator for &'a mut PacketBatch {
    type Iter = rayon::slice::IterMut<'a, Packet>;
    type Item = &'a mut Packet;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter_mut()
    }
}

impl From<PacketBatch> for Vec<Packet> {
    fn from(batch: PacketBatch) -> Self {
        batch.packets.into()
    }
}

impl<'a, I> From<I> for PacketBatch
where
    I: IntoIterator<Item = &'a TpuPacket>,
{
    fn from(tpu_packets: I) -> Self {
        let recycler = PacketBatchRecycler::default();
        let mut packet_batch =
            PacketBatch::new_with_recycler(&recycler, PACKETS_PER_BATCH, "quic_packet_coalescer");
        for tpu_packet in tpu_packets {
            let mut packet = Packet::default();
            let size = tpu_packet.size();
            if let Some(data) = tpu_packet.data(..) {
                packet.buffer_mut()[0..size].copy_from_slice(data);
            }
            *packet.meta_mut() = tpu_packet.meta.clone();
            packet.meta_mut().size = size;

            packet_batch.push(packet);
        }
        packet_batch
    }
}

pub fn to_packet_batches<T: Serialize>(items: &[T], chunk_size: usize) -> Vec<PacketBatch> {
    items
        .chunks(chunk_size)
        .map(|batch_items| {
            let mut batch = PacketBatch::with_capacity(batch_items.len());
            batch.resize(batch_items.len(), Packet::default());
            for (item, packet) in batch_items.iter().zip(batch.packets.iter_mut()) {
                Packet::populate_packet(packet, None, item).expect("serialize request");
            }
            batch
        })
        .collect()
}

#[cfg(test)]
fn to_packet_batches_for_tests<T: Serialize>(items: &[T]) -> Vec<PacketBatch> {
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
            let _first_packets = PacketBatch::new_with_recycler(&recycler, i + 1, "first one");
        }
    }
}
