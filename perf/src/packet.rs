//! The `packet` module defines data structures and methods to pull data from the network.
#[cfg(feature = "stable-abi")]
use solana_frozen_abi_macro::{StableAbi, StableAbiSample};
#[cfg(feature = "dev-context-only-utils")]
use wincode::{ReadError, ReadResult, SchemaRead, config::DefaultConfig};
use {
    bitflags::bitflags,
    bytes::Bytes,
    rayon::prelude::{IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator},
    serde::{Deserialize, Serialize},
    solana_pubkey::Pubkey,
    std::{
        io::Cursor,
        net::{IpAddr, SocketAddr},
        ops::{Deref, DerefMut},
        slice::{Iter, SliceIndex},
    },
    wincode::{
        SchemaWrite, WriteResult,
        config::{Config, Configuration},
    },
};
pub use {
    bytes,
    solana_packet::{self, Meta, PACKET_DATA_SIZE, Packet, PacketFlags as LegacyPacketFlags},
};

bitflags! {
    #[repr(C)]
    #[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct PacketFlags: u8 {
        const DISCARD          = 0b0000_0001;
        const FORWARDED        = 0b0000_0010;
        const REPAIR           = 0b0000_0100;
        const SIMPLE_VOTE_TX   = 0b0000_1000;
        const UNUSED_0         = 0b0001_0000;
        const UNUSED_1         = 0b0010_0000;
        const PERF_TRACK_PACKET = 0b0100_0000;
        const FROM_STAKED_NODE = 0b1000_0000;
    }
}

#[cfg(feature = "frozen-abi")]
impl ::solana_frozen_abi::abi_example::AbiExample for PacketFlags {
    fn example() -> Self {
        Self::empty()
    }
}

#[cfg(feature = "frozen-abi")]
impl ::solana_frozen_abi::abi_example::TransparentAsHelper for PacketFlags {}

#[cfg(feature = "frozen-abi")]
impl ::solana_frozen_abi::abi_example::EvenAsOpaque for PacketFlags {
    const TYPE_NAME_MATCHER: &'static str = "::_::InternalBitFlags";
}

#[cfg(feature = "frozen-abi")]
impl ::solana_frozen_abi::stable_abi::StableAbi for PacketFlags {
    fn random_with_context(
        rng: &mut (impl ::solana_frozen_abi::rand::RngCore + ?Sized),
        _ctx: (),
    ) -> Self {
        Self::from_bits_truncate(::solana_frozen_abi::rand::Rng::random(rng))
    }
}

pub const NUM_PACKETS: usize = 1024 * 8;

pub const PACKETS_PER_BATCH: usize = 64;
pub const NUM_RCVMMSGS: usize = 64;

pub type PacketConfig = Configuration<true, PACKET_DATA_SIZE>;

/// wincode configuration setup to use for deserializing a Packet.
/// - Zero-copy alignment check is enabled.
/// - Preallocation size limit is PACKET_DATA_SIZE.
#[inline]
const fn packet_config_inner() -> PacketConfig {
    Configuration::default().with_preallocation_size_limit::<{ solana_packet::PACKET_DATA_SIZE }>()
}

#[inline]
pub const fn packet_config() -> impl Config {
    packet_config_inner()
}

#[cfg(feature = "dev-context-only-utils")]
pub fn deserialize_slice_from_packet<'de, T, I>(packet: &'de Packet, index: I) -> ReadResult<T>
where
    T: SchemaRead<'de, PacketConfig, Dst = T>,
    I: SliceIndex<[u8], Output = [u8]>,
{
    let data = packet
        .data(index)
        .ok_or(ReadError::Custom("packet discarded"))?;
    wincode::config::deserialize(data, packet_config_inner())
}

pub fn packet_from_data<T>(dest: Option<&SocketAddr>, data: T) -> WriteResult<Packet>
where
    T: SchemaWrite<PacketConfig, Src = T>,
{
    let mut packet = Packet::default();
    let mut wr = Cursor::new(packet.buffer_mut());
    wincode::config::serialize_into(&mut wr, &data, packet_config_inner())?;
    packet.meta_mut().size = wr.position() as usize;
    if let Some(dest) = dest {
        packet.meta_mut().set_socket_addr(dest);
    }
    Ok(packet)
}

/// Serialize `data` into a freshly allocated [`BytesPacket`].
///
/// Like [`packet_from_data`], serialization is bounded to [`PACKET_DATA_SIZE`], so oversized
/// payloads fail instead of producing a packet that cannot be sent.
pub fn bytes_packet_from_data<T>(dest: Option<&SocketAddr>, data: T) -> WriteResult<BytesPacket>
where
    T: SchemaWrite<PacketConfig, Src = T>,
{
    bytes_packet_from_data_with_config(dest, data, packet_config_inner())
}

/// Like [`bytes_packet_from_data`], but serializes with a caller-provided wincode `config`.
///
/// Wincode's preallocation limit bounds in-memory collection size (`len * size_of::<T>()`),
/// not serialized bytes, so the default [`PacketConfig`] rejects payloads whose sequences
/// are small on the wire but large in memory. Output is still bounded to [`PACKET_DATA_SIZE`]
/// by the destination buffer.
pub fn bytes_packet_from_data_with_config<T, C>(
    dest: Option<&SocketAddr>,
    data: T,
    config: C,
) -> WriteResult<BytesPacket>
where
    C: Config,
    T: SchemaWrite<C, Src = T>,
{
    let mut buffer = [0u8; PACKET_DATA_SIZE];
    let mut wr = Cursor::new(buffer.as_mut_slice());
    wincode::config::serialize_into(&mut wr, &data, config)?;
    let size = wr.position() as usize;
    let buffer = Bytes::copy_from_slice(&buffer[..size]);
    Ok(match dest {
        Some(dest) => BytesPacket::new_with_addr_and_port(buffer, dest.ip(), dest.port()),
        None => BytesPacket::new(buffer),
    })
}

/// Representation of a packet used in TPU.
#[cfg_attr(feature = "stable-abi", derive(StableAbi, StableAbiSample))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BytesPacket {
    #[cfg_attr(
        feature = "stable-abi",
        stable_abi_sample(with = "solana_frozen_abi::stable_abi::sample_collection(rng)")
    )]
    buffer: Bytes,
    size: usize,
    addr: Option<IpAddr>,
    port: Option<u16>,
    flags: PacketFlags,
    remote_pubkey: Option<Pubkey>,
}

impl BytesPacket {
    pub fn new(buffer: Bytes) -> Self {
        let size = buffer.len();
        Self {
            buffer,
            size,
            addr: None,
            port: None,
            flags: PacketFlags::empty(),
            remote_pubkey: None,
        }
    }

    pub fn new_with_addr_and_port(buffer: Bytes, addr: IpAddr, port: u16) -> Self {
        Self {
            addr: Some(addr),
            port: Some(port),
            ..Self::new(buffer)
        }
    }

    pub fn new_with_socket_addr(buffer: Bytes, socket_addr: &SocketAddr) -> Self {
        Self::new_with_addr_and_port(buffer, socket_addr.ip(), socket_addr.port())
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn empty() -> Self {
        Self::new(Bytes::new())
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn from_bytes(dest: Option<&SocketAddr>, buffer: impl Into<Bytes>) -> Self {
        let buffer = buffer.into();
        match dest {
            Some(dest) => Self::new_with_socket_addr(buffer, dest),
            None => Self::new(buffer),
        }
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn from_data<T>(data: T) -> WriteResult<Self>
    where
        T: SchemaWrite<DefaultConfig, Src = T>,
    {
        let buffer = Bytes::from(wincode::serialize(&data)?);
        Ok(Self::new(buffer))
    }

    #[inline]
    pub fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        if self.discard() {
            None
        } else {
            self.buffer.get(..self.size)?.get(index)
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    #[inline]
    #[cfg(feature = "dev-context-only-utils")]
    pub fn set_size(&mut self, size: usize) {
        self.size = size;
    }

    #[inline]
    pub fn addr(&self) -> Option<IpAddr> {
        self.addr
    }

    #[inline]
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    #[inline]
    pub fn socket_addr(&self) -> Option<SocketAddr> {
        Some(SocketAddr::new(self.addr?, self.port?))
    }

    #[inline]
    pub fn set_socket_addr(&mut self, socket_addr: &SocketAddr) {
        self.addr = Some(socket_addr.ip());
        self.port = Some(socket_addr.port());
    }

    #[inline]
    pub fn flags(&self) -> PacketFlags {
        self.flags
    }

    #[inline]
    pub fn set_flags(&mut self, flags: PacketFlags) {
        self.flags = flags;
    }

    #[inline]
    pub fn insert_flags(&mut self, flags: PacketFlags) {
        self.flags.insert(flags);
    }

    #[inline]
    pub fn remove_flags(&mut self, flags: PacketFlags) {
        self.flags.remove(flags);
    }

    #[inline]
    pub fn discard(&self) -> bool {
        self.flags.contains(PacketFlags::DISCARD)
    }

    #[inline]
    pub fn set_discard(&mut self, discard: bool) {
        self.flags.set(PacketFlags::DISCARD, discard);
    }

    #[inline]
    pub fn forwarded(&self) -> bool {
        self.flags.contains(PacketFlags::FORWARDED)
    }

    #[inline]
    pub fn repair(&self) -> bool {
        self.flags.contains(PacketFlags::REPAIR)
    }

    #[inline]
    pub fn is_simple_vote_tx(&self) -> bool {
        self.flags.contains(PacketFlags::SIMPLE_VOTE_TX)
    }

    #[inline]
    pub fn is_from_staked_node(&self) -> bool {
        self.flags.contains(PacketFlags::FROM_STAKED_NODE)
    }

    #[inline]
    pub fn set_from_staked_node(&mut self, from_staked_node: bool) {
        self.flags
            .set(PacketFlags::FROM_STAKED_NODE, from_staked_node);
    }

    #[inline]
    pub fn remote_pubkey(&self) -> Option<Pubkey> {
        self.remote_pubkey
    }

    #[inline]
    pub fn set_remote_pubkey(&mut self, remote_pubkey: Option<Pubkey>) {
        self.remote_pubkey = remote_pubkey;
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn copy_from_slice(&mut self, slice: &[u8]) {
        self.buffer = Bytes::from(slice.to_vec());
        self.size = slice.len();
    }

    #[inline]
    pub fn buffer(&self) -> &Bytes {
        &self.buffer
    }

    #[inline]
    pub fn set_buffer(&mut self, buffer: impl Into<Bytes>) {
        let buffer = buffer.into();
        self.size = buffer.len();
        self.buffer = buffer;
    }
}

#[cfg_attr(feature = "stable-abi", derive(StableAbi, StableAbiSample))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PacketBatch {
    Bytes(BytesPacketBatch),
    Single(BytesPacket),
}

#[cfg(feature = "dev-context-only-utils")]
impl From<&Packet> for BytesPacket {
    fn from(packet: &Packet) -> Self {
        let buffer = packet.data(..).map(Bytes::copy_from_slice).unwrap_or_default();
        let mut bytes_packet = Self::new(buffer);
        let meta = packet.meta();
        if !meta.addr.is_unspecified() && meta.port != 0 {
            bytes_packet.set_socket_addr(&meta.socket_addr());
        }
        bytes_packet.set_flags(PacketFlags::from_bits_retain(meta.flags.bits()));
        bytes_packet.set_remote_pubkey(meta.remote_pubkey());
        bytes_packet
    }
}

impl Deref for PacketBatch {
    type Target = [BytesPacket];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Bytes(batch) => batch,
            Self::Single(packet) => core::array::from_ref(packet),
        }
    }
}

impl DerefMut for PacketBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Bytes(batch) => batch,
            Self::Single(packet) => core::array::from_mut(packet),
        }
    }
}

impl PacketBatch {
    pub fn par_iter(&self) -> rayon::slice::Iter<'_, BytesPacket> {
        (**self).par_iter()
    }

    pub fn par_iter_mut(&mut self) -> rayon::slice::IterMut<'_, BytesPacket> {
        (**self).par_iter_mut()
    }
}

impl From<BytesPacketBatch> for PacketBatch {
    fn from(batch: BytesPacketBatch) -> Self {
        Self::Bytes(batch)
    }
}

impl From<Vec<BytesPacket>> for PacketBatch {
    fn from(batch: Vec<BytesPacket>) -> Self {
        Self::Bytes(BytesPacketBatch::from(batch))
    }
}

impl<'a> IntoIterator for &'a PacketBatch {
    type Item = &'a BytesPacket;
    type IntoIter = Iter<'a, BytesPacket>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for &'a mut PacketBatch {
    type Item = &'a mut BytesPacket;
    type IntoIter = std::slice::IterMut<'a, BytesPacket>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<'a> IntoParallelIterator for &'a PacketBatch {
    type Iter = rayon::slice::Iter<'a, BytesPacket>;
    type Item = &'a BytesPacket;
    fn into_par_iter(self) -> Self::Iter {
        self.par_iter()
    }
}

impl<'a> IntoParallelIterator for &'a mut PacketBatch {
    type Iter = rayon::slice::IterMut<'a, BytesPacket>;
    type Item = &'a mut BytesPacket;
    fn into_par_iter(self) -> Self::Iter {
        self.par_iter_mut()
    }
}

pub fn to_packet_batches<T: wincode::Serialize<Src = T>>(
    items: &[T],
    chunk_size: usize,
) -> Vec<PacketBatch> {
    items
        .chunks(chunk_size)
        .map(|batch_items| {
            batch_items
                .iter()
                .map(|item| {
                    let buffer = Bytes::from(wincode::serialize(item).expect("serialize request"));
                    BytesPacket::new(buffer)
                })
                .collect::<BytesPacketBatch>()
                .into()
        })
        .collect()
}

#[cfg(test)]
fn to_packet_batches_for_tests<T: wincode::Serialize<Src = T>>(items: &[T]) -> Vec<PacketBatch> {
    to_packet_batches(items, NUM_PACKETS)
}

#[cfg_attr(feature = "stable-abi", derive(StableAbi, StableAbiSample))]
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BytesPacketBatch {
    packets: Vec<BytesPacket>,
}

impl BytesPacketBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let packets = Vec::with_capacity(capacity);
        Self { packets }
    }
}

impl Deref for BytesPacketBatch {
    type Target = Vec<BytesPacket>;

    fn deref(&self) -> &Self::Target {
        &self.packets
    }
}

impl DerefMut for BytesPacketBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.packets
    }
}

impl From<Vec<BytesPacket>> for BytesPacketBatch {
    fn from(packets: Vec<BytesPacket>) -> Self {
        Self { packets }
    }
}

impl FromIterator<BytesPacket> for BytesPacketBatch {
    fn from_iter<T: IntoIterator<Item = BytesPacket>>(iter: T) -> Self {
        let packets = Vec::from_iter(iter);
        Self { packets }
    }
}

impl<'a> IntoIterator for &'a BytesPacketBatch {
    type Item = &'a BytesPacket;
    type IntoIter = Iter<'a, BytesPacket>;

    fn into_iter(self) -> Self::IntoIter {
        self.packets.iter()
    }
}

impl<'a> IntoParallelIterator for &'a BytesPacketBatch {
    type Iter = rayon::slice::Iter<'a, BytesPacket>;
    type Item = &'a BytesPacket;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter()
    }
}

impl<'a> IntoParallelIterator for &'a mut BytesPacketBatch {
    type Iter = rayon::slice::IterMut<'a, BytesPacket>;
    type Item = &'a mut BytesPacket;
    fn into_par_iter(self) -> Self::Iter {
        self.packets.par_iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_hash::Hash, solana_keypair::Keypair, solana_signer::Signer,
        solana_system_transaction::transfer,
    };

    #[test]
    fn test_bytes_packet_constructors() {
        let buffer = Bytes::from_static(b"packet");
        let packet = BytesPacket::new(buffer.clone());
        assert_eq!(packet.buffer(), &buffer);
        assert_eq!(packet.size(), buffer.len());
        assert_eq!(packet.data(..), Some(buffer.as_ref()));
        assert_eq!(packet.addr(), None);
        assert_eq!(packet.port(), None);
        assert_eq!(packet.socket_addr(), None);
        assert_eq!(packet.flags(), PacketFlags::empty());
        assert_eq!(packet.remote_pubkey(), None);

        let socket_addr = "127.0.0.1:1234".parse::<SocketAddr>().unwrap();
        let packet = BytesPacket::new_with_socket_addr(buffer.clone(), &socket_addr);
        assert_eq!(packet.size(), buffer.len());
        assert_eq!(packet.addr(), Some(socket_addr.ip()));
        assert_eq!(packet.port(), Some(socket_addr.port()));
        assert_eq!(packet.socket_addr(), Some(socket_addr));
    }

    #[test]
    fn test_bytes_packet_metadata() {
        let socket_addr = "[::1]:4321".parse::<SocketAddr>().unwrap();
        let remote_pubkey = Pubkey::new_unique();
        let mut packet = BytesPacket::new(Bytes::from_static(b"packet"));
        packet.set_socket_addr(&socket_addr);
        packet.insert_flags(PacketFlags::REPAIR | PacketFlags::FROM_STAKED_NODE);
        packet.set_remote_pubkey(Some(remote_pubkey));

        assert_eq!(packet.socket_addr(), Some(socket_addr));
        assert!(packet.repair());
        assert!(packet.is_from_staked_node());
        assert_eq!(packet.remote_pubkey(), Some(remote_pubkey));

        packet.set_buffer(Bytes::from_static(b"new packet"));
        assert_eq!(packet.size(), 10);
        assert_eq!(packet.data(..), Some(&b"new packet"[..]));
    }

    #[test]
    fn test_from_legacy_packet_round_trip() {
        let payload = b"hello round trip";
        let socket_addr = "192.0.2.7:9999".parse::<SocketAddr>().unwrap();
        let remote_pubkey = Pubkey::new_unique();
        let mut legacy = Packet::default();
        legacy.buffer_mut()[..payload.len()].copy_from_slice(payload);
        let meta = legacy.meta_mut();
        meta.size = payload.len();
        meta.set_socket_addr(&socket_addr);
        meta.flags = LegacyPacketFlags::FORWARDED | LegacyPacketFlags::REPAIR;
        meta.set_remote_pubkey(remote_pubkey);

        let bp = BytesPacket::from(&legacy);
        assert_eq!(bp.data(..), Some(&payload[..]));
        assert_eq!(bp.size(), payload.len());
        assert_eq!(bp.socket_addr(), Some(socket_addr));
        assert_eq!(bp.addr(), Some(socket_addr.ip()));
        assert_eq!(bp.port(), Some(socket_addr.port()));
        assert_eq!(bp.remote_pubkey(), Some(remote_pubkey));
        assert!(bp.forwarded());
        assert!(bp.repair());
        assert!(!bp.discard());

        // a legacy packet with no address (default unspecified/0) maps to None
        let empty = Packet::default();
        let bp_empty = BytesPacket::from(&empty);
        assert_eq!(bp_empty.addr(), None);
        assert_eq!(bp_empty.port(), None);
        assert_eq!(bp_empty.socket_addr(), None);
        assert_eq!(bp_empty.size(), 0);
        assert!(!bp_empty.discard());

        // a discarded legacy packet: the DISCARD flag is preserved, but the
        // payload is not carried (legacy data(..) is None for discarded
        // packets), so size is 0. This mirrors the base conversion, which
        // used the same buffer expression.
        let mut discarded = Packet::default();
        discarded.meta_mut().flags = LegacyPacketFlags::DISCARD;
        let bp_disc = BytesPacket::from(&discarded);
        assert!(bp_disc.discard());
        assert_eq!(bp_disc.data(..), None);
        assert_eq!(bp_disc.size(), 0);
    }

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
}
