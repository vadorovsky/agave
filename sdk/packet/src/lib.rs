//! The definition of a Solana network packet.
#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

#[cfg(feature = "frozen-abi")]
use solana_frozen_abi_macro::AbiExample;
#[cfg(feature = "bincode")]
use {
    bincode::{Options, Result},
    std::{
        cmp,
        io::{self, Write},
    },
};
use {
    bitflags::bitflags,
    bytes::{BufMut, Bytes, BytesMut},
    smallvec::SmallVec,
    std::{
        fmt,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        ops::{Deref, DerefMut},
        slice::SliceIndex,
    },
};
#[cfg(feature = "serde")]
use {
    serde_derive::{Deserialize, Serialize},
    serde_with::serde_as,
};

#[cfg(test)]
static_assertions::const_assert_eq!(PACKET_DATA_SIZE, 1232);
/// Maximum over-the-wire size of a Transaction
///   1280 is IPv6 minimum MTU
///   40 bytes is the size of the IPv6 header
///   8 bytes is the size of the fragment header
pub const PACKET_DATA_SIZE: usize = 1280 - 40 - 8;

#[cfg(feature = "bincode")]
pub trait Encode {
    fn encode<W: Write>(&self, writer: W) -> Result<()>;
}

#[cfg(feature = "bincode")]
impl<T: ?Sized + serde::Serialize> Encode for T {
    fn encode<W: Write>(&self, writer: W) -> Result<()> {
        bincode::serialize_into::<W, T>(writer, self)
    }
}

bitflags! {
    #[repr(C)]
    #[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub struct PacketFlags: u8 {
        const DISCARD        = 0b0000_0001;
        const FORWARDED      = 0b0000_0010;
        const REPAIR         = 0b0000_0100;
        const SIMPLE_VOTE_TX = 0b0000_1000;
        // Previously used - this can now be re-used for something else.
        const UNUSED_0  = 0b0001_0000;
        // Previously used - this can now be re-used for something else.
        const UNUSED_1 = 0b0010_0000;
        /// For tracking performance
        const PERF_TRACK_PACKET  = 0b0100_0000;
        /// For marking packets from staked nodes
        const FROM_STAKED_NODE = 0b1000_0000;
    }
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Meta {
    pub size: usize,
    pub addr: IpAddr,
    pub port: u16,
    pub flags: PacketFlags,
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

/// Wrapper over [`BytesMut`] which provides a [`Write`] trait implementation.
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Clone, Eq, PartialEq)]
pub struct WritableBytesMut(BytesMut);

impl WritableBytesMut {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for WritableBytesMut {
    fn default() -> Self {
        Self(BytesMut::default())
    }
}

impl Deref for WritableBytesMut {
    type Target = BytesMut;

    #[inline]
    fn deref(&self) -> &BytesMut {
        &self.0
    }
}

impl DerefMut for WritableBytesMut {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl io::Write for WritableBytesMut {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        let n = cmp::min(self.0.remaining_mut(), src.len());

        self.0.put_slice(&src[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Clone, Eq)]
pub enum PacketData {
    /// A mutable byte buffer.
    ///
    /// Intended to be used where we are responsible for receiving messages
    /// from sockets (UDP).
    Buffer(WritableBytesMut),
    /// Immutable chunks.
    ///
    /// Intended to be used with QUIC, where we just want to consume chunks
    /// from quinn.
    Chunks(SmallVec<[Bytes; 2]>),
}

impl PartialEq for PacketData {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Buffer(buffer) => match other {
                Self::Buffer(other_buffer) => buffer.eq(other_buffer),
                _ => false,
            },
            Self::Chunks(chunks) => match other {
                Self::Chunks(other_chunks) => chunks.eq(other_chunks),
                _ => false,
            },
        }
    }
}

// serde_as is used as a work around because array isn't supported by serde
// (and serde_bytes).
//
// the root cause is of a historical special handling for [T; 0] in rust's
// `Default` and supposedly mirrored serde's `Serialize` (macro) impls,
// pre-dating stabilized const generics, meaning it'll take long time...:
//   https://github.com/rust-lang/rust/issues/61415
//   https://github.com/rust-lang/rust/issues/88744#issuecomment-1138678928
//
// Due to the nature of the root cause, the current situation is complicated.
// All in all, the serde_as solution is chosen for good perf and low maintenance
// need at the cost of another crate dependency..
//
// For details, please refer to the below various links...
//
// relevant merged/published pr for this serde_as functionality used here:
//   https://github.com/jonasbb/serde_with/pull/277
// open pr at serde_bytes:
//   https://github.com/serde-rs/bytes/pull/28
// open issue at serde:
//   https://github.com/serde-rs/serde/issues/1937
// closed pr at serde (due to the above mentioned [N; 0] issue):
//   https://github.com/serde-rs/serde/pull/1860
// ryoqun's dirty experiments:
//   https://github.com/ryoqun/serde-array-comparisons
//
// We use the cfg_eval crate as advised by the serde_with guide:
// https://docs.rs/serde_with/latest/serde_with/guide/serde_as/index.html#gating-serde_as-on-features
#[cfg_attr(feature = "serde", cfg_eval::cfg_eval, serde_as)]
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Clone, Eq)]
#[repr(C)]
pub struct Packet {
    meta: Meta,
    data: PacketData,
}

impl Packet {
    pub fn new(meta: Meta) -> Self {
        let data = PacketData::Buffer(WritableBytesMut::new());
        Self { data, meta }
    }

    /// Returns an immutable reference to the underlying buffer up to
    /// packet.meta.size. The rest of the buffer is not valid to read from.
    /// packet.data(..) returns packet.buffer.get(..packet.meta.size).
    /// Returns None if the index is invalid or if the packet is already marked
    /// as discard.
    #[inline]
    pub fn data<I>(&self, index: I) -> Option<&<I as SliceIndex<[u8]>>::Output>
    where
        I: SliceIndex<[u8]>,
    {
        // If the packet is marked as discard, it is either invalid or
        // otherwise should be ignored, and so the payload should not be read
        // from.
        if self.meta.discard() {
            None
        } else {
            match self.data {
                PacketData::Buffer(ref buffer) => buffer.get(index),
                PacketData::Chunks(ref chunks) => chunks.first()?.get(index),
            }
        }
    }

    /// Returns a mutable reference to the entirety of the underlying buffer to
    /// write into. The caller is responsible for updating Packet.meta.size
    /// after writing to the buffer.
    #[inline]
    pub fn buffer_mut(&mut self) -> Option<&mut [u8]> {
        debug_assert!(!self.meta.discard());

        match self.data {
            PacketData::Buffer(ref mut buffer) => Some(&mut buffer[..]),
            PacketData::Chunks(_) => None,
        }
    }

    #[inline]
    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    #[inline]
    pub fn meta_mut(&mut self) -> &mut Meta {
        &mut self.meta
    }

    pub fn from_chunks(dest: Option<&SocketAddr>, chunks: &[Bytes]) -> Self {
        let mut meta = Meta::default();
        meta.size = chunks.iter().map(|chunk| chunk.len()).sum();
        if let Some(dest) = dest {
            meta.set_socket_addr(dest);
        }
        let chunks = chunks
            .iter()
            .map(|chunk| chunk.clone())
            .collect::<SmallVec<[Bytes; 2]>>();
        let data = PacketData::Chunks(chunks);
        Self { data, meta }
    }

    #[cfg(feature = "bincode")]
    pub fn from_data<T: serde::Serialize>(dest: Option<&SocketAddr>, data: T) -> Result<Self> {
        let mut buffer = WritableBytesMut::new();
        bincode::serialize_into(&mut buffer, &data)?;
        let mut meta = Meta::default();
        meta.size = buffer.len();
        if let Some(dest) = dest {
            meta.set_socket_addr(dest);
        }
        let data = PacketData::Buffer(buffer);
        Ok(Self { data, meta })
    }

    #[cfg(feature = "bincode")]
    pub fn populate_packet<T: serde::Serialize>(
        &mut self,
        dest: Option<&SocketAddr>,
        data: &T,
    ) -> Result<()> {
        debug_assert!(!self.meta.discard());
        match self.data {
            PacketData::Buffer(ref mut buffer) => {
                bincode::serialize_into(&mut *buffer, data)?;
                self.meta.size = buffer.len();
                if let Some(dest) = dest {
                    self.meta.set_socket_addr(dest);
                }
            }
            PacketData::Chunks(_) => {}
        }

        Ok(())
    }

    #[cfg(feature = "bincode")]
    pub fn deserialize_slice<T, I>(&self, index: I) -> Result<T>
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

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Packet {{ size: {:?}, addr: {:?} }}",
            self.meta.size,
            self.meta.socket_addr()
        )
    }
}

impl Default for Packet {
    fn default() -> Self {
        Self {
            meta: Meta::default(),
            data: PacketData::Buffer(WritableBytesMut::new()),
        }
    }
}

impl PartialEq for Packet {
    fn eq(&self, other: &Self) -> bool {
        self.meta() == other.meta() && self.data(..) == other.data(..)
    }
}

impl Meta {
    pub fn new(size: usize, socket_addr: Option<&SocketAddr>) -> Self {
        let (addr, port) = match socket_addr {
            Some(socket_addr) => (socket_addr.ip(), socket_addr.port()),
            None => (IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        };
        Self {
            size,
            addr,
            port,
            flags: PacketFlags::empty(),
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.addr, self.port)
    }

    pub fn set_socket_addr(&mut self, socket_addr: &SocketAddr) {
        self.addr = socket_addr.ip();
        self.port = socket_addr.port();
    }

    pub fn set_from_staked_node(&mut self, from_staked_node: bool) {
        self.flags
            .set(PacketFlags::FROM_STAKED_NODE, from_staked_node);
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
    pub fn set_track_performance(&mut self, is_performance_track: bool) {
        self.flags
            .set(PacketFlags::PERF_TRACK_PACKET, is_performance_track);
    }

    #[inline]
    pub fn set_simple_vote(&mut self, is_simple_vote: bool) {
        self.flags.set(PacketFlags::SIMPLE_VOTE_TX, is_simple_vote);
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
    pub fn is_perf_track_packet(&self) -> bool {
        self.flags.contains(PacketFlags::PERF_TRACK_PACKET)
    }

    #[inline]
    pub fn is_from_staked_node(&self) -> bool {
        self.flags.contains(PacketFlags::FROM_STAKED_NODE)
    }
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            size: 0,
            addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 0,
            flags: PacketFlags::empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_slice() {
        let p = Packet::from_data(None, u32::MAX).unwrap();
        assert_eq!(p.deserialize_slice(..).ok(), Some(u32::MAX));
        assert_eq!(p.deserialize_slice(0..4).ok(), Some(u32::MAX));
        assert_eq!(
            p.deserialize_slice::<u16, _>(0..4)
                .map_err(|e| e.to_string()),
            Err("Slice had bytes remaining after deserialization".to_string()),
        );
        assert_eq!(
            p.deserialize_slice::<u32, _>(0..0)
                .map_err(|e| e.to_string()),
            Err("io error: unexpected end of file".to_string()),
        );
        assert_eq!(
            p.deserialize_slice::<u32, _>(0..1)
                .map_err(|e| e.to_string()),
            Err("io error: unexpected end of file".to_string()),
        );
        assert_eq!(
            p.deserialize_slice::<u32, _>(0..5)
                .map_err(|e| e.to_string()),
            Err("the size limit has been reached".to_string()),
        );
        #[allow(clippy::reversed_empty_ranges)]
        let reversed_empty_range = 4..0;
        assert_eq!(
            p.deserialize_slice::<u32, _>(reversed_empty_range)
                .map_err(|e| e.to_string()),
            Err("the size limit has been reached".to_string()),
        );
        assert_eq!(
            p.deserialize_slice::<u32, _>(4..5)
                .map_err(|e| e.to_string()),
            Err("the size limit has been reached".to_string()),
        );
    }
}
