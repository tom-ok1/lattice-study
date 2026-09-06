//! Transport-independent mesh frame encoding.
//!
//! The decoder is incremental: callers append bounded chunks to a
//! [`BytesMut`] and repeatedly call [`FrameDecoder::decode`]. Payload lengths
//! are rejected before the decoder waits for or slices the payload.

use bytes::{BufMut, Bytes, BytesMut};
use mb_types::NodeId;
use std::error::Error;
use std::fmt;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/meshbus.control.v1.rs"));
}

pub const FRAME_VERSION: u8 = 1;
pub const HEADER_LEN: usize = 8;
pub const MAX_PAYLOAD_LEN: usize = 1024 * 1024;
pub const FORWARD_PACKET_VERSION: u8 = 1;
pub const FORWARD_HEADER_LEN: usize = 88;
pub const MAX_FORWARD_PAYLOAD_LEN: usize = MAX_PAYLOAD_LEN - FORWARD_HEADER_LEN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PacketType {
    Unicast = 1,
    Multicast = 2,
    CircuitData = 3,
    CircuitControl = 4,
}

impl TryFrom<u8> for PacketType {
    type Error = ForwardPacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Unicast),
            2 => Ok(Self::Multicast),
            3 => Ok(Self::CircuitData),
            4 => Ok(Self::CircuitControl),
            other => Err(ForwardPacketError::UnknownPacketType(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Priority {
    P0 = 0,
    P1 = 1,
    P2 = 2,
    P3 = 3,
}

impl TryFrom<u8> for Priority {
    type Error = ForwardPacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::P0),
            1 => Ok(Self::P1),
            2 => Ok(Self::P2),
            3 => Ok(Self::P3),
            other => Err(ForwardPacketError::UnknownPriority(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ForwardFlags(u32);

impl ForwardFlags {
    pub const RELIABLE_HINT: u32 = 1 << 0;
    pub const CONFLATABLE: u32 = 1 << 1;
    pub const COMPRESSED: u32 = 1 << 2;
    const SUPPORTED: u32 = Self::RELIABLE_HINT | Self::CONFLATABLE | Self::COMPRESSED;

    pub const fn empty() -> Self {
        Self(0)
    }

    pub fn from_bits(bits: u32) -> Result<Self, ForwardPacketError> {
        if bits & !Self::SUPPORTED == 0 {
            Ok(Self(bits))
        } else {
            Err(ForwardPacketError::UnsupportedFlags(bits))
        }
    }

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn contains(self, flag: u32) -> bool {
        self.0 & flag == flag
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForwardHeader {
    pub packet_type: PacketType,
    pub priority: Priority,
    pub ttl: u8,
    pub flags: ForwardFlags,
    pub destination: NodeId,
    pub source: NodeId,
    pub flow_id: u64,
    pub conflate_key: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForwardPacket {
    pub header: ForwardHeader,
    pub payload: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForwardPacketError {
    HeaderTooShort(usize),
    UnsupportedVersion(u8),
    UnknownPacketType(u8),
    UnknownPriority(u8),
    UnsupportedFlags(u32),
    PayloadTooLarge(usize),
}

impl fmt::Display for ForwardPacketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeaderTooShort(length) => write!(
                f,
                "forward packet is {length} bytes, shorter than the {FORWARD_HEADER_LEN} byte header"
            ),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported forward packet version {version}")
            }
            Self::UnknownPacketType(packet_type) => {
                write!(f, "unknown forward packet type {packet_type}")
            }
            Self::UnknownPriority(priority) => {
                write!(f, "unknown forward packet priority {priority}")
            }
            Self::UnsupportedFlags(flags) => {
                write!(f, "unsupported forward packet flags {flags:#010x}")
            }
            Self::PayloadTooLarge(length) => write!(
                f,
                "forward packet payload length {length} exceeds the {MAX_FORWARD_PAYLOAD_LEN} byte limit"
            ),
        }
    }
}

impl Error for ForwardPacketError {}

pub struct ForwardPacketCodec;

impl ForwardPacketCodec {
    pub fn encode(packet: &ForwardPacket) -> Result<Bytes, ForwardPacketError> {
        if packet.payload.len() > MAX_FORWARD_PAYLOAD_LEN {
            return Err(ForwardPacketError::PayloadTooLarge(packet.payload.len()));
        }

        let mut encoded = BytesMut::with_capacity(FORWARD_HEADER_LEN + packet.payload.len());
        encoded.put_u8(FORWARD_PACKET_VERSION);
        encoded.put_u8(packet.header.packet_type as u8);
        encoded.put_u8(packet.header.priority as u8);
        encoded.put_u8(packet.header.ttl);
        encoded.put_u32(packet.header.flags.bits());
        encoded.extend_from_slice(packet.header.destination.as_bytes());
        encoded.extend_from_slice(packet.header.source.as_bytes());
        encoded.put_u64(packet.header.flow_id);
        encoded.put_u64(packet.header.conflate_key);
        encoded.extend_from_slice(&packet.payload);
        Ok(encoded.freeze())
    }

    pub fn decode(encoded: Bytes) -> Result<ForwardPacket, ForwardPacketError> {
        if encoded.len() < FORWARD_HEADER_LEN {
            return Err(ForwardPacketError::HeaderTooShort(encoded.len()));
        }
        if encoded.len() - FORWARD_HEADER_LEN > MAX_FORWARD_PAYLOAD_LEN {
            return Err(ForwardPacketError::PayloadTooLarge(
                encoded.len() - FORWARD_HEADER_LEN,
            ));
        }
        if encoded[0] != FORWARD_PACKET_VERSION {
            return Err(ForwardPacketError::UnsupportedVersion(encoded[0]));
        }

        let packet_type = PacketType::try_from(encoded[1])?;
        let priority = Priority::try_from(encoded[2])?;
        let flags = ForwardFlags::from_bits(u32::from_be_bytes([
            encoded[4], encoded[5], encoded[6], encoded[7],
        ]))?;
        let mut destination = [0_u8; 32];
        destination.copy_from_slice(&encoded[8..40]);
        let mut source = [0_u8; 32];
        source.copy_from_slice(&encoded[40..72]);
        let flow_id = u64::from_be_bytes(
            encoded[72..80]
                .try_into()
                .expect("flow_id slice has a fixed length"),
        );
        let conflate_key = u64::from_be_bytes(
            encoded[80..88]
                .try_into()
                .expect("conflate_key slice has a fixed length"),
        );

        Ok(ForwardPacket {
            header: ForwardHeader {
                packet_type,
                priority,
                ttl: encoded[3],
                flags,
                destination: NodeId::from_bytes(destination),
                source: NodeId::from_bytes(source),
                flow_id,
                conflate_key,
            },
            payload: encoded.slice(FORWARD_HEADER_LEN..),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum FrameType {
    LinkHello = 1,
    Lsa = 2,
    Digest = 3,
    DigestReq = 4,
    Forward = 5,
    Circuit = 6,
    Credit = 7,
    Ping = 8,
}

impl TryFrom<u8> for FrameType {
    type Error = FrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::LinkHello),
            2 => Ok(Self::Lsa),
            3 => Ok(Self::Digest),
            4 => Ok(Self::DigestReq),
            5 => Ok(Self::Forward),
            6 => Ok(Self::Circuit),
            7 => Ok(Self::Credit),
            8 => Ok(Self::Ping),
            other => Err(FrameError::UnknownFrameType(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Channel {
    Control = 0,
    Rpc = 1,
    PubSubP0 = 2,
    PubSubP1 = 3,
    PubSubP2 = 4,
    PubSubP3 = 5,
    Tunnel = 6,
}

impl TryFrom<u8> for Channel {
    type Error = FrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Control),
            1 => Ok(Self::Rpc),
            2 => Ok(Self::PubSubP0),
            3 => Ok(Self::PubSubP1),
            4 => Ok(Self::PubSubP2),
            5 => Ok(Self::PubSubP3),
            6 => Ok(Self::Tunnel),
            other => Err(FrameError::UnknownChannel(other)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireFrame {
    pub frame_type: FrameType,
    pub channel: Channel,
    pub payload: Bytes,
}

impl WireFrame {
    pub fn control(frame_type: FrameType, payload: impl Into<Bytes>) -> Self {
        Self {
            frame_type,
            channel: Channel::Control,
            payload: payload.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameError {
    UnsupportedVersion(u8),
    UnknownFrameType(u8),
    UnknownChannel(u8),
    UnsupportedFlags(u8),
    NonZeroReserved(u8),
    PayloadTooLarge(usize),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported frame version {version}")
            }
            Self::UnknownFrameType(frame_type) => write!(f, "unknown frame type {frame_type}"),
            Self::UnknownChannel(channel) => write!(f, "unknown channel {channel}"),
            Self::UnsupportedFlags(flags) => write!(f, "unsupported frame flags {flags:#04x}"),
            Self::NonZeroReserved(reserved) => {
                write!(f, "reserved frame byte must be zero, got {reserved:#04x}")
            }
            Self::PayloadTooLarge(length) => write!(
                f,
                "frame payload length {length} exceeds the {MAX_PAYLOAD_LEN} byte limit"
            ),
        }
    }
}

impl Error for FrameError {}

pub struct FrameEncoder;

impl FrameEncoder {
    pub fn encode(frame: &WireFrame) -> Result<Bytes, FrameError> {
        let payload_len = frame.payload.len();
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }

        let mut encoded = BytesMut::with_capacity(HEADER_LEN + payload_len);
        encoded.put_u8((FRAME_VERSION << 4) | frame.frame_type as u8);
        encoded.put_u8(frame.channel as u8);
        encoded.put_u8(0); // flags
        encoded.put_u8(0); // reserved
        encoded.put_u32(payload_len as u32);
        encoded.extend_from_slice(&frame.payload);
        Ok(encoded.freeze())
    }
}

pub struct FrameDecoder;

impl FrameDecoder {
    pub fn decode(&mut self, source: &mut BytesMut) -> Result<Option<WireFrame>, FrameError> {
        if source.len() < HEADER_LEN {
            return Ok(None);
        }

        let version = source[0] >> 4;
        if version != FRAME_VERSION {
            return Err(FrameError::UnsupportedVersion(version));
        }
        let frame_type = FrameType::try_from(source[0] & 0x0f)?;
        let channel = Channel::try_from(source[1])?;
        if source[2] != 0 {
            return Err(FrameError::UnsupportedFlags(source[2]));
        }
        if source[3] != 0 {
            return Err(FrameError::NonZeroReserved(source[3]));
        }

        let payload_len = u32::from_be_bytes([source[4], source[5], source[6], source[7]]) as usize;
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(FrameError::PayloadTooLarge(payload_len));
        }
        let frame_len = HEADER_LEN + payload_len;
        if source.len() < frame_len {
            return Ok(None);
        }

        let mut encoded = source.split_to(frame_len);
        let payload = encoded.split_off(HEADER_LEN).freeze();
        Ok(Some(WireFrame {
            frame_type,
            channel,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: u8) -> NodeId {
        let mut bytes = [0_u8; 32];
        bytes[31] = value;
        NodeId::from_bytes(bytes)
    }

    fn forward_packet() -> ForwardPacket {
        ForwardPacket {
            header: ForwardHeader {
                packet_type: PacketType::Unicast,
                priority: Priority::P1,
                ttl: 32,
                flags: ForwardFlags::from_bits(
                    ForwardFlags::RELIABLE_HINT | ForwardFlags::CONFLATABLE,
                )
                .expect("test flags must be supported"),
                destination: node(2),
                source: node(1),
                flow_id: 0x0102_0304_0506_0708,
                conflate_key: 0x1112_1314_1516_1718,
            },
            payload: Bytes::from_static(b"opaque payload"),
        }
    }

    #[test]
    fn frame_round_trips() {
        let original = WireFrame::control(FrameType::Lsa, Bytes::from_static(b"an lsa"));
        let encoded = FrameEncoder::encode(&original).expect("frame must encode");
        assert_eq!(encoded.len(), HEADER_LEN + original.payload.len());

        let mut source = BytesMut::from(encoded.as_ref());
        let decoded = FrameDecoder
            .decode(&mut source)
            .expect("frame must decode")
            .expect("the complete frame must be returned");

        assert_eq!(decoded, original);
        assert!(source.is_empty());
    }

    #[test]
    fn frame_decodes_one_byte_at_a_time() {
        let original = WireFrame::control(FrameType::Lsa, Bytes::from_static(b"fragmented"));
        let encoded = FrameEncoder::encode(&original).expect("frame must encode");
        let mut source = BytesMut::new();
        let mut decoder = FrameDecoder;
        let mut decoded = None;

        for byte in encoded {
            source.extend_from_slice(&[byte]);
            let next = decoder.decode(&mut source).expect("prefix must be valid");
            if next.is_some() {
                assert!(decoded.is_none());
                decoded = next;
            }
        }

        assert_eq!(decoded, Some(original));
    }

    #[test]
    fn decoder_returns_multiple_buffered_frames_individually() {
        let first = WireFrame::control(FrameType::Ping, Bytes::from_static(b"one"));
        let second = WireFrame::control(FrameType::Lsa, Bytes::from_static(b"two"));
        let mut source = BytesMut::new();
        source.extend_from_slice(&FrameEncoder::encode(&first).expect("first frame must encode"));
        source.extend_from_slice(&FrameEncoder::encode(&second).expect("second frame must encode"));
        let mut decoder = FrameDecoder;

        assert_eq!(decoder.decode(&mut source), Ok(Some(first)));
        assert_eq!(decoder.decode(&mut source), Ok(Some(second)));
        assert_eq!(decoder.decode(&mut source), Ok(None));
    }

    #[test]
    fn oversized_length_is_rejected_from_the_header_alone() {
        let mut source = BytesMut::new();
        source.put_u8((FRAME_VERSION << 4) | FrameType::Lsa as u8);
        source.put_u8(Channel::Control as u8);
        source.put_u8(0);
        source.put_u8(0);
        source.put_u32((MAX_PAYLOAD_LEN + 1) as u32);

        assert_eq!(
            FrameDecoder.decode(&mut source),
            Err(FrameError::PayloadTooLarge(MAX_PAYLOAD_LEN + 1))
        );
    }

    #[test]
    fn invalid_header_fields_are_rejected_without_panicking() {
        let cases = [
            (
                [0x22, 0, 0, 0, 0, 0, 0, 0],
                FrameError::UnsupportedVersion(2),
            ),
            (
                [0x1f, 0, 0, 0, 0, 0, 0, 0],
                FrameError::UnknownFrameType(15),
            ),
            ([0x12, 9, 0, 0, 0, 0, 0, 0], FrameError::UnknownChannel(9)),
            ([0x12, 0, 1, 0, 0, 0, 0, 0], FrameError::UnsupportedFlags(1)),
            ([0x12, 0, 0, 1, 0, 0, 0, 0], FrameError::NonZeroReserved(1)),
        ];

        for (bytes, expected) in cases {
            let mut source = BytesMut::from(bytes.as_slice());
            assert_eq!(FrameDecoder.decode(&mut source), Err(expected));
        }
    }

    #[test]
    fn forward_packet_round_trips_with_a_fixed_width_header() {
        let original = forward_packet();

        let encoded = ForwardPacketCodec::encode(&original).expect("packet must encode");
        assert_eq!(encoded.len(), FORWARD_HEADER_LEN + original.payload.len());
        assert_eq!(&encoded[FORWARD_HEADER_LEN..], original.payload.as_ref());

        let decoded = ForwardPacketCodec::decode(encoded).expect("packet must decode");
        assert_eq!(decoded, original);
    }

    #[test]
    fn invalid_forward_packet_headers_are_rejected() {
        assert_eq!(
            ForwardPacketCodec::decode(Bytes::from_static(&[0; FORWARD_HEADER_LEN - 1])),
            Err(ForwardPacketError::HeaderTooShort(FORWARD_HEADER_LEN - 1))
        );

        let encoded = ForwardPacketCodec::encode(&forward_packet()).expect("packet must encode");
        let cases = [
            (0, 2, ForwardPacketError::UnsupportedVersion(2)),
            (1, 9, ForwardPacketError::UnknownPacketType(9)),
            (2, 4, ForwardPacketError::UnknownPriority(4)),
        ];
        for (offset, value, expected) in cases {
            let mut malformed = BytesMut::from(encoded.as_ref());
            malformed[offset] = value;
            assert_eq!(
                ForwardPacketCodec::decode(malformed.freeze()),
                Err(expected)
            );
        }

        let mut unsupported_flags = BytesMut::from(encoded.as_ref());
        unsupported_flags[7] = 1 << 3;
        assert_eq!(
            ForwardPacketCodec::decode(unsupported_flags.freeze()),
            Err(ForwardPacketError::UnsupportedFlags(1 << 3))
        );
    }

    #[test]
    fn oversized_forward_payload_is_rejected_on_encode_and_decode() {
        let oversized_len = MAX_FORWARD_PAYLOAD_LEN + 1;
        let mut packet = forward_packet();
        packet.payload = Bytes::from(vec![0_u8; oversized_len]);
        assert_eq!(
            ForwardPacketCodec::encode(&packet),
            Err(ForwardPacketError::PayloadTooLarge(oversized_len))
        );

        let encoded = Bytes::from(vec![0_u8; MAX_PAYLOAD_LEN + 1]);
        assert_eq!(
            ForwardPacketCodec::decode(encoded),
            Err(ForwardPacketError::PayloadTooLarge(oversized_len))
        );
    }
}
