//! Transport-independent mesh frame encoding.
//!
//! The decoder is incremental: callers append bounded chunks to a
//! [`BytesMut`] and repeatedly call [`FrameDecoder::decode`]. Payload lengths
//! are rejected before the decoder waits for or slices the payload.

use bytes::{BufMut, Bytes, BytesMut};
use std::error::Error;
use std::fmt;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/meshbus.control.v1.rs"));
}

pub const FRAME_VERSION: u8 = 1;
pub const HEADER_LEN: usize = 8;
pub const MAX_PAYLOAD_LEN: usize = 1024 * 1024;

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
}
