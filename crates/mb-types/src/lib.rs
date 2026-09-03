//! Shared, dependency-free types used by the mesh-bus core.

use std::fmt;

/// Stable identity of a mesh node.
///
/// Production nodes derive this value from the SHA-256 digest of their public
/// key. Keeping the type opaque prevents routing code from depending on how an
/// identity is produced.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId([u8; 32]);

impl NodeId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Identifier of one local link incarnation.
///
/// A reconnect creates a new `LinkId`, even when the peer is unchanged.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LinkId(u64);

impl LinkId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Monotonic time measured from a runtime-defined epoch.
///
/// The core never reads wall clock time. A production runtime and a simulator
/// can therefore provide time from different sources without changing core
/// logic.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct MonoTime(u64);

impl MonoTime {
    pub const ZERO: Self = Self(0);

    pub const fn from_millis(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }
}

/// Common shape of an I/O-free core component.
pub trait Component {
    type Event;
    type Action;

    fn handle(&mut self, now: MonoTime, event: Self::Event) -> Vec<Self::Action>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_display_is_fixed_width_hex() {
        let mut bytes = [0_u8; 32];
        bytes[0] = 0x0a;
        bytes[31] = 0xff;

        let rendered = NodeId::from_bytes(bytes).to_string();

        assert_eq!(rendered.len(), 64);
        assert!(rendered.starts_with("0a00"));
        assert!(rendered.ends_with("00ff"));
    }
}
