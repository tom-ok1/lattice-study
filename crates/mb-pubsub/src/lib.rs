//! I/O-free live Pub/Sub core.
//!
//! Subscription discovery is supplied as an event and network sends are
//! returned as actions. Storage, backfill, encryption, and application I/O are
//! intentionally left to later slices.

use bytes::{Bytes, BytesMut};
use mb_types::{Component, MonoTime, NodeId};
use mb_wire::{
    Priority, MAX_FORWARD_PAYLOAD_LEN, MAX_MULTICAST_DESTINATIONS, MULTICAST_COUNT_LEN, NODE_ID_LEN,
};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::sync::Arc;

const ENVELOPE_MAGIC: &[u8; 4] = b"MBPS";
const MAX_TOPIC_NAME_LEN: usize = 1_024;
const MAX_PARTITION_LEN: usize = 64 * 1_024;
const MAX_LIVE_ENVELOPE_LEN: usize = MAX_FORWARD_PAYLOAD_LEN - MULTICAST_COUNT_LEN - NODE_ID_LEN;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicKey {
    name: String,
    partition: Bytes,
}

impl TopicKey {
    pub fn new(name: impl Into<String>, partition: impl Into<Bytes>) -> Result<Self, TopicError> {
        let name = name.into();
        let partition = partition.into();
        validate_topic(&name, &partition)?;
        Ok(Self { name, partition })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn partition(&self) -> &Bytes {
        &self.partition
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicError {
    EmptyName,
    NameTooLong(usize),
    PartitionTooLong(usize),
}

impl fmt::Display for TopicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName => f.write_str("topic name must not be empty"),
            Self::NameTooLong(length) => write!(
                f,
                "topic name length {length} exceeds the {MAX_TOPIC_NAME_LEN} byte limit"
            ),
            Self::PartitionTooLong(length) => write!(
                f,
                "topic partition length {length} exceeds the {MAX_PARTITION_LEN} byte limit"
            ),
        }
    }
}

impl Error for TopicError {}

fn validate_topic(name: &str, partition: &Bytes) -> Result<(), TopicError> {
    if name.is_empty() {
        return Err(TopicError::EmptyName);
    }
    if name.len() > MAX_TOPIC_NAME_LEN {
        return Err(TopicError::NameTooLong(name.len()));
    }
    if partition.len() > MAX_PARTITION_LEN {
        return Err(TopicError::PartitionTooLong(partition.len()));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicMode {
    Log,
    Latest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopicPolicy {
    pub priority: Priority,
    pub mode: TopicMode,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TopicPolicies {
    policies: BTreeMap<String, TopicPolicy>,
}

impl TopicPolicies {
    pub fn new(
        policies: impl IntoIterator<Item = (String, TopicPolicy)>,
    ) -> Result<Self, TopicError> {
        let mut collected = BTreeMap::new();
        for (name, policy) in policies {
            validate_topic(&name, &Bytes::new())?;
            collected.insert(name, policy);
        }
        Ok(Self {
            policies: collected,
        })
    }

    pub fn get(&self, topic_name: &str) -> Option<TopicPolicy> {
        self.policies.get(topic_name).copied()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SubId(u64);

impl SubId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicSelector {
    name: String,
    partition: Option<Bytes>,
}

impl TopicSelector {
    pub fn exact(topic: TopicKey) -> Self {
        Self {
            name: topic.name,
            partition: Some(topic.partition),
        }
    }

    pub fn all_partitions(name: impl Into<String>) -> Result<Self, TopicError> {
        let name = name.into();
        validate_topic(&name, &Bytes::new())?;
        Ok(Self {
            name,
            partition: None,
        })
    }

    pub fn matches(&self, topic: &TopicKey) -> bool {
        self.name == topic.name
            && self
                .partition
                .as_ref()
                .map_or(true, |partition| partition == &topic.partition)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn partition(&self) -> Option<&Bytes> {
        self.partition.as_ref()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DiscoveryIndex {
    subscribers: BTreeMap<TopicSelector, BTreeSet<NodeId>>,
}

impl DiscoveryIndex {
    pub fn set_subscribers(
        &mut self,
        selector: TopicSelector,
        subscribers: impl IntoIterator<Item = NodeId>,
    ) {
        self.subscribers
            .insert(selector, subscribers.into_iter().collect());
    }

    pub fn subscribers(&self, topic: &TopicKey) -> BTreeSet<NodeId> {
        self.subscribers
            .iter()
            .filter(|(selector, _)| selector.matches(topic))
            .flat_map(|(_, subscribers)| subscribers.iter().copied())
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    pub origin: NodeId,
    pub topic: TopicKey,
    pub seq: u64,
    pub ts_ms: u64,
    pub payload: Bytes,
}

#[derive(Clone, PartialEq, Message)]
struct WireEnvelope {
    #[prost(bytes = "bytes", tag = "1")]
    origin: Bytes,
    #[prost(string, tag = "2")]
    topic: String,
    #[prost(bytes = "bytes", tag = "3")]
    partition: Bytes,
    #[prost(uint64, tag = "4")]
    seq: u64,
    #[prost(uint64, tag = "5")]
    ts_ms: u64,
    // This carries plaintext until the security phase adds Topic encryption.
    #[prost(bytes = "bytes", tag = "6")]
    payload: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnvelopeError {
    NotPubSub,
    Protobuf(String),
    InvalidOriginLength(usize),
    InvalidSequence,
    InvalidTopic(TopicError),
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPubSub => f.write_str("payload is not a mesh-bus Pub/Sub envelope"),
            Self::Protobuf(error) => write!(f, "Pub/Sub envelope protobuf is invalid: {error}"),
            Self::InvalidOriginLength(length) => {
                write!(f, "Pub/Sub origin must be 32 bytes, got {length}")
            }
            Self::InvalidSequence => f.write_str("Pub/Sub sequence must be non-zero"),
            Self::InvalidTopic(error) => write!(f, "invalid Pub/Sub topic: {error}"),
        }
    }
}

impl Error for EnvelopeError {}

pub struct EnvelopeCodec;

impl EnvelopeCodec {
    pub fn is_envelope(encoded: &Bytes) -> bool {
        encoded.starts_with(ENVELOPE_MAGIC)
    }

    pub fn encode(envelope: &Envelope) -> Result<Bytes, EnvelopeError> {
        validate_topic(envelope.topic.name(), envelope.topic.partition())
            .map_err(EnvelopeError::InvalidTopic)?;
        if envelope.seq == 0 {
            return Err(EnvelopeError::InvalidSequence);
        }
        let wire = WireEnvelope {
            origin: Bytes::copy_from_slice(envelope.origin.as_bytes()),
            topic: envelope.topic.name.clone(),
            partition: envelope.topic.partition.clone(),
            seq: envelope.seq,
            ts_ms: envelope.ts_ms,
            payload: envelope.payload.clone(),
        };
        let mut encoded = BytesMut::with_capacity(ENVELOPE_MAGIC.len() + wire.encoded_len());
        encoded.extend_from_slice(ENVELOPE_MAGIC);
        wire.encode(&mut encoded)
            .expect("BytesMut has sufficient capacity and cannot fail");
        Ok(encoded.freeze())
    }

    pub fn decode(encoded: Bytes) -> Result<Envelope, EnvelopeError> {
        if !Self::is_envelope(&encoded) {
            return Err(EnvelopeError::NotPubSub);
        }
        let wire = WireEnvelope::decode(encoded.slice(ENVELOPE_MAGIC.len()..))
            .map_err(|error| EnvelopeError::Protobuf(error.to_string()))?;
        let origin_length = wire.origin.len();
        let origin = NodeId::from_bytes(
            wire.origin
                .as_ref()
                .try_into()
                .map_err(|_| EnvelopeError::InvalidOriginLength(origin_length))?,
        );
        if wire.seq == 0 {
            return Err(EnvelopeError::InvalidSequence);
        }
        let topic =
            TopicKey::new(wire.topic, wire.partition).map_err(EnvelopeError::InvalidTopic)?;
        Ok(Envelope {
            origin,
            topic,
            seq: wire.seq,
            ts_ms: wire.ts_ms,
            payload: wire.payload,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveredMessage {
    pub envelope: Envelope,
    pub is_backfill: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PubSubEvent {
    LocalPublish {
        topic: TopicKey,
        payload: Bytes,
    },
    LocalSubscribe {
        sub_id: SubId,
        selector: TopicSelector,
    },
    LocalUnsubscribe(SubId),
    Inbound {
        source: NodeId,
        payload: Bytes,
    },
    DiscoveryUpdated(Arc<DiscoveryIndex>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PubSubRejectReason {
    UnknownTopic(String),
    SequenceExhausted(TopicKey),
    EnvelopeTooLarge { encoded_len: usize, max: usize },
    InvalidEnvelope(EnvelopeError),
    SourceMismatch { header: NodeId, envelope: NodeId },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PubSubDropReason {
    DuplicateOrTooOld {
        origin: NodeId,
        topic: TopicKey,
        seq: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PubSubAction {
    SendMulticast {
        destinations: Vec<NodeId>,
        priority: Priority,
        flow_id: u64,
        conflate_key: u64,
        conflatable: bool,
        payload: Bytes,
    },
    DeliverToApp {
        sub_id: SubId,
        message: DeliveredMessage,
    },
    LocalSubscriptionsChanged(Vec<TopicSelector>),
    Published {
        topic: TopicKey,
        seq: u64,
    },
    Rejected(PubSubRejectReason),
    Dropped(PubSubDropReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReceiveWindow {
    highest: u64,
    bitmap: u64,
}

impl ReceiveWindow {
    fn first(seq: u64) -> Self {
        Self {
            highest: seq,
            bitmap: 1,
        }
    }

    fn accept(&mut self, seq: u64) -> bool {
        if seq > self.highest {
            let shift = seq - self.highest;
            self.bitmap = if shift >= u64::BITS as u64 {
                1
            } else {
                (self.bitmap << shift) | 1
            };
            self.highest = seq;
            return true;
        }
        let age = self.highest - seq;
        if age >= u64::BITS as u64 {
            return false;
        }
        let bit = 1_u64 << age;
        if self.bitmap & bit != 0 {
            false
        } else {
            self.bitmap |= bit;
            true
        }
    }
}

pub struct PubSub {
    me: NodeId,
    policies: Arc<TopicPolicies>,
    local_subscriptions: BTreeMap<SubId, TopicSelector>,
    discovery: Arc<DiscoveryIndex>,
    next_sequences: BTreeMap<TopicKey, u64>,
    received: BTreeMap<(NodeId, TopicKey), ReceiveWindow>,
}

impl PubSub {
    pub fn new(me: NodeId, policies: Arc<TopicPolicies>) -> Self {
        Self {
            me,
            policies,
            local_subscriptions: BTreeMap::new(),
            discovery: Arc::new(DiscoveryIndex::default()),
            next_sequences: BTreeMap::new(),
            received: BTreeMap::new(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.me
    }

    fn effective_subscriptions(&self) -> BTreeSet<TopicSelector> {
        self.local_subscriptions.values().cloned().collect()
    }

    fn subscription_change(&self, previous: BTreeSet<TopicSelector>) -> Vec<PubSubAction> {
        let current = self.effective_subscriptions();
        if current == previous {
            Vec::new()
        } else {
            vec![PubSubAction::LocalSubscriptionsChanged(
                current.into_iter().collect(),
            )]
        }
    }

    fn publish(&mut self, now: MonoTime, topic: TopicKey, payload: Bytes) -> Vec<PubSubAction> {
        let Some(policy) = self.policies.get(topic.name()) else {
            return vec![PubSubAction::Rejected(PubSubRejectReason::UnknownTopic(
                topic.name().to_owned(),
            ))];
        };
        let sequence = self.next_sequences.entry(topic.clone()).or_default();
        let Some(seq) = sequence.checked_add(1) else {
            return vec![PubSubAction::Rejected(
                PubSubRejectReason::SequenceExhausted(topic),
            )];
        };
        let envelope = Envelope {
            origin: self.me,
            topic: topic.clone(),
            seq,
            ts_ms: now.as_millis(),
            payload,
        };
        let encoded = EnvelopeCodec::encode(&envelope)
            .expect("validated in-memory topic and non-zero sequence must encode");
        if encoded.len() > MAX_LIVE_ENVELOPE_LEN {
            return vec![PubSubAction::Rejected(
                PubSubRejectReason::EnvelopeTooLarge {
                    encoded_len: encoded.len(),
                    max: MAX_LIVE_ENVELOPE_LEN,
                },
            )];
        }
        *sequence = seq;
        match self.received.get_mut(&(self.me, topic.clone())) {
            Some(window) => {
                let accepted = window.accept(envelope.seq);
                debug_assert!(
                    accepted,
                    "a new local sequence must advance its receive window"
                );
            }
            None => {
                self.received
                    .insert((self.me, topic.clone()), ReceiveWindow::first(envelope.seq));
            }
        }

        let mut actions = Vec::new();
        let mut destinations = self.discovery.subscribers(&topic);
        destinations.remove(&self.me);
        let flow_id = stable_hash(&[topic.name().as_bytes(), topic.partition().as_ref()]);
        let conflate_key = stable_hash(&[self.me.as_bytes(), topic.partition().as_ref()]);
        let destination_chunk_limit = MAX_MULTICAST_DESTINATIONS
            .min((MAX_FORWARD_PAYLOAD_LEN - encoded.len() - MULTICAST_COUNT_LEN) / NODE_ID_LEN);
        for chunk in destinations
            .into_iter()
            .collect::<Vec<_>>()
            .chunks(destination_chunk_limit)
        {
            actions.push(PubSubAction::SendMulticast {
                destinations: chunk.to_vec(),
                priority: policy.priority,
                flow_id,
                conflate_key,
                conflatable: policy.mode == TopicMode::Latest && policy.priority == Priority::P1,
                payload: encoded.clone(),
            });
        }
        actions.push(PubSubAction::Published { topic, seq });
        actions
    }

    fn receive(&mut self, source: NodeId, payload: Bytes) -> Vec<PubSubAction> {
        let envelope = match EnvelopeCodec::decode(payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                return vec![PubSubAction::Rejected(PubSubRejectReason::InvalidEnvelope(
                    error,
                ))]
            }
        };
        if envelope.origin != source {
            return vec![PubSubAction::Rejected(PubSubRejectReason::SourceMismatch {
                header: source,
                envelope: envelope.origin,
            })];
        }
        if self.policies.get(envelope.topic.name()).is_none() {
            return vec![PubSubAction::Rejected(PubSubRejectReason::UnknownTopic(
                envelope.topic.name().to_owned(),
            ))];
        }

        let key = (envelope.origin, envelope.topic.clone());
        let accepted = match self.received.get_mut(&key) {
            Some(window) => window.accept(envelope.seq),
            None => {
                self.received
                    .insert(key, ReceiveWindow::first(envelope.seq));
                true
            }
        };
        if !accepted {
            return vec![PubSubAction::Dropped(PubSubDropReason::DuplicateOrTooOld {
                origin: envelope.origin,
                topic: envelope.topic.clone(),
                seq: envelope.seq,
            })];
        }
        self.local_deliveries(envelope)
    }

    fn local_deliveries(&self, envelope: Envelope) -> Vec<PubSubAction> {
        self.local_subscriptions
            .iter()
            .filter(|(_, selector)| selector.matches(&envelope.topic))
            .map(|(sub_id, _)| PubSubAction::DeliverToApp {
                sub_id: *sub_id,
                message: DeliveredMessage {
                    envelope: envelope.clone(),
                    is_backfill: false,
                },
            })
            .collect()
    }
}

impl Component for PubSub {
    type Event = PubSubEvent;
    type Action = PubSubAction;

    fn handle(&mut self, now: MonoTime, event: Self::Event) -> Vec<Self::Action> {
        match event {
            PubSubEvent::LocalPublish { topic, payload } => self.publish(now, topic, payload),
            PubSubEvent::LocalSubscribe { sub_id, selector } => {
                let previous = self.effective_subscriptions();
                self.local_subscriptions.insert(sub_id, selector);
                self.subscription_change(previous)
            }
            PubSubEvent::LocalUnsubscribe(sub_id) => {
                let previous = self.effective_subscriptions();
                self.local_subscriptions.remove(&sub_id);
                self.subscription_change(previous)
            }
            PubSubEvent::Inbound { source, payload } => self.receive(source, payload),
            PubSubEvent::DiscoveryUpdated(discovery) => {
                self.discovery = discovery;
                Vec::new()
            }
        }
    }
}

fn stable_hash(parts: &[&[u8]]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for part in parts {
        for byte in *part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: u8) -> NodeId {
        let mut bytes = [0; 32];
        bytes[31] = value;
        NodeId::from_bytes(bytes)
    }

    fn topic(partition: &'static [u8]) -> TopicKey {
        TopicKey::new("tracks", Bytes::from_static(partition)).expect("test topic must be valid")
    }

    fn policies() -> Arc<TopicPolicies> {
        Arc::new(
            TopicPolicies::new([(
                "tracks".to_owned(),
                TopicPolicy {
                    priority: Priority::P1,
                    mode: TopicMode::Latest,
                },
            )])
            .expect("test policy must be valid"),
        )
    }

    fn envelope(origin: NodeId, sequence: u64) -> Envelope {
        Envelope {
            origin,
            topic: topic(b"drone-17"),
            seq: sequence,
            ts_ms: 10,
            payload: Bytes::from_static(b"position"),
        }
    }

    #[test]
    fn envelope_round_trips_with_a_recognizable_prefix() {
        let original = envelope(node(1), 7);
        let encoded = EnvelopeCodec::encode(&original).expect("envelope must encode");

        assert!(EnvelopeCodec::is_envelope(&encoded));
        assert_eq!(
            EnvelopeCodec::decode(encoded).expect("envelope must decode"),
            original
        );
        assert_eq!(
            EnvelopeCodec::decode(Bytes::from_static(b"raw payload")),
            Err(EnvelopeError::NotPubSub)
        );
    }

    #[test]
    fn publish_emits_canonical_multicast_and_ack() {
        let me = node(1);
        let key = topic(b"drone-17");
        let mut pubsub = PubSub::new(me, policies());
        let mut discovery = DiscoveryIndex::default();
        discovery.set_subscribers(TopicSelector::exact(key.clone()), [node(3), node(2)]);
        pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::DiscoveryUpdated(Arc::new(discovery)),
        );

        let actions = pubsub.handle(
            MonoTime::from_millis(25),
            PubSubEvent::LocalPublish {
                topic: key.clone(),
                payload: Bytes::from_static(b"position"),
            },
        );

        assert!(matches!(
            &actions[0],
            PubSubAction::SendMulticast {
                destinations,
                priority: Priority::P1,
                conflatable: true,
                ..
            } if destinations == &vec![node(2), node(3)]
        ));
        assert_eq!(actions.len(), 2);
        assert_eq!(
            actions.last(),
            Some(&PubSubAction::Published { topic: key, seq: 1 })
        );
    }

    #[test]
    fn inbound_message_is_delivered_to_a_matching_subscription() {
        let origin = node(1);
        let mut pubsub = PubSub::new(node(2), policies());
        pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::LocalSubscribe {
                sub_id: SubId::new(9),
                selector: TopicSelector::exact(topic(b"drone-17")),
            },
        );
        let received = envelope(origin, 7);

        let actions = pubsub.handle(
            MonoTime::from_millis(25),
            PubSubEvent::Inbound {
                source: origin,
                payload: EnvelopeCodec::encode(&received).expect("envelope must encode"),
            },
        );

        assert_eq!(
            actions,
            vec![PubSubAction::DeliverToApp {
                sub_id: SubId::new(9),
                message: DeliveredMessage {
                    envelope: received,
                    is_backfill: false,
                },
            }]
        );
    }

    #[test]
    fn subscription_ads_change_only_when_the_effective_selector_set_changes() {
        let mut pubsub = PubSub::new(node(1), policies());
        let selector = TopicSelector::exact(topic(b"drone-17"));

        assert_eq!(
            pubsub.handle(
                MonoTime::ZERO,
                PubSubEvent::LocalSubscribe {
                    sub_id: SubId::new(1),
                    selector: selector.clone(),
                },
            ),
            vec![PubSubAction::LocalSubscriptionsChanged(vec![
                selector.clone()
            ])]
        );
        assert!(pubsub
            .handle(
                MonoTime::ZERO,
                PubSubEvent::LocalSubscribe {
                    sub_id: SubId::new(2),
                    selector,
                },
            )
            .is_empty());
        assert!(pubsub
            .handle(MonoTime::ZERO, PubSubEvent::LocalUnsubscribe(SubId::new(1)),)
            .is_empty());
        assert_eq!(
            pubsub.handle(MonoTime::ZERO, PubSubEvent::LocalUnsubscribe(SubId::new(2)),),
            vec![PubSubAction::LocalSubscriptionsChanged(Vec::new())]
        );
    }

    #[test]
    fn inbound_dedup_accepts_reordering_once_and_rejects_duplicates() {
        let origin = node(1);
        let mut pubsub = PubSub::new(node(2), policies());
        pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::LocalSubscribe {
                sub_id: SubId::new(1),
                selector: TopicSelector::exact(topic(b"drone-17")),
            },
        );

        for sequence in [2, 1] {
            let actions = pubsub.handle(
                MonoTime::ZERO,
                PubSubEvent::Inbound {
                    source: origin,
                    payload: EnvelopeCodec::encode(&envelope(origin, sequence))
                        .expect("envelope must encode"),
                },
            );
            assert!(matches!(
                actions.as_slice(),
                [PubSubAction::DeliverToApp { .. }]
            ));
        }

        let duplicate = pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::Inbound {
                source: origin,
                payload: EnvelopeCodec::encode(&envelope(origin, 1)).expect("envelope must encode"),
            },
        );
        assert!(matches!(
            duplicate.as_slice(),
            [PubSubAction::Dropped(PubSubDropReason::DuplicateOrTooOld {
                seq: 1,
                ..
            })]
        ));
    }

    #[test]
    fn header_source_must_match_the_envelope_origin() {
        let mut pubsub = PubSub::new(node(3), policies());
        let actions = pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::Inbound {
                source: node(2),
                payload: EnvelopeCodec::encode(&envelope(node(1), 1))
                    .expect("envelope must encode"),
            },
        );
        assert!(matches!(
            actions.as_slice(),
            [PubSubAction::Rejected(PubSubRejectReason::SourceMismatch {
                header,
                envelope,
            })] if *header == node(2) && *envelope == node(1)
        ));
    }

    #[test]
    fn multicast_is_chunked_to_the_wire_destination_limit() {
        let key = topic(b"drone-17");
        let mut pubsub = PubSub::new(node(1), policies());
        let mut discovery = DiscoveryIndex::default();
        discovery.set_subscribers(TopicSelector::exact(key.clone()), (2..=70).map(node));
        pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::DiscoveryUpdated(Arc::new(discovery)),
        );

        let actions = pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::LocalPublish {
                topic: key,
                payload: Bytes::new(),
            },
        );
        let chunk_lengths = actions
            .iter()
            .filter_map(|action| match action {
                PubSubAction::SendMulticast { destinations, .. } => Some(destinations.len()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(chunk_lengths, vec![MAX_MULTICAST_DESTINATIONS, 5]);
    }

    #[test]
    fn oversized_publish_is_rejected_without_consuming_a_sequence() {
        let key = topic(b"drone-17");
        let mut pubsub = PubSub::new(node(1), policies());

        let rejected = pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::LocalPublish {
                topic: key.clone(),
                payload: Bytes::from(vec![0; MAX_LIVE_ENVELOPE_LEN]),
            },
        );
        assert!(matches!(
            rejected.as_slice(),
            [PubSubAction::Rejected(
                PubSubRejectReason::EnvelopeTooLarge { .. }
            )]
        ));

        let accepted = pubsub.handle(
            MonoTime::ZERO,
            PubSubEvent::LocalPublish {
                topic: key.clone(),
                payload: Bytes::new(),
            },
        );
        assert_eq!(
            accepted.last(),
            Some(&PubSubAction::Published { topic: key, seq: 1 })
        );
    }

    #[test]
    fn identical_inputs_produce_identical_actions() {
        fn run() -> Vec<PubSubAction> {
            let mut pubsub = PubSub::new(node(1), policies());
            let mut discovery = DiscoveryIndex::default();
            discovery.set_subscribers(
                TopicSelector::all_partitions("tracks").expect("selector must be valid"),
                [node(3), node(2)],
            );
            pubsub.handle(
                MonoTime::ZERO,
                PubSubEvent::DiscoveryUpdated(Arc::new(discovery)),
            );
            pubsub.handle(
                MonoTime::from_millis(50),
                PubSubEvent::LocalPublish {
                    topic: topic(b"drone-17"),
                    payload: Bytes::from_static(b"position"),
                },
            )
        }

        assert_eq!(run(), run());
    }
}
