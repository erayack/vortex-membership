use std::collections::VecDeque;

use bincode::config;

use crate::protocol::WireMessage;
use crate::transport::MAX_PACKET_SIZE;
use crate::types::MembershipUpdate;

const DEFAULT_RETRANSMIT_CONSTANT: usize = 1;
const DEFAULT_RETRANSMIT_MULTIPLIER: f64 = 4.0;

#[derive(Clone, Debug)]
struct QueuedUpdate {
    update: MembershipUpdate,
    remaining_transmits: usize,
}

#[derive(Clone, Debug)]
pub struct Disseminator {
    queue: VecDeque<QueuedUpdate>,
    cluster_size: usize,
    retransmit_constant: usize,
    retransmit_multiplier: f64,
    remaining_budget_bytes: usize,
}

impl Disseminator {
    #[must_use]
    pub const fn new(cluster_size: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            cluster_size,
            retransmit_constant: DEFAULT_RETRANSMIT_CONSTANT,
            retransmit_multiplier: DEFAULT_RETRANSMIT_MULTIPLIER,
            remaining_budget_bytes: MAX_PACKET_SIZE,
        }
    }

    #[must_use]
    pub fn with_retransmit_multiplier(cluster_size: usize, retransmit_multiplier: f64) -> Self {
        Self {
            retransmit_multiplier: normalize_retransmit_multiplier(retransmit_multiplier),
            ..Self::new(cluster_size)
        }
    }

    pub fn enqueue(&mut self, update: MembershipUpdate) {
        let retransmit_budget = self.retransmit_budget();

        if let Some(existing) = self
            .queue
            .iter_mut()
            .find(|entry| entry.update.node_id == update.node_id)
        {
            if update.supersedes(&existing.update) {
                existing.update = update;
                existing.remaining_transmits = retransmit_budget;
            }
            return;
        }

        self.queue.push_back(QueuedUpdate {
            update,
            remaining_transmits: retransmit_budget,
        });
    }

    pub fn next_piggyback_batch(&mut self, max_items: usize) -> Vec<MembershipUpdate> {
        let mut batch = Vec::new();
        let mut remaining_budget = self.remaining_budget_bytes.min(MAX_PACKET_SIZE);

        if max_items == 0 || remaining_budget == 0 {
            return batch;
        }

        let mut scanned = 0;
        let initial_len = self.queue.len();

        while batch.len() < max_items && scanned < initial_len {
            let Some(mut entry) = self.queue.pop_front() else {
                break;
            };

            scanned += 1;

            let encoded_len = encoded_update_len(&entry.update);
            if encoded_len > remaining_budget {
                self.queue.push_back(entry);
                continue;
            }

            remaining_budget -= encoded_len;
            batch.push(entry.update.clone());

            if entry.remaining_transmits > 1 {
                entry.remaining_transmits -= 1;
                self.queue.push_back(entry);
            }
        }

        batch
    }

    pub const fn set_cluster_size(&mut self, cluster_size: usize) {
        self.cluster_size = cluster_size;
    }

    pub const fn set_remaining_budget_bytes(&mut self, remaining_budget_bytes: usize) {
        self.remaining_budget_bytes = remaining_budget_bytes;
    }

    #[must_use]
    pub fn attach_to_message(&mut self, message: WireMessage) -> WireMessage {
        let base_size = bincode::serde::encode_to_vec(&message, config::standard())
            .ok()
            .map_or(MAX_PACKET_SIZE, |bytes| bytes.len());

        if base_size >= MAX_PACKET_SIZE {
            return message;
        }

        self.set_remaining_budget_bytes(MAX_PACKET_SIZE - base_size);

        match message {
            WireMessage::Ping {
                seq,
                from,
                piggyback,
            } => WireMessage::ping(seq, from, self.extend_piggyback(piggyback)),
            WireMessage::Ack {
                seq,
                from,
                piggyback,
            } => WireMessage::ack(seq, from, self.extend_piggyback(piggyback)),
            WireMessage::PingReq {
                seq,
                from,
                target,
                piggyback,
            } => WireMessage::ping_req(seq, from, target, self.extend_piggyback(piggyback)),
            _ => message,
        }
    }

    fn extend_piggyback(&mut self, mut existing: Vec<MembershipUpdate>) -> Vec<MembershipUpdate> {
        let max_updates = crate::protocol::MAX_PIGGYBACK_UPDATES;
        let remaining_slots = max_updates.saturating_sub(existing.len());
        if remaining_slots == 0 {
            existing.truncate(max_updates);
            return existing;
        }

        let mut fresh = self.next_piggyback_batch(remaining_slots);
        existing.append(&mut fresh);
        existing.truncate(max_updates);
        existing
    }

    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn retransmit_budget(&self) -> usize {
        let n = self.cluster_size.max(1);
        let n_f64 = n as f64;
        let swim_budget = if n <= 1 {
            1
        } else {
            let scaled = self.retransmit_multiplier * n_f64.log10();
            scaled.ceil() as usize
        };
        swim_budget
            .saturating_add(self.retransmit_constant)
            .saturating_sub(1)
            .max(1)
    }
}

fn encoded_update_len(update: &MembershipUpdate) -> usize {
    bincode::serde::encode_to_vec(update, config::standard())
        .ok()
        .map_or(MAX_PACKET_SIZE, |bytes| bytes.len())
}

fn normalize_retransmit_multiplier(multiplier: f64) -> f64 {
    if multiplier.is_finite() && multiplier > 0.0 {
        multiplier
    } else {
        DEFAULT_RETRANSMIT_MULTIPLIER
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::Disseminator;
    use crate::protocol::WireMessage;
    use crate::types::{MemberStatus, MembershipUpdate, NodeId};

    fn update(node_id: &str, incarnation: u64) -> MembershipUpdate {
        MembershipUpdate {
            node_id: NodeId::from(node_id),
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7000)),
            incarnation,
            status: MemberStatus::Alive,
            last_changed_ms: incarnation,
            origin_node_id: NodeId::from(node_id),
            source_node_id: NodeId::from(node_id),
        }
    }

    #[test]
    fn enqueue_replaces_stale_by_node_id() {
        let mut disseminator = Disseminator::new(8);

        disseminator.enqueue(update("node-a", 1));
        disseminator.enqueue(update("node-a", 3));

        let batch = disseminator.next_piggyback_batch(4);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].incarnation, 3);
    }

    #[test]
    fn retransmit_budget_exhausts_update() {
        let mut disseminator = Disseminator::new(8);
        disseminator.enqueue(update("node-a", 1));

        let first = disseminator.next_piggyback_batch(1);
        let second = disseminator.next_piggyback_batch(1);
        let third = disseminator.next_piggyback_batch(1);
        let fourth = disseminator.next_piggyback_batch(1);
        let fifth = disseminator.next_piggyback_batch(1);

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(third.len(), 1);
        assert_eq!(fourth.len(), 1);
        assert!(fifth.is_empty());
    }

    #[test]
    fn retransmit_budget_scales_with_log10_cluster_size() {
        let mut disseminator = Disseminator::with_retransmit_multiplier(10, 3.0);
        disseminator.enqueue(update("node-a", 1));

        let first = disseminator.next_piggyback_batch(1);
        let second = disseminator.next_piggyback_batch(1);
        let third = disseminator.next_piggyback_batch(1);
        let fourth = disseminator.next_piggyback_batch(1);

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(third.len(), 1);
        assert!(fourth.is_empty());
    }

    #[test]
    fn next_batch_respects_byte_budget() {
        let mut disseminator = Disseminator::new(8);
        disseminator.enqueue(update("node-a", 1));
        disseminator.enqueue(update("node-b", 1));
        disseminator.set_remaining_budget_bytes(1);

        let batch = disseminator.next_piggyback_batch(8);
        assert!(batch.is_empty());
    }

    #[test]
    fn attach_to_message_piggybacks_updates() {
        let mut disseminator = Disseminator::new(8);
        disseminator.enqueue(update("node-a", 1));

        let message = WireMessage::ping(1, NodeId::from("self"), Vec::new());
        let message = disseminator.attach_to_message(message);

        match message {
            WireMessage::Ping { piggyback, .. } => assert_eq!(piggyback.len(), 1),
            _ => panic!("expected ping"),
        }
    }

    #[test]
    fn attach_to_message_preserves_existing_piggyback() {
        let mut disseminator = Disseminator::new(8);
        disseminator.enqueue(update("node-b", 1));

        let existing = vec![update("node-a", 1)];
        let message = WireMessage::ping(1, NodeId::from("self"), existing);
        let message = disseminator.attach_to_message(message);

        match message {
            WireMessage::Ping { piggyback, .. } => {
                assert!(
                    piggyback
                        .iter()
                        .any(|entry| entry.node_id == NodeId::from("node-a"))
                );
                assert!(
                    piggyback
                        .iter()
                        .any(|entry| entry.node_id == NodeId::from("node-b"))
                );
            }
            _ => panic!("expected ping"),
        }
    }
}
