use std::collections::{BTreeMap, BTreeSet};

use crate::types::{MemberRecord, MemberStatus, NodeId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DetectorAction {
    SendPing {
        target: NodeId,
        seq: u64,
    },
    SendPingReq {
        helper: NodeId,
        target: NodeId,
        seq: u64,
    },
    MarkSuspect {
        target: NodeId,
    },
    MarkDead {
        target: NodeId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingProbe {
    target: NodeId,
    direct_deadline_ms: u64,
    indirect_deadline_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SuspectTimer {
    deadline_ms: u64,
    seq: u64,
}

#[derive(Clone, Debug)]
pub struct FailureDetector {
    local_node_id: NodeId,
    probe_interval_ms: u64,
    ack_timeout_ms: u64,
    indirect_ping_count: usize,
    suspect_timeout_ms: u64,
    next_probe_due_ms: Option<u64>,
    next_seq: u64,
    probe_cursor: usize,
    pending_probes: BTreeMap<u64, PendingProbe>,
    suspect_deadlines: BTreeMap<NodeId, SuspectTimer>,
    last_view: Vec<MemberRecord>,
}

impl FailureDetector {
    #[must_use]
    pub const fn new(
        local_node_id: NodeId,
        probe_interval_ms: u64,
        ack_timeout_ms: u64,
        indirect_ping_count: usize,
        suspect_timeout_ms: u64,
    ) -> Self {
        Self {
            local_node_id,
            probe_interval_ms,
            ack_timeout_ms,
            indirect_ping_count,
            suspect_timeout_ms,
            next_probe_due_ms: None,
            next_seq: 1,
            probe_cursor: 0,
            pending_probes: BTreeMap::new(),
            suspect_deadlines: BTreeMap::new(),
            last_view: Vec::new(),
        }
    }

    pub fn on_tick(&mut self, view: &[MemberRecord], now_ms: u64) -> Vec<DetectorAction> {
        self.last_view = view.to_vec();
        self.prune_with_view();

        let due = *self.next_probe_due_ms.get_or_insert(now_ms);
        if now_ms < due {
            return Vec::new();
        }

        self.next_probe_due_ms = Some(now_ms.saturating_add(self.probe_interval_ms));

        let Some(target) = self.select_next_target(view) else {
            return Vec::new();
        };

        let seq = self.alloc_seq();
        self.pending_probes.insert(
            seq,
            PendingProbe {
                target: target.clone(),
                direct_deadline_ms: now_ms.saturating_add(self.ack_timeout_ms),
                indirect_deadline_ms: None,
            },
        );

        vec![DetectorAction::SendPing { target, seq }]
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn on_ack(&mut self, seq: u64, from: NodeId, _now_ms: u64) {
        let matches_pending = self
            .pending_probes
            .get(&seq)
            .is_some_and(|pending| pending.target == from);
        if matches_pending {
            self.pending_probes.remove(&seq);
            self.suspect_deadlines.remove(&from);
            return;
        }

        let matches_suspect = self
            .suspect_deadlines
            .get(&from)
            .is_some_and(|timer| timer.seq == seq);
        if matches_suspect {
            self.suspect_deadlines.remove(&from);
        }
    }

    pub fn on_timeout(&mut self, now_ms: u64) -> Vec<DetectorAction> {
        let mut actions = Vec::new();
        let mut to_remove = Vec::new();

        let direct_timeouts: Vec<_> = self
            .pending_probes
            .iter()
            .filter(|(_, pending)| {
                pending.indirect_deadline_ms.is_none() && now_ms >= pending.direct_deadline_ms
            })
            .map(|(seq, pending)| (*seq, pending.target.clone()))
            .collect();

        for (seq, target) in direct_timeouts {
            let helpers = self.select_helpers(&target);
            if helpers.is_empty() {
                to_remove.push(seq);
                if self.arm_suspect_timer(target.clone(), seq, now_ms) {
                    actions.push(DetectorAction::MarkSuspect { target });
                }
                continue;
            }

            for helper in helpers {
                actions.push(DetectorAction::SendPingReq {
                    helper,
                    target: target.clone(),
                    seq,
                });
            }

            if let Some(pending) = self.pending_probes.get_mut(&seq) {
                pending.indirect_deadline_ms = Some(now_ms.saturating_add(self.ack_timeout_ms));
            }
        }

        let indirect_timeouts: Vec<_> = self
            .pending_probes
            .iter()
            .filter_map(|(seq, pending)| {
                pending.indirect_deadline_ms.and_then(|deadline| {
                    if now_ms >= deadline {
                        Some((*seq, pending.target.clone()))
                    } else {
                        None
                    }
                })
            })
            .collect();

        for (seq, target) in indirect_timeouts {
            to_remove.push(seq);
            if self.arm_suspect_timer(target.clone(), seq, now_ms) {
                actions.push(DetectorAction::MarkSuspect { target });
            }
        }

        to_remove.sort_unstable();
        to_remove.dedup();
        for seq in to_remove {
            self.pending_probes.remove(&seq);
        }

        let mut dead_nodes = Vec::new();
        for (node_id, timer) in &self.suspect_deadlines {
            if now_ms >= timer.deadline_ms {
                dead_nodes.push(node_id.clone());
            }
        }

        for node_id in dead_nodes {
            self.suspect_deadlines.remove(&node_id);
            self.pending_probes
                .retain(|_, probe| probe.target != node_id);
            actions.push(DetectorAction::MarkDead { target: node_id });
        }

        actions
    }

    const fn alloc_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        seq
    }

    fn select_next_target(&mut self, view: &[MemberRecord]) -> Option<NodeId> {
        let mut candidates: Vec<_> = view
            .iter()
            .filter(|member| member.node_id != self.local_node_id)
            .filter(|member| matches!(member.status, MemberStatus::Alive | MemberStatus::Suspect))
            .map(|member| member.node_id.clone())
            .collect();

        candidates.sort_unstable();

        if candidates.is_empty() {
            self.probe_cursor = 0;
            return None;
        }

        if self.probe_cursor >= candidates.len() {
            self.probe_cursor = 0;
        }

        let selected = candidates[self.probe_cursor].clone();
        self.probe_cursor = (self.probe_cursor + 1) % candidates.len();

        Some(selected)
    }

    fn select_helpers(&self, target: &NodeId) -> Vec<NodeId> {
        let mut helpers: Vec<_> = self
            .last_view
            .iter()
            .filter(|member| member.node_id != self.local_node_id)
            .filter(|member| member.node_id != *target)
            .filter(|member| member.status == MemberStatus::Alive)
            .map(|member| member.node_id.clone())
            .collect();

        helpers.sort_unstable();
        helpers.truncate(self.indirect_ping_count);
        helpers
    }

    fn arm_suspect_timer(&mut self, target: NodeId, seq: u64, now_ms: u64) -> bool {
        if self.suspect_deadlines.contains_key(&target) {
            return false;
        }

        self.suspect_deadlines.insert(
            target,
            SuspectTimer {
                deadline_ms: now_ms.saturating_add(self.suspect_timeout_ms),
                seq,
            },
        );
        true
    }

    fn prune_with_view(&mut self) {
        let mut reachable = BTreeSet::new();
        let mut suspects = BTreeSet::new();

        for member in &self.last_view {
            if member.node_id == self.local_node_id {
                continue;
            }

            if matches!(member.status, MemberStatus::Alive | MemberStatus::Suspect) {
                reachable.insert(member.node_id.clone());
            }
            if member.status == MemberStatus::Suspect {
                suspects.insert(member.node_id.clone());
            }
        }

        self.pending_probes
            .retain(|_, pending| reachable.contains(&pending.target));
        self.suspect_deadlines
            .retain(|node_id, _| suspects.contains(node_id));
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::{DetectorAction, FailureDetector};
    use crate::types::{MemberRecord, MemberStatus, NodeId};

    fn member(node_id: &str, status: MemberStatus) -> MemberRecord {
        MemberRecord {
            node_id: NodeId::from(node_id),
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7000)),
            incarnation: 1,
            status,
            last_changed_ms: 0,
        }
    }

    #[test]
    fn tick_sends_round_robin_pings() {
        let mut detector = FailureDetector::new(NodeId::from("self"), 100, 30, 2, 200);
        let view = vec![
            member("self", MemberStatus::Alive),
            member("node-b", MemberStatus::Alive),
            member("node-a", MemberStatus::Alive),
        ];

        let first = detector.on_tick(&view, 0);
        let second = detector.on_tick(&view, 100);

        assert_eq!(
            first,
            vec![DetectorAction::SendPing {
                target: NodeId::from("node-a"),
                seq: 1
            }]
        );
        assert_eq!(
            second,
            vec![DetectorAction::SendPing {
                target: NodeId::from("node-b"),
                seq: 2
            }]
        );
    }

    #[test]
    fn timeout_progresses_indirect_to_suspect_to_dead() {
        let mut detector = FailureDetector::new(NodeId::from("self"), 100, 25, 2, 80);
        let view = vec![
            member("self", MemberStatus::Alive),
            member("node-a", MemberStatus::Alive),
            member("node-b", MemberStatus::Alive),
            member("node-c", MemberStatus::Alive),
        ];

        let _ = detector.on_tick(&view, 0);
        let indirect = detector.on_timeout(25);

        assert_eq!(
            indirect,
            vec![
                DetectorAction::SendPingReq {
                    helper: NodeId::from("node-b"),
                    target: NodeId::from("node-a"),
                    seq: 1,
                },
                DetectorAction::SendPingReq {
                    helper: NodeId::from("node-c"),
                    target: NodeId::from("node-a"),
                    seq: 1,
                },
            ]
        );

        let suspect = detector.on_timeout(50);
        assert_eq!(
            suspect,
            vec![DetectorAction::MarkSuspect {
                target: NodeId::from("node-a")
            }]
        );

        let dead = detector.on_timeout(130);
        assert_eq!(
            dead,
            vec![DetectorAction::MarkDead {
                target: NodeId::from("node-a")
            }]
        );
    }

    #[test]
    fn ack_clears_pending_probe_and_suspicion() {
        let mut detector = FailureDetector::new(NodeId::from("self"), 100, 20, 1, 80);
        let view = vec![
            member("self", MemberStatus::Alive),
            member("node-a", MemberStatus::Alive),
            member("node-b", MemberStatus::Alive),
        ];

        let _ = detector.on_tick(&view, 0);
        let _ = detector.on_timeout(20);
        let _ = detector.on_timeout(40);

        detector.on_ack(1, NodeId::from("node-a"), 45);

        let dead = detector.on_timeout(200);
        assert!(dead.is_empty());
    }

    #[test]
    fn ack_with_unknown_seq_does_not_clear_suspicion() {
        let mut detector = FailureDetector::new(NodeId::from("self"), 100, 20, 1, 80);
        let view = vec![
            member("self", MemberStatus::Alive),
            member("node-a", MemberStatus::Alive),
        ];

        let _ = detector.on_tick(&view, 0);
        let _ = detector.on_timeout(20);
        let _ = detector.on_timeout(40);

        detector.on_ack(999, NodeId::from("node-a"), 45);

        let dead = detector.on_timeout(120);
        assert_eq!(
            dead,
            vec![DetectorAction::MarkDead {
                target: NodeId::from("node-a")
            }]
        );
    }

    #[test]
    fn ack_with_wrong_sender_does_not_clear_suspicion() {
        let mut detector = FailureDetector::new(NodeId::from("self"), 100, 20, 2, 80);
        let view = vec![
            member("self", MemberStatus::Alive),
            member("node-a", MemberStatus::Alive),
            member("node-b", MemberStatus::Alive),
        ];

        let _ = detector.on_tick(&view, 0);
        let _ = detector.on_timeout(20);
        let _ = detector.on_timeout(40);

        detector.on_ack(1, NodeId::from("node-b"), 45);

        let dead = detector.on_timeout(120);
        assert_eq!(
            dead,
            vec![DetectorAction::MarkDead {
                target: NodeId::from("node-a")
            }]
        );
    }
}
