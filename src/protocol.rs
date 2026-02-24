use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::types::{MemberDigest, MemberRecord, MembershipUpdate, NodeId, ProbeSeq, ViewEpoch};

pub const MAX_PIGGYBACK_UPDATES: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WireMessage {
    Ping {
        seq: ProbeSeq,
        from: NodeId,
        piggyback: Vec<MembershipUpdate>,
    },
    Ack {
        seq: ProbeSeq,
        from: NodeId,
        piggyback: Vec<MembershipUpdate>,
    },
    PingReq {
        seq: ProbeSeq,
        from: NodeId,
        target: NodeId,
        piggyback: Vec<MembershipUpdate>,
    },
    Join {
        from: NodeId,
        addr: SocketAddr,
    },
    JoinAck {
        view_epoch: ViewEpoch,
        accepted: MemberRecord,
        members: Vec<MemberRecord>,
    },
    SyncDigest {
        from: NodeId,
        view_epoch: ViewEpoch,
        digest: Vec<MemberDigest>,
    },
    SyncDelta {
        from: NodeId,
        view_epoch: ViewEpoch,
        updates: Vec<MembershipUpdate>,
    },
}

impl WireMessage {
    #[must_use]
    pub fn ping(seq: ProbeSeq, from: NodeId, piggyback: Vec<MembershipUpdate>) -> Self {
        Self::Ping {
            seq,
            from,
            piggyback: bound_piggyback(piggyback),
        }
    }

    #[must_use]
    pub fn ack(seq: ProbeSeq, from: NodeId, piggyback: Vec<MembershipUpdate>) -> Self {
        Self::Ack {
            seq,
            from,
            piggyback: bound_piggyback(piggyback),
        }
    }

    #[must_use]
    pub fn ping_req(
        seq: ProbeSeq,
        from: NodeId,
        target: NodeId,
        piggyback: Vec<MembershipUpdate>,
    ) -> Self {
        Self::PingReq {
            seq,
            from,
            target,
            piggyback: bound_piggyback(piggyback),
        }
    }
}

fn bound_piggyback(mut piggyback: Vec<MembershipUpdate>) -> Vec<MembershipUpdate> {
    piggyback.truncate(MAX_PIGGYBACK_UPDATES);
    piggyback
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::{MAX_PIGGYBACK_UPDATES, WireMessage};
    use crate::types::{MemberStatus, MembershipUpdate, NodeId};

    fn update(idx: usize) -> MembershipUpdate {
        MembershipUpdate {
            node_id: NodeId::from(format!("node-{idx}")),
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7000)),
            incarnation: idx as u64,
            status: MemberStatus::Alive,
            last_changed_ms: idx as u64,
            origin_node_id: NodeId::from("node-origin"),
            source_node_id: NodeId::from("node-source"),
        }
    }

    #[test]
    fn ping_constructor_bounds_piggyback() {
        let piggyback = (0..(MAX_PIGGYBACK_UPDATES + 5)).map(update).collect();
        let message = WireMessage::ping(1, NodeId::from("self"), piggyback);

        match message {
            WireMessage::Ping { piggyback, .. } => {
                assert_eq!(piggyback.len(), MAX_PIGGYBACK_UPDATES);
            }
            _ => panic!("expected ping message"),
        }
    }
}
