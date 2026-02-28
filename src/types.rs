use std::fmt;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Stable logical node identifier used for ordering, hashing, and map keys.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

pub type Incarnation = u64;
pub type ViewEpoch = u64;
pub type ProbeSeq = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum MemberStatus {
    Alive,
    Suspect,
    Dead,
    Left,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum MembershipEventKind {
    Join,
    Suspect,
    Dead,
    Left,
    Recovered,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipEvent {
    pub kind: MembershipEventKind,
    pub node_id: NodeId,
    pub previous_status: Option<MemberStatus>,
    pub current_status: MemberStatus,
    pub incarnation: Incarnation,
    pub view_epoch: ViewEpoch,
    pub observed_by: NodeId,
    pub at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberRecord {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub incarnation: Incarnation,
    pub status: MemberStatus,
    pub last_changed_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct UpdateVersion {
    pub incarnation: Incarnation,
    pub status: MemberStatus,
    pub last_changed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberDigest {
    pub node_id: NodeId,
    pub incarnation: Incarnation,
    pub status: MemberStatus,
    pub last_changed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MembershipUpdate {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub incarnation: Incarnation,
    pub status: MemberStatus,
    pub last_changed_ms: u64,
    /// Node that originally emitted this update.
    pub origin_node_id: NodeId,
    /// Node that last forwarded this update.
    pub source_node_id: NodeId,
}

impl MembershipUpdate {
    #[must_use]
    pub const fn version(&self) -> UpdateVersion {
        UpdateVersion {
            incarnation: self.incarnation,
            status: self.status,
            last_changed_ms: self.last_changed_ms,
        }
    }

    /// Returns true when `self` should replace `other` for the same target node.
    #[must_use]
    pub fn supersedes(&self, other: &Self) -> bool {
        if self.node_id != other.node_id {
            return false;
        }

        self.version() > other.version()
    }
}

impl From<&MembershipUpdate> for UpdateVersion {
    fn from(value: &MembershipUpdate) -> Self {
        value.version()
    }
}

impl From<&MemberRecord> for UpdateVersion {
    fn from(value: &MemberRecord) -> Self {
        Self {
            incarnation: value.incarnation,
            status: value.status,
            last_changed_ms: value.last_changed_ms,
        }
    }
}

impl From<&MemberDigest> for UpdateVersion {
    fn from(value: &MemberDigest) -> Self {
        Self {
            incarnation: value.incarnation,
            status: value.status,
            last_changed_ms: value.last_changed_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::{MemberStatus, MembershipUpdate, NodeId};

    fn update(incarnation: u64, status: MemberStatus, changed_ms: u64) -> MembershipUpdate {
        MembershipUpdate {
            node_id: NodeId::from("node-a"),
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7000)),
            incarnation,
            status,
            last_changed_ms: changed_ms,
            origin_node_id: NodeId::from("node-a"),
            source_node_id: NodeId::from("node-b"),
        }
    }

    #[test]
    fn higher_incarnation_supersedes() {
        let old = update(1, MemberStatus::Alive, 1_000);
        let new = update(2, MemberStatus::Alive, 900);

        assert!(new.supersedes(&old));
        assert!(!old.supersedes(&new));
    }

    #[test]
    fn status_precedence_supersedes_on_same_incarnation() {
        let alive = update(5, MemberStatus::Alive, 1_000);
        let suspect = update(5, MemberStatus::Suspect, 1_001);

        assert!(suspect.supersedes(&alive));
        assert!(!alive.supersedes(&suspect));
    }

    #[test]
    fn newer_timestamp_breaks_ties() {
        let first = update(5, MemberStatus::Suspect, 1_000);
        let second = update(5, MemberStatus::Suspect, 1_500);

        assert!(second.supersedes(&first));
        assert!(!first.supersedes(&second));
    }
}
