use std::hash::Hasher;

use twox_hash::XxHash64;

use crate::types::{MemberRecord, MemberStatus, NodeId};

const DEFAULT_RENDEZVOUS_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnershipResolver {
    seed: u64,
}

impl Default for OwnershipResolver {
    fn default() -> Self {
        Self::new(DEFAULT_RENDEZVOUS_SEED)
    }
}

impl OwnershipResolver {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { seed }
    }

    #[must_use]
    pub fn owner(&self, key: &[u8], members: &[MemberRecord]) -> Option<NodeId> {
        self.top_k(key, members, 1).into_iter().next()
    }

    #[must_use]
    pub fn top_k(&self, key: &[u8], members: &[MemberRecord], k: usize) -> Vec<NodeId> {
        if k == 0 {
            return Vec::new();
        }

        let mut scored_nodes: Vec<_> = members
            .iter()
            .filter(|member| member.status == MemberStatus::Alive)
            .map(|member| {
                (
                    self.score_node(key, &member.node_id),
                    member.node_id.clone(),
                )
            })
            .collect();

        scored_nodes.sort_unstable_by(|(left_score, left_id), (right_score, right_id)| {
            right_score
                .cmp(left_score)
                .then_with(|| left_id.cmp(right_id))
        });

        scored_nodes
            .into_iter()
            .take(k)
            .map(|(_, node_id)| node_id)
            .collect()
    }

    fn score_node(&self, key: &[u8], node_id: &NodeId) -> u64 {
        let mut hasher = XxHash64::with_seed(self.seed);
        hasher.write_u64(key.len() as u64);
        hasher.write(key);
        hasher.write_u8(0xff);
        hasher.write(node_id.as_str().as_bytes());
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::OwnershipResolver;
    use crate::types::{MemberRecord, MemberStatus, NodeId};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn member(node_id: &str, status: MemberStatus, incarnation: u64) -> MemberRecord {
        MemberRecord {
            node_id: NodeId::from(node_id),
            addr: addr(7000),
            incarnation,
            status,
            last_changed_ms: incarnation,
        }
    }

    #[test]
    fn owner_returns_none_for_empty_membership() {
        let resolver = OwnershipResolver::default();
        assert_eq!(resolver.owner(b"key-a", &[]), None);
    }

    #[test]
    fn owner_is_deterministic_for_member_order() {
        let resolver = OwnershipResolver::default();
        let members = vec![
            member("node-a", MemberStatus::Alive, 1),
            member("node-b", MemberStatus::Alive, 1),
            member("node-c", MemberStatus::Alive, 1),
        ];
        let reversed = vec![
            member("node-c", MemberStatus::Alive, 1),
            member("node-b", MemberStatus::Alive, 1),
            member("node-a", MemberStatus::Alive, 1),
        ];

        assert_eq!(
            resolver.owner(b"key-a", &members),
            resolver.owner(b"key-a", &reversed)
        );
    }

    #[test]
    fn top_k_ignores_non_alive_members() {
        let resolver = OwnershipResolver::default();
        let members = vec![
            member("node-a", MemberStatus::Alive, 1),
            member("node-b", MemberStatus::Dead, 1),
            member("node-c", MemberStatus::Suspect, 1),
            member("node-d", MemberStatus::Alive, 1),
        ];

        let top = resolver.top_k(b"key-a", &members, 4);
        assert_eq!(top.len(), 2);
        assert!(top.contains(&NodeId::from("node-a")));
        assert!(top.contains(&NodeId::from("node-d")));
    }

    #[test]
    fn top_k_is_prefix_stable() {
        let resolver = OwnershipResolver::default();
        let members = vec![
            member("node-a", MemberStatus::Alive, 1),
            member("node-b", MemberStatus::Alive, 1),
            member("node-c", MemberStatus::Alive, 1),
            member("node-d", MemberStatus::Alive, 1),
        ];

        let top1 = resolver.top_k(b"key-a", &members, 1);
        let top3 = resolver.top_k(b"key-a", &members, 3);
        assert_eq!(top1[0], top3[0]);
    }
}
