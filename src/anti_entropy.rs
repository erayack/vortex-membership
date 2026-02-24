use std::collections::HashMap;

use crate::state::{ApplyResult, MembershipStore};
use crate::types::{MemberDigest, MembershipUpdate};

#[derive(Clone, Debug, Default)]
pub struct AntiEntropy;

impl AntiEntropy {
    #[must_use]
    pub fn build_digest(store: &MembershipStore) -> Vec<MemberDigest> {
        store.digest()
    }

    #[must_use]
    pub fn diff_for_remote(
        store: &MembershipStore,
        remote: &[MemberDigest],
    ) -> Vec<MembershipUpdate> {
        let remote_by_id: HashMap<_, _> = remote
            .iter()
            .map(|digest| (digest.node_id.clone(), digest.incarnation))
            .collect();

        let local_node_id = store.local_node_id().clone();
        let mut updates = Vec::new();

        for member in store.snapshot_members() {
            let remote_incarnation = remote_by_id.get(&member.node_id).copied().unwrap_or(0);
            // Digest-only comparison cannot detect status/timestamp divergence
            // within the same incarnation, so we also ship equal-incarnation
            // entries as a convergence backstop.
            if member.incarnation < remote_incarnation {
                continue;
            }

            updates.push(MembershipUpdate {
                node_id: member.node_id.clone(),
                addr: member.addr,
                incarnation: member.incarnation,
                status: member.status,
                last_changed_ms: member.last_changed_ms,
                origin_node_id: member.node_id,
                source_node_id: local_node_id.clone(),
            });
        }

        updates
    }

    pub fn apply_delta(store: &mut MembershipStore, updates: Vec<MembershipUpdate>, now_ms: u64) {
        for update in updates {
            let _ = store.apply_update(update, now_ms);
        }
    }

    #[must_use]
    pub const fn should_run(last_run_ms: u64, now_ms: u64, interval_ms: u64) -> bool {
        now_ms.saturating_sub(last_run_ms) >= interval_ms
    }

    #[must_use]
    pub fn apply_delta_with_results(
        store: &mut MembershipStore,
        updates: Vec<MembershipUpdate>,
        now_ms: u64,
    ) -> Vec<ApplyResult> {
        updates
            .into_iter()
            .map(|update| store.apply_update(update, now_ms))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::AntiEntropy;
    use crate::state::MembershipStore;
    use crate::types::{MemberDigest, MemberStatus, MembershipUpdate, NodeId};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn update(node_id: &str, incarnation: u64) -> MembershipUpdate {
        MembershipUpdate {
            node_id: NodeId::from(node_id),
            addr: addr(7_000),
            incarnation,
            status: MemberStatus::Alive,
            last_changed_ms: incarnation,
            origin_node_id: NodeId::from(node_id),
            source_node_id: NodeId::from(node_id),
        }
    }

    #[test]
    fn build_digest_includes_all_known_members() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9_000), 1_000);
        let _ = store.apply_update(update("node-a", 2), 10);
        let _ = store.apply_update(update("node-b", 3), 11);

        let digest = AntiEntropy::build_digest(&store);

        assert_eq!(digest.len(), 3);
        assert!(
            digest
                .iter()
                .any(|entry| entry.node_id == NodeId::from("local"))
        );
        assert!(
            digest
                .iter()
                .any(|entry| entry.node_id == NodeId::from("node-a"))
        );
        assert!(
            digest
                .iter()
                .any(|entry| entry.node_id == NodeId::from("node-b"))
        );
    }

    #[test]
    fn diff_for_remote_returns_only_newer_entries() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9_000), 1_000);
        let _ = store.apply_update(update("node-a", 2), 10);
        let _ = store.apply_update(update("node-b", 3), 11);

        let remote = vec![
            MemberDigest {
                node_id: NodeId::from("local"),
                incarnation: 0,
            },
            MemberDigest {
                node_id: NodeId::from("node-a"),
                incarnation: 2,
            },
        ];

        let delta = AntiEntropy::diff_for_remote(&store, &remote);

        assert_eq!(delta.len(), 3);
        assert!(
            delta
                .iter()
                .any(|entry| entry.node_id == NodeId::from("local"))
        );
        assert!(
            delta
                .iter()
                .any(|entry| entry.node_id == NodeId::from("node-a"))
        );
        assert!(
            delta
                .iter()
                .any(|entry| entry.node_id == NodeId::from("node-b"))
        );
    }

    #[test]
    fn diff_for_remote_includes_same_incarnation_for_convergence() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9_000), 1_000);
        let _ = store.apply_update(update("node-a", 2), 10);

        let remote = vec![
            MemberDigest {
                node_id: NodeId::from("local"),
                incarnation: 0,
            },
            MemberDigest {
                node_id: NodeId::from("node-a"),
                incarnation: 2,
            },
        ];

        let delta = AntiEntropy::diff_for_remote(&store, &remote);

        assert!(
            delta
                .iter()
                .any(|entry| entry.node_id == NodeId::from("node-a"))
        );
    }

    #[test]
    fn apply_delta_routes_updates_through_store() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9_000), 1_000);

        AntiEntropy::apply_delta(&mut store, vec![update("node-a", 2)], 100);

        let members = store.snapshot_members();
        assert!(
            members
                .iter()
                .any(|entry| { entry.node_id == NodeId::from("node-a") && entry.incarnation == 2 })
        );
    }
}
