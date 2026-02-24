use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;

use crate::types::{MemberDigest, MemberRecord, MemberStatus, MembershipUpdate, NodeId, ViewEpoch};

const DEFAULT_RECENT_UPDATE_LIMIT: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyResult {
    Applied { epoch_bumped: bool },
    IgnoredStale,
    RequiresRefutation(NodeId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerEligibleSnapshot {
    pub view_epoch: ViewEpoch,
    pub members: Vec<MemberRecord>,
}

#[derive(Clone, Debug)]
pub struct MembershipStore {
    members: HashMap<NodeId, MemberRecord>,
    view_epoch: ViewEpoch,
    recent_updates: VecDeque<MembershipUpdate>,
    quarantine_until_ms: HashMap<NodeId, u64>,
    local_node_id: NodeId,
    local_addr: SocketAddr,
    quarantine_ms: u64,
    recent_update_limit: usize,
}

impl MembershipStore {
    #[must_use]
    pub fn new(local_node_id: NodeId, local_addr: SocketAddr, quarantine_ms: u64) -> Self {
        let local_record = MemberRecord {
            node_id: local_node_id.clone(),
            addr: local_addr,
            incarnation: 0,
            status: MemberStatus::Alive,
            last_changed_ms: 0,
        };

        let mut members = HashMap::new();
        members.insert(local_node_id.clone(), local_record);

        Self {
            members,
            view_epoch: 0,
            recent_updates: VecDeque::new(),
            quarantine_until_ms: HashMap::new(),
            local_node_id,
            local_addr,
            quarantine_ms,
            recent_update_limit: DEFAULT_RECENT_UPDATE_LIMIT,
        }
    }

    pub fn apply_update(&mut self, update: MembershipUpdate, now_ms: u64) -> ApplyResult {
        let current = self.members.get(&update.node_id).cloned();

        if update.node_id == self.local_node_id {
            return self.apply_local_update(update, current, now_ms);
        }

        match current {
            None => self.commit_update(update, now_ms),
            Some(current) => match update.incarnation.cmp(&current.incarnation) {
                std::cmp::Ordering::Greater => self.commit_update(update, now_ms),
                std::cmp::Ordering::Less => ApplyResult::IgnoredStale,
                std::cmp::Ordering::Equal => {
                    self.apply_same_incarnation_remote(update, &current, now_ms)
                }
            },
        }
    }

    #[must_use]
    pub fn mark_local_alive_with_new_incarnation(&mut self, now_ms: u64) -> MembershipUpdate {
        let next_incarnation = self
            .members
            .get(&self.local_node_id)
            .map_or(1, |record| record.incarnation.saturating_add(1));

        let update = MembershipUpdate {
            node_id: self.local_node_id.clone(),
            addr: self.local_addr,
            incarnation: next_incarnation,
            status: MemberStatus::Alive,
            last_changed_ms: now_ms,
            origin_node_id: self.local_node_id.clone(),
            source_node_id: self.local_node_id.clone(),
        };

        let _ = self.apply_update(update.clone(), now_ms);
        update
    }

    #[must_use]
    pub fn snapshot_alive(&self) -> Vec<MemberRecord> {
        let mut alive: Vec<_> = self
            .members
            .values()
            .filter(|record| record.status == MemberStatus::Alive)
            .cloned()
            .collect();
        alive.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        alive
    }

    #[must_use]
    pub fn snapshot_owner_eligible(&self, now_ms: u64) -> Vec<MemberRecord> {
        let mut eligible: Vec<_> = self
            .members
            .values()
            .filter(|record| record.status == MemberStatus::Alive)
            .filter(|record| {
                self.quarantine_until_ms
                    .get(&record.node_id)
                    .is_none_or(|until_ms| now_ms >= *until_ms)
            })
            .cloned()
            .collect();
        eligible.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        eligible
    }

    #[must_use]
    pub fn snapshot_owner_eligible_with_epoch(&self, now_ms: u64) -> OwnerEligibleSnapshot {
        OwnerEligibleSnapshot {
            view_epoch: self.view_epoch,
            members: self.snapshot_owner_eligible(now_ms),
        }
    }

    #[must_use]
    pub fn digest(&self) -> Vec<MemberDigest> {
        let mut digest: Vec<_> = self
            .members
            .values()
            .map(|record| MemberDigest {
                node_id: record.node_id.clone(),
                incarnation: record.incarnation,
            })
            .collect();
        digest.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        digest
    }

    #[must_use]
    pub fn snapshot_members(&self) -> Vec<MemberRecord> {
        let mut members: Vec<_> = self.members.values().cloned().collect();
        members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        members
    }

    #[must_use]
    pub const fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    #[must_use]
    pub const fn view_epoch(&self) -> ViewEpoch {
        self.view_epoch
    }

    fn apply_local_update(
        &mut self,
        update: MembershipUpdate,
        current: Option<MemberRecord>,
        now_ms: u64,
    ) -> ApplyResult {
        let Some(current) = current else {
            return self.commit_update(update, now_ms);
        };

        match update.incarnation.cmp(&current.incarnation) {
            std::cmp::Ordering::Greater => self.commit_update(update, now_ms),
            std::cmp::Ordering::Less => ApplyResult::IgnoredStale,
            std::cmp::Ordering::Equal => {
                if update.status == MemberStatus::Alive {
                    if current.status == MemberStatus::Suspect {
                        return ApplyResult::RequiresRefutation(self.local_node_id.clone());
                    }
                    if matches!(current.status, MemberStatus::Dead | MemberStatus::Left) {
                        return ApplyResult::RequiresRefutation(self.local_node_id.clone());
                    }
                    return self.apply_same_status_update(update, &current, now_ms);
                }

                ApplyResult::RequiresRefutation(self.local_node_id.clone())
            }
        }
    }

    fn apply_same_incarnation_remote(
        &mut self,
        update: MembershipUpdate,
        current: &MemberRecord,
        now_ms: u64,
    ) -> ApplyResult {
        if update.status == current.status {
            return self.apply_same_status_update(update, current, now_ms);
        }

        if matches!(current.status, MemberStatus::Dead | MemberStatus::Left)
            && matches!(update.status, MemberStatus::Alive | MemberStatus::Suspect)
        {
            return ApplyResult::IgnoredStale;
        }

        if current.status == MemberStatus::Suspect && update.status == MemberStatus::Alive {
            return ApplyResult::IgnoredStale;
        }

        if update.status > current.status {
            return self.commit_update(update, now_ms);
        }

        ApplyResult::IgnoredStale
    }

    fn apply_same_status_update(
        &mut self,
        update: MembershipUpdate,
        current: &MemberRecord,
        now_ms: u64,
    ) -> ApplyResult {
        let has_newer_timestamp = update.last_changed_ms > current.last_changed_ms;
        let has_addr_change = update.addr != current.addr;

        if has_newer_timestamp || has_addr_change {
            return self.commit_update(update, now_ms);
        }

        ApplyResult::IgnoredStale
    }

    fn commit_update(&mut self, update: MembershipUpdate, now_ms: u64) -> ApplyResult {
        let new_record = MemberRecord {
            node_id: update.node_id.clone(),
            addr: update.addr,
            incarnation: update.incarnation,
            status: update.status,
            last_changed_ms: update.last_changed_ms,
        };

        let changed = self
            .members
            .get(&update.node_id)
            .is_none_or(|current| *current != new_record);

        if !changed {
            return ApplyResult::IgnoredStale;
        }

        self.members.insert(update.node_id.clone(), new_record);
        self.apply_quarantine_effects(&update.node_id, update.status, now_ms);
        self.push_recent_update(update);
        self.view_epoch = self.view_epoch.saturating_add(1);

        ApplyResult::Applied { epoch_bumped: true }
    }

    fn apply_quarantine_effects(&mut self, node_id: &NodeId, status: MemberStatus, now_ms: u64) {
        match status {
            MemberStatus::Dead | MemberStatus::Left => {
                self.quarantine_until_ms
                    .insert(node_id.clone(), now_ms.saturating_add(self.quarantine_ms));
            }
            MemberStatus::Alive | MemberStatus::Suspect => {}
        }
    }

    fn push_recent_update(&mut self, update: MembershipUpdate) {
        if self.recent_update_limit == 0 {
            return;
        }

        if self.recent_updates.len() == self.recent_update_limit {
            self.recent_updates.pop_front();
        }
        self.recent_updates.push_back(update);
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::{ApplyResult, MembershipStore};
    use crate::types::{MemberStatus, MembershipUpdate, NodeId};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn update(
        node_id: &str,
        status: MemberStatus,
        incarnation: u64,
        last_changed_ms: u64,
        origin: &str,
        source: &str,
    ) -> MembershipUpdate {
        MembershipUpdate {
            node_id: NodeId::from(node_id),
            addr: addr(7000),
            incarnation,
            status,
            last_changed_ms,
            origin_node_id: NodeId::from(origin),
            source_node_id: NodeId::from(source),
        }
    }

    #[test]
    fn higher_incarnation_always_wins() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("node-a", MemberStatus::Suspect, 1, 10, "node-a", "node-a"),
            10,
        );
        let result = store.apply_update(
            update("node-a", MemberStatus::Alive, 2, 20, "node-a", "node-b"),
            20,
        );

        assert_eq!(result, ApplyResult::Applied { epoch_bumped: true });
        let alive = store.snapshot_alive();
        assert!(
            alive
                .iter()
                .any(|member| member.node_id == NodeId::from("node-a"))
        );
    }

    #[test]
    fn same_incarnation_alive_does_not_override_suspect_for_remote() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("node-a", MemberStatus::Suspect, 7, 100, "node-a", "node-b"),
            100,
        );
        let result = store.apply_update(
            update("node-a", MemberStatus::Alive, 7, 101, "node-a", "node-c"),
            101,
        );

        assert_eq!(result, ApplyResult::IgnoredStale);
    }

    #[test]
    fn local_same_incarnation_suspect_requires_refutation() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let result = store.apply_update(
            update("local", MemberStatus::Suspect, 0, 123, "node-x", "node-x"),
            123,
        );

        assert_eq!(
            result,
            ApplyResult::RequiresRefutation(NodeId::from("local"))
        );
    }

    #[test]
    fn dead_blocks_stale_resurrection_without_new_incarnation() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("node-a", MemberStatus::Dead, 3, 50, "node-a", "node-b"),
            50,
        );
        let result = store.apply_update(
            update("node-a", MemberStatus::Alive, 3, 80, "node-a", "node-c"),
            80,
        );

        assert_eq!(result, ApplyResult::IgnoredStale);
    }

    #[test]
    fn quarantine_filters_owner_eligible_until_expiry() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("node-a", MemberStatus::Dead, 2, 5, "node-a", "node-b"),
            10,
        );
        let _ = store.apply_update(
            update("node-a", MemberStatus::Alive, 3, 11, "node-a", "node-c"),
            11,
        );

        let owner_eligible = store.snapshot_owner_eligible(500);
        assert!(
            owner_eligible
                .iter()
                .all(|member| member.node_id != NodeId::from("node-a"))
        );

        let owner_eligible_after = store.snapshot_owner_eligible(1_500);
        assert!(
            owner_eligible_after
                .iter()
                .any(|member| member.node_id == NodeId::from("node-a"))
        );
    }

    #[test]
    fn local_dead_requires_refutation_to_become_alive_at_same_incarnation() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("local", MemberStatus::Dead, 1, 100, "node-x", "node-x"),
            100,
        );
        let result = store.apply_update(
            update("local", MemberStatus::Alive, 1, 200, "local", "local"),
            200,
        );

        assert_eq!(
            result,
            ApplyResult::RequiresRefutation(NodeId::from("local"))
        );
    }

    #[test]
    fn local_left_same_incarnation_alive_is_not_applied() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("local", MemberStatus::Left, 1, 100, "node-x", "node-x"),
            100,
        );
        let result = store.apply_update(
            update("local", MemberStatus::Alive, 1, 200, "local", "local"),
            200,
        );

        assert!(matches!(
            result,
            ApplyResult::RequiresRefutation(_) | ApplyResult::IgnoredStale
        ));
    }

    #[test]
    fn local_refutation_bumps_incarnation_and_restores_alive() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("local", MemberStatus::Dead, 1, 100, "node-x", "node-x"),
            100,
        );
        assert!(
            store
                .snapshot_alive()
                .iter()
                .all(|member| member.node_id != NodeId::from("local"))
        );

        let refutation = store.mark_local_alive_with_new_incarnation(200);
        assert_eq!(refutation.incarnation, 2);
        assert_eq!(refutation.status, MemberStatus::Alive);
        assert!(
            store
                .snapshot_alive()
                .iter()
                .any(|member| member.node_id == NodeId::from("local"))
        );
    }

    #[test]
    fn quarantine_persists_for_alive_after_left_until_expiry() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("node-a", MemberStatus::Left, 2, 5, "node-a", "node-b"),
            10,
        );
        let _ = store.apply_update(
            update("node-a", MemberStatus::Alive, 3, 11, "node-a", "node-c"),
            11,
        );
        let _ = store.apply_update(
            update("node-a", MemberStatus::Alive, 4, 12, "node-a", "node-c"),
            12,
        );

        let owner_eligible = store.snapshot_owner_eligible(500);
        assert!(
            owner_eligible
                .iter()
                .all(|member| member.node_id != NodeId::from("node-a"))
        );

        let owner_eligible_after = store.snapshot_owner_eligible(1_500);
        assert!(
            owner_eligible_after
                .iter()
                .any(|member| member.node_id == NodeId::from("node-a"))
        );
    }

    #[test]
    fn mark_local_alive_increments_incarnation_and_epoch() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let update = store.mark_local_alive_with_new_incarnation(99);

        assert_eq!(update.incarnation, 1);
        assert_eq!(update.status, MemberStatus::Alive);
        assert_eq!(store.view_epoch(), 1);
    }

    #[test]
    fn owner_eligible_snapshot_returns_epoch_and_filtered_members() {
        let mut store = MembershipStore::new(NodeId::from("local"), addr(9000), 1_000);

        let _ = store.apply_update(
            update("node-a", MemberStatus::Dead, 2, 5, "node-a", "node-b"),
            10,
        );
        let _ = store.apply_update(
            update("node-a", MemberStatus::Alive, 3, 11, "node-a", "node-c"),
            11,
        );

        let snapshot_before = store.snapshot_owner_eligible_with_epoch(500);
        assert_eq!(snapshot_before.view_epoch, store.view_epoch());
        assert!(
            snapshot_before
                .members
                .iter()
                .all(|member| member.node_id != NodeId::from("node-a"))
        );

        let snapshot_after = store.snapshot_owner_eligible_with_epoch(1_500);
        assert_eq!(snapshot_after.view_epoch, store.view_epoch());
        assert!(
            snapshot_after
                .members
                .iter()
                .any(|member| member.node_id == NodeId::from("node-a"))
        );
    }
}
