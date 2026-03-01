use std::hash::Hasher;
use std::num::NonZeroUsize;

use lru::LruCache;

use twox_hash::XxHash64;

use crate::types::{MemberRecord, MemberStatus, NodeId};

const DEFAULT_RENDEZVOUS_SEED: u64 = 0x9e37_79b9_7f4a_7c15;
const DEFAULT_STICKY_HASH_SEED: u64 = 0xd6e8_feb8_6659_fd93;
const DEFAULT_STICKY_STABILITY_WINDOW_MS: u64 = 3_000;
const DEFAULT_STICKY_MAX_TRACKED_KEYS: usize = 16_384;
const DEFAULT_STICKY_FAST_CUTOVER_ON_TERMINAL: bool = true;

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

        let mut alive_nodes: Vec<_> = members
            .iter()
            .filter(|member| member.status == MemberStatus::Alive)
            .map(|member| member.node_id.clone())
            .collect();
        alive_nodes.sort_unstable();
        alive_nodes.dedup();

        let mut scored_nodes: Vec<_> = alive_nodes
            .into_iter()
            .map(|node_id| (self.score_node(key, &node_id), node_id))
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StickyOwnershipConfig {
    pub stability_window_ms: u64,
    pub max_tracked_keys: usize,
    pub fast_cutover_on_terminal: bool,
}

impl Default for StickyOwnershipConfig {
    fn default() -> Self {
        Self {
            stability_window_ms: DEFAULT_STICKY_STABILITY_WINDOW_MS,
            max_tracked_keys: DEFAULT_STICKY_MAX_TRACKED_KEYS,
            fast_cutover_on_terminal: DEFAULT_STICKY_FAST_CUTOVER_ON_TERMINAL,
        }
    }
}

impl StickyOwnershipConfig {
    #[must_use]
    pub const fn new(
        stability_window_ms: u64,
        max_tracked_keys: usize,
        fast_cutover_on_terminal: bool,
    ) -> Self {
        Self {
            stability_window_ms,
            max_tracked_keys,
            fast_cutover_on_terminal,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StickyDecision {
    Stable {
        owner: NodeId,
    },
    Pending {
        stable_owner: NodeId,
        candidate_owner: NodeId,
        pending_since_ms: u64,
    },
    NoRoute,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TrackedKeyState {
    stable_owner: Option<NodeId>,
    candidate_owner: Option<NodeId>,
    candidate_since_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct StickyOwnershipResolver {
    base: OwnershipResolver,
    config: StickyOwnershipConfig,
    tracked_keys: LruCache<u64, TrackedKeyState>,
}

impl StickyOwnershipResolver {
    #[must_use]
    pub fn new(base: OwnershipResolver, mut config: StickyOwnershipConfig) -> Self {
        if config.max_tracked_keys == 0 {
            config.max_tracked_keys = 1;
        }
        let cap = NonZeroUsize::new(config.max_tracked_keys).unwrap_or(NonZeroUsize::MIN);
        Self {
            base,
            config,
            tracked_keys: LruCache::new(cap),
        }
    }

    #[must_use]
    pub fn decide(
        &mut self,
        key: &[u8],
        owner_eligible: &[MemberRecord],
        all_members: &[MemberRecord],
        now_ms: u64,
    ) -> StickyDecision {
        let key_hash = hash_key(key);
        let candidate_owner = self.base.owner(key, owner_eligible);

        let Some(candidate_owner) = candidate_owner else {
            if let Some(state) = self.tracked_keys.get_mut(&key_hash) {
                clear_pending(state);
            }
            return StickyDecision::NoRoute;
        };

        let state = self
            .tracked_keys
            .get_or_insert_mut(key_hash, || TrackedKeyState {
                stable_owner: Some(candidate_owner.clone()),
                candidate_owner: None,
                candidate_since_ms: None,
            });

        let Some(stable_owner) = state.stable_owner.clone() else {
            state.stable_owner = Some(candidate_owner.clone());
            clear_pending(state);
            return StickyDecision::Stable {
                owner: candidate_owner,
            };
        };

        if stable_owner == candidate_owner {
            clear_pending(state);
            return StickyDecision::Stable {
                owner: stable_owner,
            };
        }

        if should_cut_over_immediately(
            self.config.fast_cutover_on_terminal,
            &stable_owner,
            all_members,
        ) {
            state.stable_owner = Some(candidate_owner.clone());
            clear_pending(state);
            return StickyDecision::Stable {
                owner: candidate_owner,
            };
        }

        let pending_since_ms = if state.candidate_owner.as_ref() == Some(&candidate_owner) {
            if let Some(candidate_since_ms) = state.candidate_since_ms {
                candidate_since_ms
            } else {
                state.candidate_since_ms = Some(now_ms);
                now_ms
            }
        } else {
            state.candidate_owner = Some(candidate_owner.clone());
            state.candidate_since_ms = Some(now_ms);
            now_ms
        };

        if now_ms.saturating_sub(pending_since_ms) >= self.config.stability_window_ms {
            state.stable_owner = Some(candidate_owner.clone());
            clear_pending(state);
            return StickyDecision::Stable {
                owner: candidate_owner,
            };
        }

        StickyDecision::Pending {
            stable_owner,
            candidate_owner,
            pending_since_ms,
        }
    }
}

fn clear_pending(state: &mut TrackedKeyState) {
    state.candidate_owner = None;
    state.candidate_since_ms = None;
}

fn should_cut_over_immediately(
    fast_cutover_on_terminal: bool,
    stable_owner: &NodeId,
    all_members: &[MemberRecord],
) -> bool {
    if !fast_cutover_on_terminal {
        return false;
    }

    all_members
        .iter()
        .find(|member| member.node_id == *stable_owner)
        .is_none_or(|member| matches!(member.status, MemberStatus::Dead | MemberStatus::Left))
}

fn hash_key(key: &[u8]) -> u64 {
    let mut hasher = XxHash64::with_seed(DEFAULT_STICKY_HASH_SEED);
    hasher.write_u64(key.len() as u64);
    hasher.write(key);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    use super::{
        OwnershipResolver, StickyDecision, StickyOwnershipConfig, StickyOwnershipResolver,
    };
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

    #[test]
    fn top_k_deduplicates_repeated_node_ids() {
        let resolver = OwnershipResolver::default();
        let members = vec![
            member("node-a", MemberStatus::Alive, 1),
            member("node-a", MemberStatus::Alive, 2),
            member("node-b", MemberStatus::Alive, 1),
        ];

        let top = resolver.top_k(b"key-a", &members, 4);
        assert_eq!(top.len(), 2);
    }

    #[test]
    fn sticky_returns_no_route_when_no_owner_eligible() {
        let mut resolver = StickyOwnershipResolver::new(
            OwnershipResolver::default(),
            StickyOwnershipConfig::new(100, 128, true),
        );

        assert_eq!(
            resolver.decide(b"key-a", &[], &[], 10),
            StickyDecision::NoRoute
        );
    }

    #[test]
    fn sticky_pending_then_confirms_after_stability_window() {
        let mut resolver = StickyOwnershipResolver::new(
            OwnershipResolver::default(),
            StickyOwnershipConfig::new(100, 128, false),
        );
        let owner_a = NodeId::from("node-a");
        let owner_b = NodeId::from("node-b");

        let alive_a = vec![member("node-a", MemberStatus::Alive, 1)];
        assert_eq!(
            resolver.decide(b"key-a", &alive_a, &alive_a, 0),
            StickyDecision::Stable {
                owner: owner_a.clone(),
            }
        );

        let owner_eligible = vec![member("node-b", MemberStatus::Alive, 2)];
        let all_members = vec![
            member("node-a", MemberStatus::Dead, 2),
            member("node-b", MemberStatus::Alive, 2),
        ];

        assert_eq!(
            resolver.decide(b"key-a", &owner_eligible, &all_members, 10),
            StickyDecision::Pending {
                stable_owner: owner_a.clone(),
                candidate_owner: owner_b.clone(),
                pending_since_ms: 10,
            }
        );

        assert_eq!(
            resolver.decide(b"key-a", &owner_eligible, &all_members, 80),
            StickyDecision::Pending {
                stable_owner: owner_a,
                candidate_owner: owner_b.clone(),
                pending_since_ms: 10,
            }
        );

        assert_eq!(
            resolver.decide(b"key-a", &owner_eligible, &all_members, 110),
            StickyDecision::Stable { owner: owner_b }
        );
    }

    #[test]
    fn sticky_cancels_pending_when_candidate_reverts() {
        let mut resolver = StickyOwnershipResolver::new(
            OwnershipResolver::default(),
            StickyOwnershipConfig::new(100, 128, false),
        );
        let owner_a = NodeId::from("node-a");
        let owner_b = NodeId::from("node-b");
        let alive_a = vec![member("node-a", MemberStatus::Alive, 1)];

        let _ = resolver.decide(b"key-a", &alive_a, &alive_a, 0);

        let owner_eligible_b = vec![member("node-b", MemberStatus::Alive, 2)];
        let all_members_ab = vec![
            member("node-a", MemberStatus::Alive, 1),
            member("node-b", MemberStatus::Alive, 2),
        ];
        assert_eq!(
            resolver.decide(b"key-a", &owner_eligible_b, &all_members_ab, 10),
            StickyDecision::Pending {
                stable_owner: owner_a.clone(),
                candidate_owner: owner_b,
                pending_since_ms: 10,
            }
        );

        assert_eq!(
            resolver.decide(b"key-a", &alive_a, &all_members_ab, 20),
            StickyDecision::Stable { owner: owner_a }
        );
    }

    #[test]
    fn sticky_fast_cutover_promotes_candidate_on_terminal_stable_owner() {
        let mut resolver = StickyOwnershipResolver::new(
            OwnershipResolver::default(),
            StickyOwnershipConfig::new(10_000, 128, true),
        );
        let owner_b = NodeId::from("node-b");

        let alive_a = vec![member("node-a", MemberStatus::Alive, 1)];
        let _ = resolver.decide(b"key-a", &alive_a, &alive_a, 0);

        let owner_eligible = vec![member("node-b", MemberStatus::Alive, 2)];
        let all_members = vec![
            member("node-a", MemberStatus::Dead, 2),
            member("node-b", MemberStatus::Alive, 2),
        ];
        assert_eq!(
            resolver.decide(b"key-a", &owner_eligible, &all_members, 5),
            StickyDecision::Stable {
                owner: owner_b.clone(),
            }
        );

        assert_eq!(
            resolver.decide(b"key-a", &owner_eligible, &all_members, 6),
            StickyDecision::Stable { owner: owner_b }
        );
    }

    #[test]
    fn sticky_evicts_oldest_key_when_capacity_is_reached() {
        let mut resolver = StickyOwnershipResolver::new(
            OwnershipResolver::default(),
            StickyOwnershipConfig::new(100, 1, false),
        );
        let alive_a = vec![member("node-a", MemberStatus::Alive, 1)];
        let alive_b = vec![member("node-b", MemberStatus::Alive, 1)];
        let all_members = vec![
            member("node-a", MemberStatus::Alive, 1),
            member("node-b", MemberStatus::Alive, 1),
        ];

        let _ = resolver.decide(b"key-a", &alive_a, &all_members, 0);
        let _ = resolver.decide(b"key-b", &alive_a, &all_members, 1);

        // key-a was evicted, so it should initialize directly to stable owner instead of pending.
        assert_eq!(
            resolver.decide(b"key-a", &alive_b, &all_members, 2),
            StickyDecision::Stable {
                owner: NodeId::from("node-b"),
            }
        );
    }
}
