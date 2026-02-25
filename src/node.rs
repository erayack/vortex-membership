use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::select;
use tokio::sync::{mpsc, watch};
use tokio::time;

use crate::anti_entropy::AntiEntropy;
use crate::dissemination::Disseminator;
use crate::failure_detector::{DetectorAction, FailureDetector};
use crate::ownership::OwnershipResolver;
use crate::protocol::WireMessage;
use crate::state::{ApplyResult, MembershipStore};
use crate::transport::{TransportError, UdpTransport};
use crate::types::{MemberRecord, MemberStatus, MembershipUpdate, NodeId};

const RUNTIME_TICK_MS: u64 = 50;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteDecision {
    Local {
        view_epoch: u64,
    },
    Remote {
        owner: NodeId,
        addr: SocketAddr,
        view_epoch: u64,
    },
    NoRoute {
        view_epoch: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeEvent {
    DetectorMarked {
        emitter: NodeId,
        target: NodeId,
        status: MemberStatus,
        at_ms: u64,
    },
    MembershipApplied {
        observer: NodeId,
        update: MembershipUpdate,
        view_epoch: u64,
        alive_members: Vec<MemberRecord>,
        at_ms: u64,
    },
}

pub struct NodeRuntime {
    local_node_id: NodeId,
    anti_entropy_interval_ms: u64,
    transport: UdpTransport,
    store: MembershipStore,
    failure_detector: FailureDetector,
    disseminator: Disseminator,
    ownership: OwnershipResolver,
    observer: Option<mpsc::UnboundedSender<NodeEvent>>,
    started_at: Instant,
    last_anti_entropy_ms: u64,
}

impl NodeRuntime {
    #[must_use]
    pub fn new(
        local_node_id: NodeId,
        anti_entropy_interval_ms: u64,
        transport: UdpTransport,
        store: MembershipStore,
        failure_detector: FailureDetector,
        disseminator: Disseminator,
        ownership: OwnershipResolver,
    ) -> Self {
        Self {
            local_node_id,
            anti_entropy_interval_ms,
            transport,
            store,
            failure_detector,
            disseminator,
            ownership,
            observer: None,
            started_at: Instant::now(),
            last_anti_entropy_ms: 0,
        }
    }

    #[must_use]
    pub fn with_observer(mut self, observer: mpsc::UnboundedSender<NodeEvent>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Runs the node event loop until interrupted by CTRL-C.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError`] when receive/send operations fail.
    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut runtime_tick = time::interval(Duration::from_millis(RUNTIME_TICK_MS));

        loop {
            select! {
                _ = tokio::signal::ctrl_c() => {
                    return Ok(());
                }
                _ = runtime_tick.tick() => {
                    let now_ms = self.now_ms();
                    self.handle_detector_tick(now_ms).await?;
                    self.maybe_run_anti_entropy(now_ms).await?;
                }
                recv = self.transport.recv() => {
                    let (from_addr, msg) = recv.map_err(NodeError::Transport)?;
                    let now_ms = self.now_ms();
                    self.handle_inbound(from_addr, msg, now_ms).await?;
                }
            }
        }
    }

    /// Runs the node event loop until a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// Returns [`NodeError`] when receive/send operations fail.
    pub async fn run_until_shutdown(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), NodeError> {
        let mut runtime_tick = time::interval(Duration::from_millis(RUNTIME_TICK_MS));

        loop {
            select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = runtime_tick.tick() => {
                    let now_ms = self.now_ms();
                    self.handle_detector_tick(now_ms).await?;
                    self.maybe_run_anti_entropy(now_ms).await?;
                }
                recv = self.transport.recv() => {
                    let (from_addr, msg) = recv.map_err(NodeError::Transport)?;
                    let now_ms = self.now_ms();
                    self.handle_inbound(from_addr, msg, now_ms).await?;
                }
            }
        }
    }

    #[must_use]
    pub fn route_key(&self, key: &[u8]) -> RouteDecision {
        let now_ms = self.now_ms();
        let owner_snapshot = self.store.snapshot_owner_eligible_with_epoch(now_ms);
        let Some(owner) = self.ownership.owner(key, &owner_snapshot.members) else {
            return RouteDecision::NoRoute {
                view_epoch: owner_snapshot.view_epoch,
            };
        };

        if owner == self.local_node_id {
            return RouteDecision::Local {
                view_epoch: owner_snapshot.view_epoch,
            };
        }

        let owner_addr = self
            .store
            .snapshot_members()
            .into_iter()
            .find_map(|member| (member.node_id == owner).then_some(member.addr));

        match owner_addr {
            Some(addr) => RouteDecision::Remote {
                owner,
                addr,
                view_epoch: owner_snapshot.view_epoch,
            },
            None => RouteDecision::NoRoute {
                view_epoch: owner_snapshot.view_epoch,
            },
        }
    }

    fn now_ms(&self) -> u64 {
        let millis = self
            .started_at
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX));
        u64::try_from(millis).map_or(u64::MAX, |value| value)
    }

    async fn handle_detector_tick(&mut self, now_ms: u64) -> Result<(), NodeError> {
        let view = self.store.snapshot_alive();
        let mut actions = self.failure_detector.on_tick(&view, now_ms);
        actions.extend(self.failure_detector.on_timeout(now_ms));
        self.execute_detector_actions(actions, now_ms).await
    }

    async fn execute_detector_actions(
        &mut self,
        actions: Vec<DetectorAction>,
        now_ms: u64,
    ) -> Result<(), NodeError> {
        for action in actions {
            match action {
                DetectorAction::SendPing { target, seq } => {
                    if let Some(addr) = self.member_addr(&target) {
                        let message =
                            WireMessage::ping(seq, self.local_node_id.clone(), Vec::new());
                        let message = self.disseminator.attach_to_message(message);
                        self.transport
                            .send(addr, &message)
                            .await
                            .map_err(NodeError::Transport)?;
                    }
                }
                DetectorAction::SendPingReq {
                    helper,
                    target,
                    seq,
                } => {
                    if let Some(addr) = self.member_addr(&helper) {
                        let message = WireMessage::ping_req(
                            seq,
                            self.local_node_id.clone(),
                            target,
                            Vec::new(),
                        );
                        let message = self.disseminator.attach_to_message(message);
                        self.transport
                            .send(addr, &message)
                            .await
                            .map_err(NodeError::Transport)?;
                    }
                }
                DetectorAction::MarkSuspect { target } => {
                    self.emit_detector_event(&target, MemberStatus::Suspect, now_ms);
                    self.emit_detector_update(&target, MemberStatus::Suspect, now_ms);
                }
                DetectorAction::MarkDead { target } => {
                    self.emit_detector_event(&target, MemberStatus::Dead, now_ms);
                    self.emit_detector_update(&target, MemberStatus::Dead, now_ms);
                }
            }
        }

        Ok(())
    }

    fn emit_detector_update(&mut self, target: &NodeId, status: MemberStatus, now_ms: u64) {
        let Some(member) = self
            .store
            .snapshot_members()
            .into_iter()
            .find(|m| m.node_id == *target)
        else {
            return;
        };

        let update = MembershipUpdate {
            node_id: member.node_id.clone(),
            addr: member.addr,
            incarnation: member.incarnation,
            status,
            last_changed_ms: now_ms,
            origin_node_id: self.local_node_id.clone(),
            source_node_id: self.local_node_id.clone(),
        };
        self.apply_and_track(update, now_ms);
    }

    fn emit_detector_event(&self, target: &NodeId, status: MemberStatus, now_ms: u64) {
        if let Some(observer) = &self.observer {
            let _ = observer.send(NodeEvent::DetectorMarked {
                emitter: self.local_node_id.clone(),
                target: target.clone(),
                status,
                at_ms: now_ms,
            });
        }
    }

    async fn maybe_run_anti_entropy(&mut self, now_ms: u64) -> Result<(), NodeError> {
        if !AntiEntropy::should_run(
            self.last_anti_entropy_ms,
            now_ms,
            self.anti_entropy_interval_ms,
        ) {
            return Ok(());
        }

        self.last_anti_entropy_ms = now_ms;
        let digest = AntiEntropy::build_digest(&self.store);
        let peers: Vec<_> = self
            .store
            .snapshot_alive()
            .into_iter()
            .filter(|member| member.node_id != self.local_node_id)
            .map(|member| member.addr)
            .collect();

        for peer in peers {
            let message = WireMessage::SyncDigest {
                from: self.local_node_id.clone(),
                view_epoch: self.store.view_epoch(),
                digest: digest.clone(),
            };
            self.transport
                .send(peer, &message)
                .await
                .map_err(NodeError::Transport)?;
        }

        Ok(())
    }

    async fn handle_inbound(
        &mut self,
        from_addr: SocketAddr,
        message: WireMessage,
        now_ms: u64,
    ) -> Result<(), NodeError> {
        match message {
            WireMessage::Ping {
                seq,
                from: _,
                piggyback,
            } => {
                self.handle_ping(from_addr, seq, piggyback, now_ms).await?;
            }
            WireMessage::Ack {
                seq,
                from,
                piggyback,
            } => {
                self.handle_ack(seq, from, piggyback, now_ms);
            }
            WireMessage::PingReq {
                seq,
                from: _,
                target,
                piggyback,
            } => {
                self.handle_ping_req(from_addr, seq, target, piggyback, now_ms)
                    .await?;
            }
            WireMessage::Join { from, addr } => {
                self.handle_join(from_addr, from, addr, now_ms).await?;
            }
            WireMessage::JoinAck {
                accepted, members, ..
            } => {
                self.handle_join_ack(accepted, members, now_ms);
            }
            WireMessage::SyncDigest { digest, .. } => {
                self.handle_sync_digest(from_addr, digest).await?;
            }
            WireMessage::SyncDelta { updates, .. } => {
                self.handle_sync_delta(updates, now_ms);
            }
        }

        Ok(())
    }

    async fn handle_ping(
        &mut self,
        from_addr: SocketAddr,
        seq: u64,
        piggyback: Vec<MembershipUpdate>,
        now_ms: u64,
    ) -> Result<(), NodeError> {
        self.apply_piggyback(piggyback, now_ms);
        let ack = WireMessage::ack(seq, self.local_node_id.clone(), Vec::new());
        let ack = self.disseminator.attach_to_message(ack);
        self.transport
            .send(from_addr, &ack)
            .await
            .map_err(NodeError::Transport)
    }

    fn handle_ack(
        &mut self,
        seq: u64,
        from: NodeId,
        piggyback: Vec<MembershipUpdate>,
        now_ms: u64,
    ) {
        self.apply_piggyback(piggyback, now_ms);
        self.failure_detector.on_ack(seq, from, now_ms);
    }

    async fn handle_ping_req(
        &mut self,
        from_addr: SocketAddr,
        seq: u64,
        target: NodeId,
        piggyback: Vec<MembershipUpdate>,
        now_ms: u64,
    ) -> Result<(), NodeError> {
        self.apply_piggyback(piggyback, now_ms);
        if target != self.local_node_id {
            return Ok(());
        }

        let ack = WireMessage::ack(seq, self.local_node_id.clone(), Vec::new());
        let ack = self.disseminator.attach_to_message(ack);
        self.transport
            .send(from_addr, &ack)
            .await
            .map_err(NodeError::Transport)
    }

    async fn handle_join(
        &mut self,
        from_addr: SocketAddr,
        from: NodeId,
        addr: SocketAddr,
        now_ms: u64,
    ) -> Result<(), NodeError> {
        let update = MembershipUpdate {
            node_id: from.clone(),
            addr,
            incarnation: 0,
            status: MemberStatus::Alive,
            last_changed_ms: now_ms,
            origin_node_id: from.clone(),
            source_node_id: self.local_node_id.clone(),
        };
        self.apply_and_track(update, now_ms);

        let accepted = self
            .store
            .snapshot_members()
            .into_iter()
            .find(|member| member.node_id == from)
            .ok_or(NodeError::MissingMemberForJoinAck)?;
        let join_ack = WireMessage::JoinAck {
            view_epoch: self.store.view_epoch(),
            accepted,
            members: self.store.snapshot_members(),
        };
        self.transport
            .send(from_addr, &join_ack)
            .await
            .map_err(NodeError::Transport)
    }

    fn handle_join_ack(&mut self, accepted: MemberRecord, members: Vec<MemberRecord>, now_ms: u64) {
        let accepted_update = MembershipUpdate {
            node_id: accepted.node_id.clone(),
            addr: accepted.addr,
            incarnation: accepted.incarnation,
            status: accepted.status,
            last_changed_ms: accepted.last_changed_ms,
            origin_node_id: accepted.node_id,
            source_node_id: self.local_node_id.clone(),
        };
        self.apply_and_track(accepted_update, now_ms);

        for member in members {
            let update = MembershipUpdate {
                node_id: member.node_id.clone(),
                addr: member.addr,
                incarnation: member.incarnation,
                status: member.status,
                last_changed_ms: member.last_changed_ms,
                origin_node_id: member.node_id,
                source_node_id: self.local_node_id.clone(),
            };
            self.apply_and_track(update, now_ms);
        }
    }

    async fn handle_sync_digest(
        &self,
        from_addr: SocketAddr,
        digest: Vec<crate::types::MemberDigest>,
    ) -> Result<(), NodeError> {
        let delta = AntiEntropy::diff_for_remote(&self.store, &digest);
        let reply = WireMessage::SyncDelta {
            from: self.local_node_id.clone(),
            view_epoch: self.store.view_epoch(),
            updates: delta,
        };
        self.transport
            .send(from_addr, &reply)
            .await
            .map_err(NodeError::Transport)
    }

    fn handle_sync_delta(&mut self, updates: Vec<MembershipUpdate>, now_ms: u64) {
        for update in updates {
            self.apply_and_track(update, now_ms);
        }
    }

    fn apply_piggyback(&mut self, updates: Vec<MembershipUpdate>, now_ms: u64) {
        for update in updates {
            self.apply_and_track(update, now_ms);
        }
    }

    fn apply_and_track(&mut self, update: MembershipUpdate, now_ms: u64) {
        match self.store.apply_update(update.clone(), now_ms) {
            ApplyResult::Applied { .. } => {
                self.emit_membership_applied(update.clone(), now_ms);
                self.disseminator.enqueue(update);
                self.sync_disseminator_cluster_size();
            }
            ApplyResult::RequiresRefutation(node_id) if node_id == self.local_node_id => {
                let refutation = self.store.mark_local_alive_with_new_incarnation(now_ms);
                self.emit_membership_applied(refutation.clone(), now_ms);
                self.disseminator.enqueue(refutation);
                self.sync_disseminator_cluster_size();
            }
            ApplyResult::IgnoredStale | ApplyResult::RequiresRefutation(_) => {}
        }
    }

    fn member_addr(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.store
            .snapshot_members()
            .into_iter()
            .find_map(|member| (member.node_id == *node_id).then_some(member.addr))
    }

    fn emit_membership_applied(&self, update: MembershipUpdate, now_ms: u64) {
        if let Some(observer) = &self.observer {
            let _ = observer.send(NodeEvent::MembershipApplied {
                observer: self.local_node_id.clone(),
                update,
                view_epoch: self.store.view_epoch(),
                alive_members: self.store.snapshot_alive(),
                at_ms: now_ms,
            });
        }
    }

    fn sync_disseminator_cluster_size(&mut self) {
        let alive_count = self.store.snapshot_alive().len().max(1);
        self.disseminator.set_cluster_size(alive_count);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("transport error: {0}")]
    Transport(TransportError),
    #[error("join ack could not find accepted member")]
    MissingMemberForJoinAck,
}
