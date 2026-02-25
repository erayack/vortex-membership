use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Instant;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

use crate::config::{FaultAction, ScenarioConfig, ScheduledFault};
use crate::dissemination::Disseminator;
use crate::failure_detector::FailureDetector;
use crate::node::{NodeError, NodeEvent, NodeRuntime};
use crate::ownership::OwnershipResolver;
use crate::report::{ReportError, RunArtifact, RunReport, percentile, write_run_artifact};
use crate::state::MembershipStore;
use crate::transport::{TransportError, UdpTransport, netem};
use crate::types::{MemberStatus, MembershipUpdate, NodeId};

/// Runs a complete simulation scenario and writes the resulting artifact report.
///
/// # Errors
///
/// Returns [`HarnessError`] when the scenario is invalid, transport/node tasks fail,
/// or artifact generation/writing fails.
pub async fn run_scenario(cfg: ScenarioConfig) -> Result<RunReport, HarnessError> {
    let mut faults = cfg.harness.events.clone();
    faults.sort_by_key(|fault| fault.at_ms);

    let addresses = build_addresses(cfg.harness.base_port, cfg.harness.node_count)?;
    let node_ids: Vec<_> = (0..cfg.harness.node_count)
        .map(|idx| NodeId::from(format!("node-{idx}")))
        .collect();

    let observer_node = node_ids
        .first()
        .cloned()
        .ok_or_else(|| HarnessError::InvalidScenario("no nodes configured".to_owned()))?;
    let keys = generate_keys(cfg.harness.key_count, cfg.harness.random_seed);

    netem().clear();

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut handles = spawn_nodes(&cfg, &node_ids, &addresses, &event_tx, &shutdown_rx).await?;
    drop(event_tx);

    let mut metrics = MetricsCollector::new(
        node_ids.clone(),
        observer_node,
        keys,
        cfg.harness.runtime_ms,
    );

    run_event_loop(
        &cfg,
        &faults,
        &addresses,
        &node_ids,
        &mut handles,
        &mut metrics,
        &mut event_rx,
    )
    .await?;

    let _ = shutdown_tx.send(true);
    for handle in handles.into_iter().flatten() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(HarnessError::Node(error)),
            Err(join_error) if join_error.is_cancelled() => {}
            Err(join_error) => {
                return Err(HarnessError::Join {
                    message: join_error.to_string(),
                });
            }
        }
    }

    while let Ok(event) = event_rx.try_recv() {
        metrics.observe_node_event(event);
    }

    let artifact = metrics.finish(cfg.clone());
    let artifact_path = artifact.artifact_path_or_default(cfg.harness.artifact_path.clone());
    write_run_artifact(&artifact_path, &artifact)?;
    Ok(artifact.report)
}

async fn spawn_nodes(
    cfg: &ScenarioConfig,
    node_ids: &[NodeId],
    addresses: &[SocketAddr],
    event_tx: &mpsc::UnboundedSender<NodeEvent>,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<Vec<Option<JoinHandle<Result<(), NodeError>>>>, HarnessError> {
    let mut handles = Vec::with_capacity(cfg.harness.node_count);
    for idx in 0..cfg.harness.node_count {
        let local_id = node_ids[idx].clone();
        let local_addr = addresses[idx];
        let transport = UdpTransport::bind(local_addr).await?;
        let store = initialize_store(&local_id, idx, node_ids, addresses, cfg.swim.quarantine_ms);
        let runtime = NodeRuntime::new(
            local_id.clone(),
            cfg.swim.anti_entropy_interval_ms,
            transport,
            store,
            FailureDetector::new(
                local_id,
                cfg.swim.probe_interval_ms,
                cfg.swim.ack_timeout_ms,
                cfg.swim.indirect_ping_count,
                cfg.swim.suspect_timeout_ms,
            ),
            Disseminator::new(cfg.harness.node_count),
            OwnershipResolver::default(),
        )
        .with_observer(event_tx.clone());

        handles.push(Some(tokio::spawn(
            runtime.run_until_shutdown(shutdown_rx.clone()),
        )));
    }
    Ok(handles)
}

fn initialize_store(
    local_id: &NodeId,
    local_idx: usize,
    node_ids: &[NodeId],
    addresses: &[SocketAddr],
    quarantine_ms: u64,
) -> MembershipStore {
    let mut store = MembershipStore::new(local_id.clone(), addresses[local_idx], quarantine_ms);
    for (peer_idx, peer_id) in node_ids.iter().enumerate() {
        if peer_idx == local_idx {
            continue;
        }

        let _ = store.apply_update(
            MembershipUpdate {
                node_id: peer_id.clone(),
                addr: addresses[peer_idx],
                incarnation: 0,
                status: MemberStatus::Alive,
                last_changed_ms: 0,
                origin_node_id: peer_id.clone(),
                source_node_id: local_id.clone(),
            },
            0,
        );
    }
    store
}

async fn run_event_loop(
    cfg: &ScenarioConfig,
    faults: &[ScheduledFault],
    addresses: &[SocketAddr],
    node_ids: &[NodeId],
    handles: &mut [Option<JoinHandle<Result<(), NodeError>>>],
    metrics: &mut MetricsCollector,
    event_rx: &mut mpsc::UnboundedReceiver<NodeEvent>,
) -> Result<(), HarnessError> {
    let start = Instant::now();
    let mut next_fault_idx = 0;
    let mut tick = time::interval(Duration::from_millis(20));

    loop {
        let elapsed_ms = elapsed_ms(start);
        while next_fault_idx < faults.len() && faults[next_fault_idx].at_ms <= elapsed_ms {
            apply_fault(
                &faults[next_fault_idx],
                elapsed_ms,
                addresses,
                node_ids,
                handles,
                metrics,
            )?;
            next_fault_idx += 1;
        }

        while let Ok(event) = event_rx.try_recv() {
            metrics.observe_node_event(event);
        }

        if elapsed_ms >= cfg.harness.runtime_ms {
            break;
        }
        tick.tick().await;
    }
    Ok(())
}

fn apply_fault(
    fault: &ScheduledFault,
    now_ms: u64,
    addresses: &[SocketAddr],
    node_ids: &[NodeId],
    handles: &mut [Option<JoinHandle<Result<(), NodeError>>>],
    metrics: &mut MetricsCollector,
) -> Result<(), HarnessError> {
    match &fault.action {
        FaultAction::Kill { node } => {
            let Some(handle) = handles.get_mut(*node) else {
                return Err(HarnessError::InvalidScenario(
                    "kill references unknown node".to_owned(),
                ));
            };
            if let Some(handle) = handle.take() {
                handle.abort();
                metrics.note_kill(node_ids[*node].clone(), now_ms);
            }
        }
        FaultAction::Delay { from, to, delay_ms } => {
            let Some(from_addr) = addresses.get(*from) else {
                return Err(HarnessError::InvalidScenario(
                    "delay references unknown node".to_owned(),
                ));
            };
            let Some(to_addr) = addresses.get(*to) else {
                return Err(HarnessError::InvalidScenario(
                    "delay references unknown node".to_owned(),
                ));
            };
            netem().set_delay(*from_addr, *to_addr, *delay_ms);
        }
        FaultAction::Loss {
            from,
            to,
            loss_rate,
        } => {
            let Some(from_addr) = addresses.get(*from) else {
                return Err(HarnessError::InvalidScenario(
                    "loss references unknown node".to_owned(),
                ));
            };
            let Some(to_addr) = addresses.get(*to) else {
                return Err(HarnessError::InvalidScenario(
                    "loss references unknown node".to_owned(),
                ));
            };
            netem().set_loss(*from_addr, *to_addr, *loss_rate);
        }
        FaultAction::Partition { left, right } => {
            let left_set = addresses_for_nodes(left, addresses)?;
            let right_set = addresses_for_nodes(right, addresses)?;
            netem().add_partition(left_set, right_set);
        }
        FaultAction::Heal => {
            netem().clear_partitions();
            metrics.note_heal(now_ms);
        }
    }

    Ok(())
}

fn build_addresses(base_port: u16, node_count: usize) -> Result<Vec<SocketAddr>, HarnessError> {
    let mut addresses = Vec::with_capacity(node_count);
    for idx in 0..node_count {
        let offset = u16::try_from(idx)
            .map_err(|_| HarnessError::InvalidScenario("node index overflowed u16".to_owned()))?;
        let port = base_port.checked_add(offset).ok_or_else(|| {
            HarnessError::InvalidScenario("port overflow while building node addresses".to_owned())
        })?;
        addresses.push(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)));
    }
    Ok(addresses)
}

fn addresses_for_nodes(
    indexes: &[usize],
    addresses: &[SocketAddr],
) -> Result<BTreeSet<SocketAddr>, HarnessError> {
    let mut set = BTreeSet::new();
    for idx in indexes {
        let Some(addr) = addresses.get(*idx) else {
            return Err(HarnessError::InvalidScenario(
                "partition references unknown node".to_owned(),
            ));
        };
        set.insert(*addr);
    }
    Ok(set)
}

fn elapsed_ms(start: Instant) -> u64 {
    let millis = start.elapsed().as_millis().min(u128::from(u64::MAX));
    u64::try_from(millis).map_or(u64::MAX, |value| value)
}

fn generate_keys(count: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut state = seed;
    (0..count)
        .map(|idx| {
            state = splitmix64(state.wrapping_add(idx as u64).wrapping_add(1));
            state.to_le_bytes().to_vec()
        })
        .collect()
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn u64_to_f64_lossless(value: u64) -> f64 {
    let hi = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let lo = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(hi) * 4_294_967_296.0 + f64::from(lo)
}

struct MetricsCollector {
    detection_samples_ms: Vec<u64>,
    detected_kills: HashSet<NodeId>,
    kill_started_ms: HashMap<NodeId, u64>,
    false_suspicions: u64,
    suspicion_events_total: u64,
    active_nodes: HashSet<NodeId>,
    alive_views: HashMap<NodeId, BTreeSet<NodeId>>,
    heal_started_ms: Option<u64>,
    convergence_ms: Option<u64>,
    observer_node: NodeId,
    keyspace: Vec<Vec<u8>>,
    ownership_resolver: OwnershipResolver,
    previous_owners: Option<Vec<Option<NodeId>>>,
    owner_reassignments: u64,
    duration_ms: u64,
}

impl MetricsCollector {
    fn new(
        all_nodes: Vec<NodeId>,
        observer_node: NodeId,
        keyspace: Vec<Vec<u8>>,
        duration_ms: u64,
    ) -> Self {
        Self {
            detection_samples_ms: Vec::new(),
            detected_kills: HashSet::new(),
            kill_started_ms: HashMap::new(),
            false_suspicions: 0,
            suspicion_events_total: 0,
            active_nodes: all_nodes.into_iter().collect(),
            alive_views: HashMap::new(),
            heal_started_ms: None,
            convergence_ms: None,
            observer_node,
            keyspace,
            ownership_resolver: OwnershipResolver::default(),
            previous_owners: None,
            owner_reassignments: 0,
            duration_ms,
        }
    }

    fn note_kill(&mut self, node_id: NodeId, at_ms: u64) {
        self.active_nodes.remove(&node_id);
        self.kill_started_ms.insert(node_id, at_ms);
    }

    const fn note_heal(&mut self, at_ms: u64) {
        self.heal_started_ms = Some(at_ms);
        self.convergence_ms = None;
    }

    fn observe_node_event(&mut self, event: NodeEvent) {
        match event {
            NodeEvent::DetectorMarked {
                target,
                status,
                at_ms,
                ..
            } => {
                if !matches!(status, MemberStatus::Suspect | MemberStatus::Dead) {
                    return;
                }

                self.suspicion_events_total = self.suspicion_events_total.saturating_add(1);
                if let Some(started_ms) = self.kill_started_ms.get(&target) {
                    if self.detected_kills.insert(target) {
                        self.detection_samples_ms
                            .push(at_ms.saturating_sub(*started_ms));
                    }
                } else {
                    self.false_suspicions = self.false_suspicions.saturating_add(1);
                }
            }
            NodeEvent::MembershipApplied {
                observer,
                alive_members,
                at_ms,
                ..
            } => {
                let alive_nodes = alive_members
                    .iter()
                    .map(|member| member.node_id.clone())
                    .collect::<BTreeSet<_>>();
                self.alive_views.insert(observer.clone(), alive_nodes);

                if observer == self.observer_node {
                    self.observe_ownership(&alive_members);
                }

                self.maybe_mark_converged(at_ms);
            }
        }
    }

    fn observe_ownership(&mut self, alive_members: &[crate::types::MemberRecord]) {
        let current_owners = self
            .keyspace
            .iter()
            .map(|key| self.ownership_resolver.owner(key, alive_members))
            .collect::<Vec<_>>();

        if let Some(previous) = &self.previous_owners {
            for (old, new) in previous.iter().zip(current_owners.iter()) {
                if old != new {
                    self.owner_reassignments = self.owner_reassignments.saturating_add(1);
                }
            }
        }

        self.previous_owners = Some(current_owners);
    }

    fn maybe_mark_converged(&mut self, at_ms: u64) {
        let Some(heal_started_ms) = self.heal_started_ms else {
            return;
        };
        if self.convergence_ms.is_some() {
            return;
        }

        let mut iter = self
            .active_nodes
            .iter()
            .filter_map(|node_id| self.alive_views.get(node_id));
        let Some(first_view) = iter.next() else {
            return;
        };
        let node_views_seen = self
            .active_nodes
            .iter()
            .filter(|node_id| self.alive_views.contains_key(*node_id))
            .count();
        if node_views_seen != self.active_nodes.len() {
            return;
        }

        if iter.all(|view| view == first_view) {
            self.convergence_ms = Some(at_ms.saturating_sub(heal_started_ms));
        }
    }

    fn finish(self, scenario: ScenarioConfig) -> RunArtifact {
        let duration_minutes = if self.duration_ms == 0 {
            0.0
        } else {
            u64_to_f64_lossless(self.duration_ms) / 60_000.0
        };
        let owner_churn_per_min = if duration_minutes > 0.0 {
            u64_to_f64_lossless(self.owner_reassignments) / duration_minutes
        } else {
            0.0
        };
        let false_suspicion_rate = if self.suspicion_events_total == 0 {
            0.0
        } else {
            u64_to_f64_lossless(self.false_suspicions)
                / u64_to_f64_lossless(self.suspicion_events_total)
        };

        RunArtifact {
            scenario,
            report: RunReport {
                detection_p50_ms: percentile(&self.detection_samples_ms, 0.5),
                detection_p95_ms: percentile(&self.detection_samples_ms, 0.95),
                false_suspicions: self.false_suspicions,
                convergence_ms: self.convergence_ms.unwrap_or(0),
                owner_churn_per_min,
            },
            false_suspicion_rate,
            detection_samples_ms: self.detection_samples_ms,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("scenario is invalid: {0}")]
    InvalidScenario(String),
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    #[error("node runtime error: {0}")]
    Node(#[from] NodeError),
    #[error("report error: {0}")]
    Report(#[from] ReportError),
    #[error("task join error: {message}")]
    Join { message: String },
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::panic::{self, AssertUnwindSafe};

    use super::{HarnessError, MetricsCollector, apply_fault};
    use crate::config::{FaultAction, ScenarioConfig, ScheduledFault};
    use crate::node::NodeEvent;
    use crate::types::{MemberRecord, MemberStatus, MembershipUpdate, NodeId};

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn test_metrics() -> MetricsCollector {
        let nodes = vec![NodeId::from("node-0"), NodeId::from("node-1")];
        MetricsCollector::new(nodes, NodeId::from("node-0"), vec![b"k".to_vec()], 1_000)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn apply_fault_without_panic(
        fault: ScheduledFault,
    ) -> std::thread::Result<Result<(), HarnessError>> {
        panic::catch_unwind(AssertUnwindSafe(|| {
            let addresses = vec![localhost(15_000), localhost(15_001)];
            let node_ids = vec![NodeId::from("node-0"), NodeId::from("node-1")];
            let mut handles = Vec::new();
            let mut metrics = test_metrics();

            apply_fault(
                &fault,
                10,
                &addresses,
                &node_ids,
                &mut handles,
                &mut metrics,
            )
        }))
    }

    fn member(node_id: &str, port: u16) -> MemberRecord {
        MemberRecord {
            node_id: NodeId::from(node_id),
            addr: localhost(port),
            incarnation: 0,
            status: MemberStatus::Alive,
            last_changed_ms: 0,
        }
    }

    fn membership_applied(
        observer: &str,
        alive_members: Vec<MemberRecord>,
        at_ms: u64,
    ) -> NodeEvent {
        let observer_id = NodeId::from(observer);
        NodeEvent::MembershipApplied {
            observer: observer_id.clone(),
            update: MembershipUpdate {
                node_id: observer_id.clone(),
                addr: localhost(16_000),
                incarnation: 0,
                status: MemberStatus::Alive,
                last_changed_ms: at_ms,
                origin_node_id: observer_id.clone(),
                source_node_id: observer_id,
            },
            view_epoch: 0,
            alive_members,
            at_ms,
        }
    }

    #[test]
    fn apply_fault_delay_with_unknown_node_returns_error() {
        let fault = ScheduledFault {
            at_ms: 10,
            action: FaultAction::Delay {
                from: 2,
                to: 0,
                delay_ms: 5,
            },
        };
        let result = apply_fault_without_panic(fault);

        assert!(result.is_ok());
        let Ok(result) = result else {
            unreachable!("asserted above");
        };

        assert!(matches!(
            result,
            Err(HarnessError::InvalidScenario(message)) if message == "delay references unknown node"
        ));
    }

    #[test]
    fn apply_fault_loss_with_unknown_node_returns_error() {
        let fault = ScheduledFault {
            at_ms: 10,
            action: FaultAction::Loss {
                from: 0,
                to: 2,
                loss_rate: 0.5,
            },
        };
        let result = apply_fault_without_panic(fault);

        assert!(result.is_ok());
        let Ok(result) = result else {
            unreachable!("asserted above");
        };

        assert!(matches!(
            result,
            Err(HarnessError::InvalidScenario(message)) if message == "loss references unknown node"
        ));
    }

    #[test]
    fn apply_fault_kill_with_unknown_node_returns_error_without_panicking() {
        let fault = ScheduledFault {
            at_ms: 10,
            action: FaultAction::Kill { node: 2 },
        };
        let result = apply_fault_without_panic(fault);

        assert!(result.is_ok());
        let Ok(result) = result else {
            unreachable!("asserted above");
        };
        assert!(matches!(
            result,
            Err(HarnessError::InvalidScenario(message)) if message == "kill references unknown node"
        ));
    }

    #[test]
    fn apply_fault_partition_with_unknown_node_returns_error_without_panicking() {
        let fault = ScheduledFault {
            at_ms: 10,
            action: FaultAction::Partition {
                left: vec![0],
                right: vec![2],
            },
        };
        let result = apply_fault_without_panic(fault);

        assert!(result.is_ok());
        let Ok(result) = result else {
            unreachable!("asserted above");
        };
        assert!(matches!(
            result,
            Err(HarnessError::InvalidScenario(message)) if message == "partition references unknown node"
        ));
    }

    #[test]
    fn metrics_record_detection_and_convergence_for_tiny_deterministic_scenario() {
        let nodes = vec![
            NodeId::from("node-0"),
            NodeId::from("node-1"),
            NodeId::from("node-2"),
        ];
        let mut metrics =
            MetricsCollector::new(nodes, NodeId::from("node-0"), vec![b"k".to_vec()], 1_000);

        metrics.note_kill(NodeId::from("node-2"), 100);
        metrics.observe_node_event(NodeEvent::DetectorMarked {
            emitter: NodeId::from("node-0"),
            target: NodeId::from("node-2"),
            status: MemberStatus::Dead,
            at_ms: 130,
        });

        metrics.note_heal(200);
        let alive = vec![member("node-0", 15_000), member("node-1", 15_001)];
        metrics.observe_node_event(membership_applied("node-0", alive.clone(), 250));
        metrics.observe_node_event(membership_applied("node-1", alive, 260));

        let report = metrics.finish(ScenarioConfig::default()).report;
        assert_eq!(report.detection_p50_ms, 30);
        assert_eq!(report.detection_p95_ms, 30);
        assert_eq!(report.convergence_ms, 60);
    }
}
