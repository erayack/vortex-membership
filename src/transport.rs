use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};

use bincode::config;
use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep};

use crate::protocol::WireMessage;

pub const MAX_PACKET_SIZE: usize = 1_200;
const MAX_UDP_DATAGRAM_SIZE: usize = 65_507;
type Link = (SocketAddr, SocketAddr);

#[derive(Clone, Debug, Default)]
pub struct NetworkEmulator {
    rules: std::sync::Arc<Mutex<NetworkRules>>,
}

#[derive(Clone, Debug, Default)]
struct NetworkRules {
    delay_by_link_ms: HashMap<Link, u64>,
    loss_by_link: HashMap<Link, f64>,
    partitions: Vec<(BTreeSet<SocketAddr>, BTreeSet<SocketAddr>)>,
    packet_counter: u64,
}

impl NetworkEmulator {
    pub fn clear(&self) {
        if let Ok(mut rules) = self.rules.lock() {
            rules.delay_by_link_ms.clear();
            rules.loss_by_link.clear();
            rules.partitions.clear();
            rules.packet_counter = 0;
        }
    }

    pub fn set_delay(&self, from: SocketAddr, to: SocketAddr, delay_ms: u64) {
        if let Ok(mut rules) = self.rules.lock() {
            if delay_ms == 0 {
                rules.delay_by_link_ms.remove(&(from, to));
            } else {
                rules.delay_by_link_ms.insert((from, to), delay_ms);
            }
        }
    }

    pub fn set_loss(&self, from: SocketAddr, to: SocketAddr, loss_rate: f64) {
        if let Ok(mut rules) = self.rules.lock() {
            if loss_rate <= 0.0 {
                rules.loss_by_link.remove(&(from, to));
            } else {
                rules
                    .loss_by_link
                    .insert((from, to), loss_rate.clamp(0.0, 1.0));
            }
        }
    }

    pub fn add_partition(&self, left: BTreeSet<SocketAddr>, right: BTreeSet<SocketAddr>) {
        if let Ok(mut rules) = self.rules.lock() {
            rules.partitions.push((left, right));
        }
    }

    pub fn clear_partitions(&self) {
        if let Ok(mut rules) = self.rules.lock() {
            rules.partitions.clear();
        }
    }

    fn plan_send(&self, from: SocketAddr, to: SocketAddr) -> SendPlan {
        let Ok(mut rules) = self.rules.lock() else {
            return SendPlan {
                drop_packet: false,
                delay_ms: 0,
            };
        };

        let is_partitioned = rules.partitions.iter().any(|(left, right)| {
            (left.contains(&from) && right.contains(&to))
                || (left.contains(&to) && right.contains(&from))
        });
        if is_partitioned {
            return SendPlan {
                drop_packet: true,
                delay_ms: 0,
            };
        }

        let delay_ms = rules
            .delay_by_link_ms
            .get(&(from, to))
            .copied()
            .unwrap_or(0);
        let loss_rate = rules.loss_by_link.get(&(from, to)).copied().unwrap_or(0.0);

        rules.packet_counter = rules.packet_counter.saturating_add(1);
        let random = unit_interval(splitmix64(rules.packet_counter));
        let drop_packet = random < loss_rate;

        SendPlan {
            drop_packet,
            delay_ms,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SendPlan {
    drop_packet: bool,
    delay_ms: u64,
}

#[must_use]
pub fn netem() -> &'static NetworkEmulator {
    static NETEM: OnceLock<NetworkEmulator> = OnceLock::new();
    NETEM.get_or_init(NetworkEmulator::default)
}

pub struct UdpTransport {
    socket: UdpSocket,
}

impl UdpTransport {
    /// Binds a UDP transport socket to the provided local address.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Bind`] when the socket cannot be bound.
    pub async fn bind(addr: SocketAddr) -> Result<Self, TransportError> {
        let socket = UdpSocket::bind(addr)
            .await
            .map_err(|source| TransportError::Bind { addr, source })?;
        Ok(Self { socket })
    }

    /// Returns the local bound address of this transport socket.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::LocalAddr`] if the runtime cannot retrieve it.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.socket
            .local_addr()
            .map_err(|source| TransportError::LocalAddr { source })
    }

    /// Serializes and sends a wire message to `to`.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`TransportError::Serialize`] on encode failure
    /// - [`TransportError::PacketTooLarge`] when encoded bytes exceed 1200
    /// - [`TransportError::SendTo`] on UDP send failure
    /// - [`TransportError::ShortSend`] if the socket reports partial send
    pub async fn send(&self, to: SocketAddr, msg: &WireMessage) -> Result<(), TransportError> {
        let from = self.local_addr()?;
        let send_plan = netem().plan_send(from, to);
        if send_plan.drop_packet {
            return Ok(());
        }
        if send_plan.delay_ms > 0 {
            sleep(Duration::from_millis(send_plan.delay_ms)).await;
        }

        let payload = bincode::serde::encode_to_vec(msg, config::standard())
            .map_err(|source| TransportError::Serialize { source })?;

        if payload.len() > MAX_PACKET_SIZE {
            return Err(TransportError::PacketTooLarge {
                size: payload.len(),
                max: MAX_PACKET_SIZE,
            });
        }

        let sent = self
            .socket
            .send_to(&payload, to)
            .await
            .map_err(|source| TransportError::SendTo { to, source })?;

        if sent != payload.len() {
            return Err(TransportError::ShortSend {
                expected: payload.len(),
                sent,
            });
        }

        Ok(())
    }

    /// Receives and decodes a wire message from the UDP socket.
    ///
    /// # Errors
    ///
    /// Returns:
    /// - [`TransportError::Recv`] on socket receive failure
    /// - [`TransportError::PacketTooLarge`] when datagram exceeds 1200 bytes
    /// - [`TransportError::Decode`] on deserialization failure
    pub async fn recv(&self) -> Result<(SocketAddr, WireMessage), TransportError> {
        let mut buf = vec![0_u8; MAX_UDP_DATAGRAM_SIZE];
        let (len, from) = self
            .socket
            .recv_from(&mut buf)
            .await
            .map_err(|source| TransportError::Recv { source })?;

        if len > MAX_PACKET_SIZE {
            return Err(TransportError::PacketTooLarge {
                size: len,
                max: MAX_PACKET_SIZE,
            });
        }

        let (msg, _): (WireMessage, usize) =
            bincode::serde::decode_from_slice(&buf[..len], config::standard())
                .map_err(|source| TransportError::Decode { source })?;

        Ok((from, msg))
    }
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

fn unit_interval(value: u64) -> f64 {
    let numerator = u64_to_f64_lossless(value);
    let denominator = u64_to_f64_lossless(u64::MAX);
    numerator / denominator
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("failed to bind UDP socket at {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[error("failed to resolve local UDP socket address: {source}")]
    LocalAddr { source: std::io::Error },
    #[error("failed to serialize wire message: {source}")]
    Serialize { source: bincode::error::EncodeError },
    #[error("failed to decode wire message: {source}")]
    Decode { source: bincode::error::DecodeError },
    #[error("packet size {size} exceeds max allowed {max}")]
    PacketTooLarge { size: usize, max: usize },
    #[error("failed to send UDP packet to {to}: {source}")]
    SendTo {
        to: SocketAddr,
        source: std::io::Error,
    },
    #[error("failed to receive UDP packet: {source}")]
    Recv { source: std::io::Error },
    #[error("UDP short send: expected {expected} bytes, sent {sent}")]
    ShortSend { expected: usize, sent: usize },
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::{error::Error, fmt, io::ErrorKind};

    use tokio::net::UdpSocket;

    use super::{MAX_PACKET_SIZE, TransportError, UdpTransport};
    use crate::protocol::WireMessage;
    use crate::types::{MemberStatus, MembershipUpdate, NodeId};

    fn localhost(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    #[derive(Debug)]
    struct TestError(String);

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl Error for TestError {}

    fn boxed_err(message: impl Into<String>) -> Box<dyn Error> {
        Box::new(TestError(message.into()))
    }

    async fn bind_transport_optional() -> Result<Option<UdpTransport>, Box<dyn Error>> {
        match UdpTransport::bind(localhost(0)).await {
            Ok(transport) => Ok(Some(transport)),
            Err(TransportError::Bind { source, .. })
                if source.kind() == ErrorKind::PermissionDenied =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn bind_socket_optional() -> Result<Option<UdpSocket>, Box<dyn Error>> {
        match UdpSocket::bind(localhost(0)).await {
            Ok(socket) => Ok(Some(socket)),
            Err(error) if error.kind() == ErrorKind::PermissionDenied => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    #[tokio::test]
    async fn send_and_recv_round_trip_ping() -> Result<(), Box<dyn Error>> {
        let Some(sender) = bind_transport_optional().await? else {
            return Ok(());
        };
        let Some(receiver) = bind_transport_optional().await? else {
            return Ok(());
        };

        let message = WireMessage::ping(7, NodeId::from("node-a"), Vec::new());
        let receiver_addr = receiver.local_addr()?;
        sender.send(receiver_addr, &message).await?;

        let (_, decoded) = receiver.recv().await?;
        assert_eq!(decoded, message);
        Ok(())
    }

    #[tokio::test]
    async fn send_and_recv_round_trip_all_wire_variants() -> Result<(), Box<dyn Error>> {
        let Some(sender) = bind_transport_optional().await? else {
            return Ok(());
        };
        let Some(receiver) = bind_transport_optional().await? else {
            return Ok(());
        };

        let accepted = crate::types::MemberRecord {
            node_id: NodeId::from("node-a"),
            addr: localhost(7100),
            incarnation: 2,
            status: MemberStatus::Alive,
            last_changed_ms: 100,
        };
        let members = vec![
            accepted.clone(),
            crate::types::MemberRecord {
                node_id: NodeId::from("node-b"),
                addr: localhost(7200),
                incarnation: 1,
                status: MemberStatus::Suspect,
                last_changed_ms: 90,
            },
        ];
        let digest = vec![
            crate::types::MemberDigest {
                node_id: NodeId::from("node-a"),
                incarnation: 2,
                status: MemberStatus::Alive,
                last_changed_ms: 100,
            },
            crate::types::MemberDigest {
                node_id: NodeId::from("node-b"),
                incarnation: 1,
                status: MemberStatus::Suspect,
                last_changed_ms: 90,
            },
        ];
        let updates = vec![
            MembershipUpdate {
                node_id: NodeId::from("node-a"),
                addr: localhost(7100),
                incarnation: 2,
                status: MemberStatus::Alive,
                last_changed_ms: 100,
                origin_node_id: NodeId::from("node-a"),
                source_node_id: NodeId::from("node-c"),
            },
            MembershipUpdate {
                node_id: NodeId::from("node-b"),
                addr: localhost(7200),
                incarnation: 1,
                status: MemberStatus::Suspect,
                last_changed_ms: 95,
                origin_node_id: NodeId::from("node-b"),
                source_node_id: NodeId::from("node-c"),
            },
        ];
        let messages = vec![
            WireMessage::ping(1, NodeId::from("node-a"), updates.clone()),
            WireMessage::ack(2, NodeId::from("node-b"), updates.clone()),
            WireMessage::ping_req(
                3,
                NodeId::from("node-c"),
                NodeId::from("node-d"),
                updates.clone(),
            ),
            WireMessage::Join {
                from: NodeId::from("node-e"),
                addr: localhost(7300),
            },
            WireMessage::JoinAck {
                view_epoch: 9,
                accepted,
                members,
            },
            WireMessage::SyncDigest {
                from: NodeId::from("node-f"),
                view_epoch: 10,
                digest,
            },
            WireMessage::SyncDelta {
                from: NodeId::from("node-g"),
                view_epoch: 11,
                updates,
            },
        ];

        let receiver_addr = receiver.local_addr()?;
        for message in messages {
            sender.send(receiver_addr, &message).await?;
            let (_, decoded) = receiver.recv().await?;
            assert_eq!(decoded, message);
        }

        Ok(())
    }

    #[tokio::test]
    async fn send_rejects_oversized_payload() -> Result<(), Box<dyn Error>> {
        let Some(sender) = bind_transport_optional().await? else {
            return Ok(());
        };
        let Some(receiver) = bind_transport_optional().await? else {
            return Ok(());
        };

        let oversized = WireMessage::Ping {
            seq: 1,
            from: NodeId::from("x".repeat(MAX_PACKET_SIZE * 2)),
            piggyback: Vec::new(),
        };

        let receiver_addr = receiver.local_addr()?;
        let result = sender.send(receiver_addr, &oversized).await;
        match result {
            Err(TransportError::PacketTooLarge { .. }) => Ok(()),
            Err(other) => Err(other.into()),
            Ok(()) => Err(boxed_err("oversized payload unexpectedly succeeded")),
        }
    }

    #[tokio::test]
    async fn recv_rejects_malformed_payload() -> Result<(), Box<dyn Error>> {
        let Some(receiver) = bind_transport_optional().await? else {
            return Ok(());
        };
        let Some(raw_sender) = bind_socket_optional().await? else {
            return Ok(());
        };
        let dst = receiver.local_addr()?;

        let garbage = [0xAA_u8, 0xBB, 0xCC, 0xDD];
        let sent = raw_sender.send_to(&garbage, dst).await?;
        assert_eq!(sent, garbage.len());

        let result = receiver.recv().await;
        match result {
            Err(TransportError::Decode { .. }) => Ok(()),
            Err(other) => Err(other.into()),
            Ok((from, _)) => Err(boxed_err(format!(
                "malformed payload unexpectedly decoded from {from}"
            ))),
        }
    }

    #[tokio::test]
    async fn recv_rejects_oversized_datagram() -> Result<(), Box<dyn Error>> {
        let Some(receiver) = bind_transport_optional().await? else {
            return Ok(());
        };
        let Some(raw_sender) = bind_socket_optional().await? else {
            return Ok(());
        };
        let dst = receiver.local_addr()?;

        let update = MembershipUpdate {
            node_id: NodeId::from("node-a"),
            addr: localhost(7000),
            incarnation: 1,
            status: MemberStatus::Alive,
            last_changed_ms: 1,
            origin_node_id: NodeId::from("node-a"),
            source_node_id: NodeId::from("node-a"),
        };
        let oversized = WireMessage::Ping {
            seq: 1,
            from: NodeId::from("node-b"),
            piggyback: vec![update; 80],
        };
        let payload = bincode::serde::encode_to_vec(&oversized, bincode::config::standard())?;
        assert!(payload.len() > MAX_PACKET_SIZE);

        raw_sender.send_to(&payload, dst).await?;

        let result = receiver.recv().await;
        match result {
            Err(TransportError::PacketTooLarge { .. }) => Ok(()),
            Err(other) => Err(other.into()),
            Ok((from, _)) => Err(boxed_err(format!(
                "oversized datagram unexpectedly decoded from {from}"
            ))),
        }
    }
}
