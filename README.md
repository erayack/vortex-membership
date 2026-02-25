# vortex-membership

A decentralized cluster membership engine in Rust. Implements the SWIM protocol with a suspect-timeout orchestration and rendezvous hashing for deterministic, coordination-free ownership of distributed edge state.

`vortex-membership` solves two foundational distributed systems questions at the edge: *Who is alive?* and *Who owns this key?* It provides eventually consistent membership and deterministic ownership without the latency tax of Raft or Paxos.

## Key Features

*   **SWIM Protocol:** Decentralized gossip with an adaptive `K × log₁₀(N)` retransmission budget and **1200-byte MTU safety**.
*   **Deterministic Ownership:** Built-in **Rendezvous Hashing** over the membership view for coordination-free `owner(key)` calculation.
*   **Stability Guard:** A strict **Quarantine Policy** that excludes "flapping" nodes from ownership eligibility to prevent re-balancing storms.
*   **Self-Healing:** Incarnation-based **Refutation logic** that allows healthy nodes to immediately clear false "Suspect" rumors.
*   **Chaos Lab:** A built-in simulation harness to measure failure detection latency and ownership churn under deterministic fault injection.

## Architecture

Built with **Zero-Unsafe Rust** and a strictly decoupled event-loop architecture:

- **`failure_detector.rs`**: Indirect probing (Ping/PingReq) to eliminate false positives.
- **`state.rs`**: Monotonic state machine with incarnation-conflict resolution.
- **`ownership.rs`**: Order-independent Rendezvous scoring for stable key-to-node mapping.
- **`transport.rs`**: Non-blocking UDP transport with `bincode` serialization.

## The Lab (Simulation)

Run a deterministic chaos scenario to measure convergence:

```bash
cargo build --release
./target/release/vortex-membership lab --scenario scenario.toml --out report.json
```

**Example Report:**
```json
{
  "detection_p95_ms": 580,
  "false_suspicions": 0,
  "convergence_ms": 1240,
  "owner_churn_per_min": 4.2
}
```

## Quality & Performance

*   **`unsafe_code = "forbid"`**
*   **Strict Linting:** `deny(warnings)`, `deny(clippy::unwrap_used)`, `deny(clippy::perf)`.
*   **Optimized Release:** `lto = "thin"`, `panic = "abort"`, `codegen-units = 1`.

---
*Part of the **Edge Consistency Suite** along with [chronos-edge](https://github.com/erayack/chronos-edge) and [edge-consistency](https://github.com/erayack/edge-consistency).*