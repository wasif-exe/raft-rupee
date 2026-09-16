# Raft-₹ (Raft-Rupee)

> A from-scratch Raft consensus engine in Rust with a deterministic simulation testing harness that can reproduce any network partition, disk fault, message reordering, or clock skew from a single u64 seed.


---

## Terminal Proof Dashboard

![Raft-₹ Dashboard](./assets/proof_dashboard.png)

---

## What is this?

Raft-₹ is a complete implementation of the Raft Consensus Algorithm (Ongaro, Stanford PhD 2014) written in pure Rust with zero external consensus crates. It is designed with three architectural principles:

1. **Pure I/O-Free Core**: The consensus kernel (`raft-core`) contains zero threads, zero async runtimes, zero network sockets, and zero syscalls.
2. **Deterministic Simulation Testing**: A FoundationDB / TigerBeetle-inspired chaos harness (`raft-sim`) that drives the kernel from a single `u64` PRNG seed and injects network partitions, packet drops, disk faults, and node crashes.
3. **Production Durability**: A real multi-process server (`raft-server`) that uses Linux `O_DIRECT` + `O_DSYNC` for sector-aligned WAL writes bypassing the kernel page cache.

## Benchmark Numbers (Single Core, x86_64 Linux)

| Metric | Value |
| :--- | :--- |
| Simulation Throughput | ~397,000 ticks/sec |
| Chaos Verification | 100 seeds × 1,000 ticks × 5 nodes in 0.11s |
| Hardware WAL | 4096-byte O_DIRECT sector alignment + CRC32 |

## Features Implemented

- **Leader Election (§5.2)** with randomized election timeouts
- **Log Replication (§5.3)** with fast-backtracking conflict resolution
- **Strict Figure 8 Commit Guard (§5.4.2)** forbidding indirect previous-term commits
- **Persistent Hard State** with CRC32 corruption detection
- **Snapshotting & Log Compaction (§7)** with InstallSnapshot RPC fallback
- **Pre-Vote Protocol (§9.6)** preventing disruptive partitioned servers
- **Client Redirection (§6)** with automatic leader hinting

## Safety Invariants Verified

Every simulator tick evaluates:

- **Election Safety (§5.2)**: At most one leader per term
- **Log Matching (§5.3)**: Pairwise log consistency across all nodes
- **Leader Append-Only (§5.3)**: Immutable committed history
- **State Machine Safety (§5.4.3)**: Identical execution order across replicas

## Workspace Structure

```text
raft-rupee/
├── raft-core/   # Pure consensus state machine (no I/O)
├── raft-sim/    # Deterministic chaos simulator + invariants
├── raft-server/ # Production TCP + O_DIRECT WAL binary
└── raft-client/ # CLI client with automatic leader redirection

```

## Quick Start

### Run the Deterministic Chaos Simulator

```bash
cargo test -p raft-sim --release --test simulation_tests -- --nocapture

```

### Run the Criterion Benchmarks

```bash
cargo bench -p raft-sim --bench sim_benchmark

```

### Spin Up a Local 3-Node Physical Cluster

```bash
./run_local_cluster.sh

```

### Verify Full Proof Suite (Simulator + Benchmark + Live Cluster)

```bash
./generate_proof.sh

```

## References

* Ongaro & Ousterhout — [In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf)
* Ongaro — [Consensus: Bridging Theory and Practice (Stanford PhD Dissertation)](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
* Will Wilson — [Testing Distributed Systems w/ Deterministic Simulation (FoundationDB, Strange Loop 2014)](https://www.youtube.com/watch?v=4fFDFbi3toc)
* [TigerBeetle VOPR Design Documentation](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DESIGN.md)


