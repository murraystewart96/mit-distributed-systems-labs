# MIT 6.5840 Distributed Systems

My implementations of the labs from MIT's [6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/) course.

All code I wrote is marked with `// ***** MY CODE START *****` `// ***** MY CODE END *****` comments to distinguish it from the provided framework.

---

## Lab 1 — MapReduce

> [Lab spec](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

A distributed MapReduce system modelled after the [Google MapReduce paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf). A coordinator process hands out map and reduce tasks to worker processes over RPC, detects crashed workers and reassigns their tasks.

**Implementation**

| File | Description |
|---|---|
| [src/mr/coordinator.go](src/mr/coordinator.go) | Task scheduling, worker failure detection via timeouts |
| [src/mr/worker.go](src/mr/worker.go) | Map and reduce task execution, intermediate file handling |
| [src/mr/task.go](src/mr/task.go) | Task state types shared between coordinator and worker |

**Run the tests**

```bash
cd src/main
bash test-mr.sh
```

The suite covers: correct word-count output, parallel map execution, parallel reduce execution, job counting, early-exit handling, and crash recovery.

---

## Lab 3 — Raft Consensus

> [Lab spec](https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html)

A complete implementation of the [Raft consensus algorithm](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf) — the foundation for building fault-tolerant replicated state machines.

**Implementation**

| File | Description |
|---|---|
| [src/raft1/raft.go](src/raft1/raft.go) | Core Raft struct, leader election, persistence, snapshotting |
| [src/raft1/raft_replication.go](src/raft1/raft_replication.go) | Log replication, AppendEntries RPC, commit logic |

**Parts completed**

| Part | What it covers |
|---|---|
| 3A — Leader Election | RequestVote RPCs, election timeouts, heartbeats |
| 3B — Log Replication | AppendEntries RPC, log consistency, `Start()` API |
| 3C — Persistence | Durable state (term, vote, log) survives crashes; accelerated log backtracking |
| 3D — Log Compaction *(TODO)* | Snapshots to bound log size; `InstallSnapshot` RPC for lagging followers |

**Run the tests**

```bash
cd src/raft1

# Part 3A — leader election
go test -run 3A

# Part 3B — log replication
go test -run 3B

# Part 3C — persistence
go test -run 3C
```
