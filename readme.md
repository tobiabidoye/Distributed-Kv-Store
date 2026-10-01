# Distributed Key-Value Store
A linearizable, fault tolerant distributed key value store in Go, built on a custom Raft
consensus engine with durable persistence, snapshotting, and client side retries.
Correctness is verified with model checking: client visible histories are checked for
linearizability with Porcupine under concurrent load.

## Highlights
- **Custom Raft implementation** — leader election, log replication, persistence, and
  snapshotting, with no consensus or storage dependencies.
- **Verified linearizable** — concurrent client histories are model checked against a
  hand written sequential specification of the versioned Put/Get semantics using Porcupine.
- **Durable by construction** — atomic write then rename persistence with file *and*
  directory fsync; a write is either fully durable or never visible.
- **Failure-tested** — replication verified with dead followers and dead leaders mid traffic,
  snapshot based catch up exercised under forced log truncation.
- **Deployable** — nodes run as standalone binaries with flag based peer configuration and
  graceful shutdown nothing is test harness only.

## Quickstart

```bash
# terminal 1-3: one node per process
go run ./cmd/server -id 0 -peers 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002
go run ./cmd/server -id 1 -peers 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002
go run ./cmd/server -id 2 -peers 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002

# terminal 4: interactive client
go run ./cmd/client -servers 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002
raft-kv [node:0]> put apple orange 0
raft-kv [node:0]> get apple
```

Run the test suite (includes the linearizability check):

```bash
go test ./... -race
```
## Verification

Testing is the point of this project, so this is what is and is not verified:

- **Linearizability (Porcupine).** `kv/sequential_model.go` defines a sequential
  specification of the store including the versioned Put semantics and
  ambiguous acknowledgment handling  and `TestPorcupineLinearizability` checks live
  concurrent histories against it. A failing run dumps a visualization of the violating
  history.
- **Replication under failure.** Tests cover leader election under disruption, replication
  with a dead follower, and a killed and restarted leader.
- **Snapshots.** A dedicated test forces aggressive log truncation (`maxraftstate=1000`) so
  followers must catch up via InstallSnapshot rather than the log.
- **Race detection.** The client library is exercised concurrently under `-race`; this
  surfaced and fixed a data race in the client's connection cache.
- **Flakiness as a signal.** The suite was stabilized by fixing root causes such as port reuse
  across tests and disk flush timing under snapshot contention.

Known limits: histories are checked on a stable cluster, fault injection *during*
linearizability checking is planned. The `ErrMaybe` model branch covers an
ambiguous-ack path that the current harness does not yet generate.

### Benchmarks

Local: 3 nodes, 1 machine. Cloud: 3 × t3.small across AZs, client on a 4th instance.

| Metric | Local | AWS |
|---|---|---|
| Put throughput (ops/s) | 48.77 ops/sec | *pending* |
| Get throughput (ops/s) | 44.71 ops/sec | *pending* |
| Put p50 / p99 latency | 315.5ms / 502.8ms | *pending* |
| Get p50 / p99 latency | 341.3ms / 557.9ms | *pending* |
| Leader re-election time after kill | *pending* | *pending* |
| Node catch-up time after restart (snapshot) | *pending* | *pending* |
## Failure modes

| Injected failure | Expected behavior | Observed |
|---|---|---|
| Leader killed mid-traffic | Re-election, no lost acknowledged writes | covered by test suite |
| Follower killed | Replication continues at quorum | covered by test suite |
| Partition (SG rule drop) | Minority isolated; heals cleanly | _pending AWS run_ |

## Architecture

Each node runs the same stack:

```
client ──► KV API (RPC) ──► state machine (versioning + dedup)
                              │  ▲
                        submit │  │ apply (committed entries)
                              ▼  │
                    Raft core ◄──┘
                   (log, election, commit)
                       │           │
              raft_state.bin   snapshots
```

- **Raft** (`raft/`): consensus election, log replication, commit index, snapshots.
  Owns ordering and durability of the log.
- **RSM** (`rsm/`): generic replicated state machine submit/apply loop, snapshot
  triggering. Storage is plugged in behind a `StateMachine` interface (`DoOp`/`Snapshot`/
  `Restore`).
- **KV** (`kv/`): the state machine — versioned Puts with client dedup for exactly once
  replay semantics.
- **Persister** (`persister/`): atomic write then rename with fsync on file and directory.
- **Client** (`kv/client.go`): retries, leader rediscovery, connection cache with
  timeout guarded dialing.

Key design decision: the consensus layer knows nothing about key-value semantics, and the
state machine knows nothing about consensus. That's what makes the storage engine
replaceable (see roadmap) — and it's the property that lets the Raft test suite guarantee
no regressions when storage changes.

## Roadmap & Active Development
- [x] Raft leader election
- [x] Raft log replication
- [x] Persistent Raft state
- [x] Snapshot support
- [x] Replicated key value state machine
- [x] Basic client `Put` / `Get`
- [x] Shared RPC server for Raft and KV services
- [x] Expanded KV test suite
- [x] Linearizability checking with Porcupine
- [ ] AWS deployment + cloud benchmark table
- [ ] Fault injection tests
- [ ] Documentation and demo polish
- [ ] Observability: metrics endpoint (term, commit index, log length, snapshot count)
- [ ] Lsm Tree Storage Engine Extentsion


## Origin
Built as a standalone system after implementing MIT 6.5840's labs, the transport layer,
persistence, versioning/dedup semantics, snapshot integration, and test/verification
infrastructure here are original work

## Authorship
All systems code (Raft, RSM, KV, persister, client, server) is written by hand. The
Porcupine test harness interactive CLI and readme ere AI-assisted and reviewed line by line.

