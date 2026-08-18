# Distributed Key-Value Store

A fault tolerant, replicated distributed key value store built in Go, powered by a custom Raft consensus engine.
This project focuses on understanding how replicated state machines are built: leader election, log replication, persistence, snapshotting, client retries, and linearizable key value operations.
## Overview
This project decouples and refactors core state machine replication and consensus mechanisms originally implemented in MIT 6.5840 into an open source, standalone key value database. 

## Status
This project is experimental and educational. It is not production grade.

## Key Features & Architecture
- **Consensus Engine:** Raft protocol handling leader election, log replication, and safety guarantees.
- **State Compaction:** Snapshotting and fast log recovery to maintain bounded state size under heavy write traffic.
- **RPC Communication:** Concurrent network transport layer utilizing the net/rpc package, Go channels, mutexes, and condition variables.

## Roadmap & Active Development
- [x] Raft leader election
- [x] Raft log replication
- [x] Persistent Raft state
- [x] Snapshot support
- [x] Replicated key value state machine
- [x] Basic client `Put` / `Get`
- [x] Shared RPC server for Raft and KV services
- [ ] Expanded KV test suite
- [ ] Fault injection tests
- [ ] Linearizability checking with Porcupine
- [ ] Documentation and demo polish

## Architecture

Each node runs both a Raft peer and a key value server.
