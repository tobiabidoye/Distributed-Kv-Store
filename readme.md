# Distributed Key-Value Store

A fault tolerant, replicated distributed key value store built in Go, powered by a custom Raft consensus engine.

## Overview
This project decouples and refactors core state machine replication and consensus mechanisms originally implemented in MIT 6.5840 into an open source, standalone key value database. 

## Key Features & Architecture
- **Consensus Engine:** Raft protocol handling leader election, log replication, and safety guarantees.
- **State Compaction:** Snapshotting and fast log recovery to maintain bounded state size under heavy write traffic.
- **RPC Communication:** Concurrent network transport layer utilizing the net/rpc package, Go channels, mutexes, and condition variables.

## Roadmap & Active Development
- [x] Initial repository scaffolding & core interface definitions
- [x] Port consensus module & RPC synchronization
- [ ] Implement key-value state machine layer & snapshotting
- [ ] Add linearizability test suite & fault-injection framework
