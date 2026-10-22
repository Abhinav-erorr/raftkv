# RaftKV

RaftKV is an educational key-value store being built in small, verified
milestones. The current slice establishes a deterministic Raft election and
heartbeat core plus a concurrency-safe local KV state machine.

## Current milestone

- Go module and package boundaries: `raft`, `network`, `kv`, and `client`.
- Raft follower/candidate/leader transitions with term and vote handling.
- Empty `AppendEntries` heartbeats and leader recognition.
- Deterministic in-memory network with controllable links and synchronous
  delivery.
- Focused election, heartbeat, partition, and KV tests.

Log replication, persistence, client leader routing, request deduplication,
randomized fault injection, and Porcupine histories are intentionally future
milestones, not simulated by this implementation.

## Development

```text
go test ./...
go test -race ./...
gofmt -w .
```

See [docs/design.md](docs/design.md) for architecture and milestone details.
For a guided reading order, see [docs/learning.md](docs/learning.md).
