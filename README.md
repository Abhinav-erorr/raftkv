# RaftKV

RaftKV is an educational key-value store being built in small, verified
milestones. The current slice establishes a deterministic Raft election and
heartbeat core plus a concurrency-safe local KV state machine.

## Current milestone

- Go module and package boundaries: `raft`, `network`, `kv`, and `client`.
- Raft follower/candidate/leader transitions with term and vote handling.
- Empty `AppendEntries` heartbeats and leader recognition.
- First log-replication slice: leader proposals, follower matching, and
  majority commit tracking.
- Deterministic in-memory network with controllable links and synchronous
  delivery.
- Focused election, heartbeat, partition, and KV tests.
- Runnable election demonstration under `cmd/raftkv-demo`.

Persistence, state-machine application, client leader routing, request
deduplication, randomized fault injection, and Porcupine histories remain
future milestones.

## Development

```text
go test ./...
go test -race ./...
gofmt -w .
```

See [docs/design.md](docs/design.md) for architecture and milestone details.
For a guided reading order, see [docs/learning.md](docs/learning.md).

## Run the election demo

```text
go run ./cmd/raftkv-demo
```

The demo advances logical time in fixed 25 ms steps and prints each node's
role, term, and known leader. It uses the same deterministic fake network as
the tests, so the election can be read and reproduced without sleeping or
running background goroutines.
