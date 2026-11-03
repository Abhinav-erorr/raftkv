# Design

## Architecture

`raft.Node` owns the Raft state machine and exposes deterministic `Tick`,
`Step`, and `DrainMessages` operations. It does not own goroutines, wall-clock
timers, or transport details. This keeps election behavior reproducible and
lets a real transport be added without changing the core state transitions.

`network.FakeNetwork` is a synchronous transport for tests. It advances every
node by a supplied duration, collects outbound messages, and delivers them
until quiescent. Links can be disconnected independently in both directions
through the public pair operations.

`kv.Store` is the initial concurrency-safe state machine. `client.Local`
provides a deliberately small client boundary while the future network client
will add leader discovery and retry behavior.

`cmd/raftkv-demo` is a small executable walkthrough of the current
milestone. It constructs three nodes, advances logical time, and prints the
resulting election state without introducing a second implementation path.

## Milestones

1. **Complete:** module/package foundation; deterministic election and
   heartbeat core; fake network; focused tests; and a runnable demo.
2. **Next:** replicated log entries, commit index, apply channel, and
   AppendEntries consistency checks.
3. **Next:** stable storage and restart recovery.
4. **Next:** client routing, request IDs, and deduplication.
5. **Next:** randomized partitions/delays and Porcupine history generation.

The current heartbeat message intentionally carries no log entries. Any API
that would imply committed writes should wait for milestone 2 rather than
pretend the local store is replicated.
