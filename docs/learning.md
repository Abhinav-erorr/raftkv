# Learning guide: Raft election and heartbeats

This repository intentionally stops before log replication. The goal of this
milestone is to understand the safety-critical election core without hiding it
behind a large implementation.

## Suggested reading order

1. Read the Raft paper's **Figure 2** and the sections on server rules,
   elections, and leader election.
2. Read [`raft/types.go`](../raft/types.go) for the vocabulary used in this
   implementation.
3. Read [`raft/node.go`](../raft/node.go) in this order:
   `Tick`, `startElectionLocked`, `Step`, `handleRequestVoteLocked`,
   `handleVoteResponseLocked`, `handleAppendEntriesLocked`, and
   `becomeLeaderLocked`.
4. Read [`network/fake.go`](../network/fake.go) to see how time and message
   delivery are made deterministic.
5. Run and study [`raft/node_test.go`](../raft/node_test.go), then change one
   timing or link condition at a time and predict the result.

## Figure 2 mapped to this code

| Figure 2 concept | Current implementation |
| --- | --- |
| `currentTerm` | `Node.term` |
| `votedFor` | `Node.votedFor` |
| server state: follower/candidate/leader | `Node.role` |
| election timer | `electionElapsed` compared with `electionTimeout` |
| heartbeat timer | `heartbeatElapsed` compared with `heartbeatInterval` |
| candidate increments term and votes for itself | `startElectionLocked` |
| candidate sends `RequestVote` | `startElectionLocked` queues messages |
| vote response and majority | `handleVoteResponseLocked` and `quorumLocked` |
| leader sends empty `AppendEntries` | `broadcastAppendEntriesLocked` |
| follower resets election timer from leader contact | `handleAppendEntriesLocked` |
| higher-term message causes step-down | `Step` calls `becomeFollowerLocked` |

The message type already called `AppendEntries` is currently an empty
heartbeat. It carries no log entries, commit index, or previous-log
consistency fields yet. That is deliberate: those belong to the next
milestone.

## Terms, voting, and election safety

`currentTerm` is monotonic. A message with a higher term updates the node's
term and forces it back to follower. A message from an older term is ignored,
so stale leaders and delayed responses cannot overwrite newer state.

Each node grants at most one vote per term. The candidate records its own vote,
then becomes leader only after receiving a quorum. In a three-node cluster the
quorum is two; two different candidates cannot both obtain two votes because
their majorities overlap in at least one node.

This first slice does not yet compare candidate log freshness. That check is
required once logs exist and is intentionally absent rather than faked.

## Heartbeats

The leader sends empty `AppendEntries` messages at the configured heartbeat
interval. A follower that accepts one records the leader ID and resets its
election timer. As long as heartbeats arrive before the election timeout, the
followers do not start a competing election.

The fake network advances logical time with `Tick`, collects outbound messages,
and delivers them synchronously until the queue is empty. There are no wall
clock sleeps or transport goroutines in these tests, so a failure can be
reproduced from the same sequence of ticks and link changes.

## Failure scenarios to reason through

- **Leader crash:** stop delivering messages from the leader. A follower
  eventually reaches its election timeout and starts a new term.
- **One-way or full partition:** a disconnected minority cannot form a
  quorum. The connected majority can elect a replacement leader.
- **Delayed vote response:** deliver an old response after a newer election.
  The term and candidate-role checks prevent it from granting leadership.
- **Stale leader heartbeat:** deliver an old-term heartbeat after a newer term
  is known. It is ignored.
- **Simultaneous timeout:** identical timeouts can create split votes. The
  tests use stable per-node timeout offsets to make the scenario deterministic;
  a production scheduler should use randomized election timeouts.

## Current boundaries

`Node.Step` is the boundary between the Raft state machine and transport.
`network.FakeNetwork` is only a test transport. `kv.Store` and
`client.Local` are local, concurrency-safe foundations; writes are not
replicated and must not be described as committed Raft operations.

Not implemented in this milestone: log entries, `nextIndex`/`matchIndex`,
commit advancement, persistence, client leader routing, request
deduplication, randomized fault injection, and Porcupine histories. The next
milestone should add these one at a time, starting with a single replicated
log entry and a commit/apply path.
