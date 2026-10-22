package raft

import "time"

// Role is the current role of a node in the Raft state machine.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

type MessageType uint8

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResponse
	MsgAppendEntries
	MsgAppendEntriesResponse
)

// Message is the transport-independent Raft wire message. Entries are
// intentionally absent from this first milestone; AppendEntries is a
// heartbeat until log replication is added.
type Message struct {
	From        uint64
	To          uint64
	Type        MessageType
	Term        uint64
	Granted     bool
	Success     bool
	LeaderID    uint64
	LastLogIdx  uint64
	LastLogTerm uint64
}

// Config controls deterministic node timing. Timeouts are logical durations:
// callers advance them explicitly through Tick rather than depending on wall
// clock scheduling.
type Config struct {
	ID                uint64
	Peers             []uint64
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
}
