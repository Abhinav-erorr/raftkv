package raft

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidMessage = errors.New("raft: invalid message")
	ErrNotLeader      = errors.New("raft: not leader")
)

type Node struct {
	mu sync.Mutex

	id       uint64
	peers    map[uint64]struct{}
	role     Role
	term     uint64
	votedFor uint64
	leader   uint64
	votes    map[uint64]struct{}
	outbox   []Message

	electionTimeout   time.Duration
	heartbeatInterval time.Duration
	electionElapsed   time.Duration
	heartbeatElapsed  time.Duration
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == 0 || cfg.ElectionTimeout <= 0 || cfg.HeartbeatInterval <= 0 ||
		cfg.HeartbeatInterval >= cfg.ElectionTimeout {
		return nil, errors.New("raft: invalid config")
	}
	n := &Node{
		id: cfg.ID, peers: make(map[uint64]struct{}), role: Follower,
		electionTimeout: cfg.ElectionTimeout, heartbeatInterval: cfg.HeartbeatInterval,
	}
	for _, peer := range cfg.Peers {
		if peer != 0 && peer != cfg.ID {
			n.peers[peer] = struct{}{}
		}
	}
	return n, nil
}

func (n *Node) ID() uint64       { n.mu.Lock(); defer n.mu.Unlock(); return n.id }
func (n *Node) Role() Role       { n.mu.Lock(); defer n.mu.Unlock(); return n.role }
func (n *Node) Term() uint64     { n.mu.Lock(); defer n.mu.Unlock(); return n.term }
func (n *Node) LeaderID() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.leader }

func (n *Node) Tick(elapsed time.Duration) {
	if elapsed <= 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.electionElapsed += elapsed
	if n.role == Leader {
		n.heartbeatElapsed += elapsed
		if n.heartbeatElapsed >= n.heartbeatInterval {
			n.heartbeatElapsed = 0
			n.broadcastAppendEntriesLocked()
		}
		return
	}
	if n.electionElapsed >= n.electionTimeout {
		n.startElectionLocked()
	}
}

func (n *Node) DrainMessages() []Message {
	n.mu.Lock()
	defer n.mu.Unlock()
	messages := append([]Message(nil), n.outbox...)
	n.outbox = n.outbox[:0]
	return messages
}

func (n *Node) Step(msg Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if msg.To != n.id || msg.From == 0 || msg.Term == 0 {
		return ErrInvalidMessage
	}
	if msg.Term > n.term {
		n.becomeFollowerLocked(msg.Term, 0)
	}
	if msg.Term < n.term {
		if msg.Type == MsgRequestVote {
			n.enqueueLocked(Message{From: n.id, To: msg.From, Type: MsgRequestVoteResponse, Term: n.term})
		}
		return nil
	}

	switch msg.Type {
	case MsgRequestVote:
		n.handleRequestVoteLocked(msg)
	case MsgRequestVoteResponse:
		n.handleVoteResponseLocked(msg)
	case MsgAppendEntries:
		n.handleAppendEntriesLocked(msg)
	case MsgAppendEntriesResponse:
		// Reserved for log replication.
	default:
		return ErrInvalidMessage
	}
	return nil
}

func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.leader = 0
	n.votes = map[uint64]struct{}{n.id: {}}
	n.electionElapsed = 0
	for peer := range n.peers {
		n.enqueueLocked(Message{From: n.id, To: peer, Type: MsgRequestVote, Term: n.term})
	}
	if len(n.votes) >= n.quorumLocked() {
		n.becomeLeaderLocked()
	}
}

func (n *Node) handleRequestVoteLocked(msg Message) {
	grant := false
	if n.votedFor == 0 || n.votedFor == msg.From {
		n.votedFor = msg.From
		n.electionElapsed = 0
		grant = true
	}
	n.enqueueLocked(Message{From: n.id, To: msg.From, Type: MsgRequestVoteResponse, Term: n.term, Granted: grant})
}

func (n *Node) handleVoteResponseLocked(msg Message) {
	if n.role != Candidate || !msg.Granted {
		return
	}
	if n.votes == nil {
		n.votes = make(map[uint64]struct{})
	}
	n.votes[msg.From] = struct{}{}
	if len(n.votes) >= n.quorumLocked() {
		n.becomeLeaderLocked()
	}
}

func (n *Node) handleAppendEntriesLocked(msg Message) {
	if msg.LeaderID == 0 {
		return
	}
	if n.role != Follower || n.leader != msg.LeaderID {
		n.role = Follower
		n.votedFor = 0
	}
	n.leader = msg.LeaderID
	n.electionElapsed = 0
	n.enqueueLocked(Message{From: n.id, To: msg.From, Type: MsgAppendEntriesResponse, Term: n.term, Success: true})
}

func (n *Node) becomeFollowerLocked(term, leader uint64) {
	n.role = Follower
	n.term = term
	n.votedFor = 0
	n.leader = leader
	n.votes = nil
	n.electionElapsed = 0
	n.heartbeatElapsed = 0
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leader = n.id
	n.votedFor = n.id
	n.heartbeatElapsed = 0
	n.broadcastAppendEntriesLocked()
}

func (n *Node) broadcastAppendEntriesLocked() {
	for peer := range n.peers {
		n.enqueueLocked(Message{From: n.id, To: peer, Type: MsgAppendEntries, Term: n.term, LeaderID: n.id})
	}
}

func (n *Node) quorumLocked() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) enqueueLocked(msg Message) { n.outbox = append(n.outbox, msg) }
