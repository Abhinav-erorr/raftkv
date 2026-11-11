package network

import (
	"errors"
	"sync"
	"time"

	"raftkv/raft"
)

// FakeNetwork is a deterministic, in-memory transport. DeliverAll drains the
// queue synchronously, making tests independent of goroutine scheduling.
type FakeNetwork struct {
	mu        sync.Mutex
	nodes     map[uint64]*raft.Node
	connected map[[2]uint64]bool
	queue     []raft.Message
}

func NewFakeNetwork(nodes ...*raft.Node) *FakeNetwork {
	n := &FakeNetwork{nodes: make(map[uint64]*raft.Node), connected: make(map[[2]uint64]bool)}
	for _, node := range nodes {
		n.nodes[node.ID()] = node
	}
	for from := range n.nodes {
		for to := range n.nodes {
			if from != to {
				n.connected[[2]uint64{from, to}] = true
			}
		}
	}
	return n
}

func (n *FakeNetwork) Disconnect(a, b uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[[2]uint64{a, b}] = false
	n.connected[[2]uint64{b, a}] = false
}

func (n *FakeNetwork) Connect(a, b uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[[2]uint64{a, b}] = true
	n.connected[[2]uint64{b, a}] = true
}

func (n *FakeNetwork) Tick(elapsed time.Duration) {
	for _, node := range n.nodesSnapshot() {
		node.Tick(elapsed)
		n.collect(node)
	}
	n.DeliverAll()
}

func (n *FakeNetwork) Flush() {
	for _, node := range n.nodesSnapshot() {
		n.collect(node)
	}
	n.DeliverAll()
}

func (n *FakeNetwork) collect(node *raft.Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.queue = append(n.queue, node.DrainMessages()...)
}

func (n *FakeNetwork) DeliverAll() {
	for {
		n.mu.Lock()
		if len(n.queue) == 0 {
			n.mu.Unlock()
			return
		}
		msg := n.queue[0]
		n.queue = n.queue[1:]
		connected := n.connected[[2]uint64{msg.From, msg.To}]
		target := n.nodes[msg.To]
		n.mu.Unlock()
		if !connected || target == nil {
			continue
		}
		if err := target.Step(msg); err != nil && !errors.Is(err, raft.ErrInvalidMessage) {
			continue
		}
		n.collect(target)
	}
}

func (n *FakeNetwork) Node(id uint64) *raft.Node {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodes[id]
}

func (n *FakeNetwork) nodesSnapshot() []*raft.Node {
	n.mu.Lock()
	defer n.mu.Unlock()
	nodes := make([]*raft.Node, 0, len(n.nodes))
	for _, node := range n.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}
