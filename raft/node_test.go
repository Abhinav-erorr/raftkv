package raft_test

import (
	"testing"
	"time"

	"raftkv/network"
	"raftkv/raft"
)

func makeCluster(t *testing.T, count int) (*network.FakeNetwork, []*raft.Node) {
	t.Helper()
	nodes := make([]*raft.Node, count)
	ids := make([]uint64, count)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	for i, id := range ids {
		peers := append([]uint64(nil), ids[:i]...)
		peers = append(peers, ids[i+1:]...)
		node, err := raft.NewNode(raft.Config{
			ID: id, Peers: peers, ElectionTimeout: (100 + time.Duration(id-1)*20) * time.Millisecond,
			HeartbeatInterval: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node
	}
	return network.NewFakeNetwork(nodes...), nodes
}

func TestElectionAndHeartbeat(t *testing.T) {
	net, nodes := makeCluster(t, 3)
	for i := 0; i < 8; i++ {
		net.Tick(25 * time.Millisecond)
	}
	var leaders int
	var leaderID uint64
	for _, node := range nodes {
		if node.Role() == raft.Leader {
			leaders++
			leaderID = node.ID()
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders = %d, want 1", leaders)
	}
	for _, node := range nodes {
		if node.ID() != leaderID && (node.LeaderID() != leaderID || node.Role() != raft.Follower) {
			t.Fatalf("node %d does not recognize leader %d", node.ID(), leaderID)
		}
	}
	term := nodes[0].Term()
	for i := 0; i < 10; i++ {
		net.Tick(20 * time.Millisecond)
	}
	for _, node := range nodes {
		if node.Term() != term {
			t.Fatalf("node %d term changed from %d to %d during heartbeats", node.ID(), term, node.Term())
		}
	}
}

func TestPartitionElectsNewLeaderWithMajority(t *testing.T) {
	net, nodes := makeCluster(t, 3)
	for i := 0; i < 8; i++ {
		net.Tick(25 * time.Millisecond)
	}

	var oldLeader *raft.Node
	for _, node := range nodes {
		if node.Role() == raft.Leader {
			oldLeader = node
		}
	}
	if oldLeader == nil {
		t.Fatal("no initial leader")
	}
	for _, node := range nodes {
		if node.ID() != oldLeader.ID() {
			net.Disconnect(oldLeader.ID(), node.ID())
		}
	}
	for i := 0; i < 8; i++ {
		net.Tick(25 * time.Millisecond)
	}
	var majorityLeader *raft.Node
	for _, node := range nodes {
		if node.ID() != oldLeader.ID() && node.Role() == raft.Leader {
			majorityLeader = node
		}
	}
	if majorityLeader == nil {
		t.Fatal("majority partition did not elect a leader")
	}
	if majorityLeader.Term() <= oldLeader.Term() {
		t.Fatalf("new leader term = %d, old leader term = %d", majorityLeader.Term(), oldLeader.Term())
	}
}

func TestLeaderReplicatesAndCommitsEntry(t *testing.T) {
	net, nodes := makeCluster(t, 3)
	for i := 0; i < 8; i++ {
		net.Tick(25 * time.Millisecond)
	}
	var leader *raft.Node
	for _, node := range nodes {
		if node.Role() == raft.Leader {
			leader = node
		}
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}
	if err := leader.Propose("set x=1"); err != nil {
		t.Fatal(err)
	}
	net.Flush()
	if leader.CommitIndex() != 1 {
		t.Fatalf("leader commit index = %d, want 1", leader.CommitIndex())
	}
	for _, node := range nodes {
		entries := node.Log()
		if len(entries) != 1 || entries[0].Command != "set x=1" {
			t.Fatalf("node %d log = %#v, want replicated command", node.ID(), entries)
		}
	}
}
