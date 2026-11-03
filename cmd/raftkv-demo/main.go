package main

import (
	"fmt"
	"time"

	"raftkv/network"
	"raftkv/raft"
)

func main() {
	ids := []uint64{1, 2, 3}
	nodes := make([]*raft.Node, 0, len(ids))

	for _, id := range ids {
		peers := make([]uint64, 0, len(ids)-1)
		for _, peerID := range ids {
			if peerID != id {
				peers = append(peers, peerID)
			}
		}

		node, err := raft.NewNode(raft.Config{
			ID:                id,
			Peers:             peers,
			ElectionTimeout:   (100 + time.Duration(id-1)*20) * time.Millisecond,
			HeartbeatInterval: 20 * time.Millisecond,
		})
		if err != nil {
			panic(err)
		}
		nodes = append(nodes, node)
	}

	net := network.NewFakeNetwork(nodes...)
	fmt.Println("advancing the deterministic network...")
	for step := 1; step <= 8; step++ {
		net.Tick(25 * time.Millisecond)
		fmt.Printf("tick %d\n", step)
	}

	fmt.Println("\ncluster state:")
	for _, node := range nodes {
		fmt.Printf("node %d: role=%s term=%d leader=%d\n",
			node.ID(), node.Role(), node.Term(), node.LeaderID())
	}
}
