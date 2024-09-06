package node

import (
	"testing"
)

func ensureFullCommunication(t *testing.T, nodes []*Node) {
	for _, node := range nodes {
		for _, peerNode := range nodes {
			if !node.canCommunicate(peerNode) {
				t.Errorf("Expected node %v to be able to communicate with %v", node.ID, peerNode.ID)
			}

			if !peerNode.canCommunicate(node) {
				t.Errorf("Expected node %v to be able to communicate with %v", peerNode.ID, node.ID)
			}
		}
	}
}

func TestCanCommunicateWithoutPartition(t *testing.T) {
	nodes := Create([]string{"localhost:2001", "localhost:2002", "localhost:2003"})
	ensureFullCommunication(t, nodes)
}

func TestCanPartitionOneNode(t *testing.T) {
	nodes := Create([]string{"localhost:3001", "localhost:3002", "localhost:3003"})
	StartPartition(nodes, nodes[0], nodes[1])
	StartPartition(nodes, nodes[2])

	// Make sure that nodes[0] cannot communiate with anybody but itself
	for _, node := range nodes {
		if node.ID == nodes[2].ID {
			if !node.canCommunicate(nodes[2]) {
				t.Errorf(
					"Expected node %d to be able to communicate with %d, but could not",
					node.ID,
					nodes[2].ID,
				)
			}
			if node.canCommunicate(nodes[0]) || node.canCommunicate(nodes[1]) {
				t.Errorf(
					"Expected node %d to not be able to communicate with node %d and %d, but could",
					node.ID,
					nodes[0].ID,
					nodes[1].ID,
				)
			}
		} else {
			if node.canCommunicate(nodes[2]) {
				t.Errorf(
					"Expected node %d to not be able to communicate with %d, but could",
					node.ID,
					nodes[2].ID,
				)
			}
			if !node.canCommunicate(nodes[0]) || !node.canCommunicate(nodes[1]) {
				t.Errorf(
					"Expected node %d to be able to communicate with node %d and %d, but could not",
					node.ID,
					nodes[0].ID,
					nodes[1].ID,
				)
			}
		}
	}

	EndPartition(nodes, nodes...)
	ensureFullCommunication(t, nodes)
}

func TestAsymmetricPartition(t *testing.T) {
	nodes := Create([]string{"localhost:4001", "localhost:4002", "localhost:4003"})
	StartPartition(nodes, nodes[0], nodes[1])

	for _, node := range nodes {
		if node.ID == nodes[2].ID {
			if !node.canCommunicate(nodes[0]) || !node.canCommunicate(nodes[1]) ||
				!node.canCommunicate(nodes[2]) {
				t.Errorf(
					"Expected node %d to be able to communicate with all nodes, but could not",
					node.ID,
				)
			}
		} else {
			if !node.canCommunicate(nodes[0]) || !node.canCommunicate(nodes[1]) {
				t.Errorf(
					"Expected node %d to be able to communicate with node %v and %v, but could not",
					node.ID,
					nodes[0].ID,
					nodes[1].ID,
				)
			}
			if node.canCommunicate(nodes[2]) {
				t.Errorf(
					"Expected node %d to not be able to communicate with node %v, but could",
					node.ID,
					nodes[2].ID,
				)
			}
		}
	}
}
