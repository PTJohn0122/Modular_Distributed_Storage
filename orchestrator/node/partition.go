package node

// StartPartition updates the network configuration to allow the given nodes to talk only to each
// other.
//
// Note: In most cases, you will have to call this function twice.
// Ex. If you want a complete partition of node 0 in a cluster of 3 nodes, you would first call
// StartPartition(clusterNodes, node1, node2) to allow nodes 1 and 2 to communicate only with each
// other followed by StartPartition(clusterNodes, node0) to allow node 0 to communicate only with
// itself
func StartPartition(cluster []*Node, partition ...*Node) {
	for _, node := range partition {
		node.partitionAllowMu.Lock()
		for _, peerNode := range partition {
			node.partitionAllowList[peerNode.ID] = struct{}{}
		}
		node.partitionAllowMu.Unlock()
	}
}

// EndPartition updates the network configuration to unconstrain the given nodes from being able
// to communicate with only each other.
//
// Note: If a node N is in multiple partitions P1 and P2, EndPartition(P1) will not make N
// available to the entire cluster. N will then only be able to communicate with the nodes in P2.
// We recommend not keeping a node in multiple partitions unless you are sure you want an
// asymmetric network partition.
func EndPartition(cluster []*Node, partition ...*Node) {
	for _, node := range partition {
		node.partitionAllowMu.Lock()
		for _, peerNode := range partition {
			delete(node.partitionAllowList, peerNode.ID)
		}
		node.partitionAllowMu.Unlock()
	}
}

// SuspendCommunication prevents nodes from sending a message to any of the nodes in peerNodes.
// Note that peerNode can still send messages to node.
//
// This can be used to:
// 1. Suspend communication between nodes and peerNodes (requires that partitionAllowList
// will not be empty following the removal of peerNodes)
// 2. Empty partitionAllowList, essentially resuming communication between nodes and all
// other nodes in the cluster
func SuspendCommunication(nodes []*Node, peerNodes ...*Node) {
	for _, node := range nodes {
		node.partitionAllowMu.Lock()
		for _, peerNode := range peerNodes {
			delete(node.partitionAllowList, peerNode.ID)
		}
		node.partitionAllowMu.Unlock()
	}
}

// EnableCommunication updates the network configuration to allow nodes to send messages to any of
// the nodes in peerNodes
//
// This can be used to:
// 1. Allow communication only between nodes and peerNodes
// 2. Simulate a delayed network between nodes and the remaining nodes not present in peerNodes
func EnableCommunication(nodes []*Node, peerNodes ...*Node) {
	for _, node := range nodes {
		node.partitionAllowMu.Lock()
		for _, peerNode := range peerNodes {
			node.partitionAllowList[peerNode.ID] = struct{}{}
		}
		node.partitionAllowMu.Unlock()
	}
}

// Whether n can communicate with peerNode. When not in a partition, a node can communicate with
// any other node. But when there is even a single peer in the allow-list, then the node can only
// talk to nodes that it is explicitly allowed to talk to.
func (n *Node) canCommunicate(peerNode *Node) bool {
	n.partitionAllowMu.RLock()
	defer n.partitionAllowMu.RUnlock()

	isNodeInPartition := len(n.partitionAllowList) != 0

	if !isNodeInPartition {
		return true
	}

	_, ok := n.partitionAllowList[peerNode.ID]
	return ok
}
