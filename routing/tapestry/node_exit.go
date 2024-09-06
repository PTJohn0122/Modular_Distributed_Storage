/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines functions for a node leaving the Tapestry mesh, and
 *  transferring its stored locations to a new node.
 */

package tapestry

import (
	"context"
	pb "modist/proto"
	"sync"
)

// Kill this node without gracefully leaving the tapestry.
func (local *TapestryNode) Kill() {
	local.blobstore.DeleteAll()
	local.Node.GrpcServer.Stop()
}

// Leave gracefully exits the Tapestry mesh.
//
// - Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
// - If possible, give each backpointer a suitable alternative node from our routing table
func (local *TapestryNode) Leave() error {
	// According to TA, replacement could just be the first node in each level
	var wg sync.WaitGroup

	for level := 0; level < DIGITS; level++ {
		replacement := ""
		nodes := local.Table.GetLevel(level)
		if len(nodes) > 0 {
			replacement = nodes[0].String()
		}

		set := local.Backpointers.Get(level)
		for _, nodeId := range set {
			wg.Add(1)
			go func(nodeId ID) {
				conn := local.Node.PeerConns[local.RetrieveID(nodeId)]
				node := pb.NewTapestryRPCClient(conn)
				node.NotifyLeave(context.Background(), &pb.LeaveNotification{
					From:        local.Id.String(),
					Replacement: replacement,
				})
				wg.Done()
			}(nodeId)
		}
	}

	wg.Wait()
	local.blobstore.DeleteAll()
	go local.Node.GrpcServer.GracefulStop()
	return nil
}

// NotifyLeave occurs when another node is informing us of a graceful exit.
// - Remove references to the `from` node from our routing table and backpointers
// - If replacement is not an empty string, add replacement to our routing table
func (local *TapestryNode) NotifyLeave(
	ctx context.Context,
	leaveNotification *pb.LeaveNotification,
) (*pb.Ok, error) {
	from, err := ParseID(leaveNotification.From)
	if err != nil {
		return nil, err
	}
	// Replacement can be an empty string so we don't want to parse it here
	replacement := leaveNotification.Replacement

	local.log.Printf(
		"Received leave notification from %v with replacement node %v\n",
		from,
		replacement,
	)

	local.Table.Remove(from)
	local.Backpointers.Remove(from)

	if replacement != "" {
		replacementId, err := ParseID(replacement)
		if err != nil {
			return nil, err
		}

		local.Table.Add(replacementId) // TODO: Q: need to check if there's an evicted node?
	}

	return &pb.Ok{Ok: true}, nil
}
