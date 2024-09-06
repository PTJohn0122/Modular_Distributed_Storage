/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines global constants and functions to create and join a new
 *  node into a Tapestry mesh, and functions for altering the routing table
 *  and backpointers of the local node that are invoked over RPC.
 */

package tapestry

import (
	"context"
	"fmt"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sort"
	"time"
)

// BASE is the base of a digit of an ID.  By default, a digit is base-16.
const BASE = 16

// DIGITS is the number of digits in an ID.  By default, an ID has 40 digits.
const DIGITS = 40

// RETRIES is the number of retries on failure. By default we have 3 retries.
const RETRIES = 3

// K is neigborset size during neighbor traversal before fetching backpointers. By default this has a value of 10.
const K = 10

// SLOTSIZE is the size each slot in the routing table should store this many nodes. By default this is 3.
const SLOTSIZE = 3

// REPUBLISH is object republish interval for nodes advertising objects.
const REPUBLISH = 10 * time.Second

// TIMEOUT is object timeout interval for nodes storing objects.
const TIMEOUT = 25 * time.Second

// TapestryNode is the main struct for the local Tapestry node. Methods can be invoked locally on this struct.
type TapestryNode struct {
	Node *node.Node // Node that this Tapestry node is part of
	Id   ID         // ID of node in the form of a slice (makes it easier to iterate)

	Table          *RoutingTable // The routing table
	Backpointers   *Backpointers // Backpointers to keep track of other nodes that point to us
	LocationsByKey *LocationMap  // Stores keys for which this node is the root
	blobstore      *BlobStore    // Stores blobs on the local node

	// Observability
	log *log.Logger

	// These functions are the internal, private RPCs for a routing node using Tapestry
	pb.UnsafeTapestryRPCServer
}

func (local *TapestryNode) String() string {
	return fmt.Sprint(local.Id)
}

// Uniqueresults is a helper function to remove repetitive results in a number of string slices
// ref: https://www.code-paste.com/golang-merge-slices-unique-remove-duplicates/
func UniqueResults[T string | ID](slices ...[]T) []T {
	seen := make(map[T]bool)
	result := []T{}

	for _, slice := range slices {
		for _, e := range slice {
			if _, ok := seen[e]; !ok {
				seen[e] = true
				result = append(result, e)
			}
		}
	}

	return result
}

// Called in tapestry initialization to create a tapestry node struct
func newTapestryNode(node *node.Node) *TapestryNode {
	tn := new(TapestryNode)

	tn.Node = node
	tn.Id = MakeID(node.ID)
	tn.Table = NewRoutingTable(tn.Id)
	tn.Backpointers = NewBackpointers(tn.Id)
	tn.LocationsByKey = NewLocationMap()
	tn.blobstore = NewBlobStore()

	tn.log = node.Log

	return tn
}

// Start Tapestry Node
func StartTapestryNode(node *node.Node, connectTo uint64, join bool) (tn *TapestryNode, err error) {
	// Create the local node
	tn = newTapestryNode(node)

	tn.log.Printf("Created tapestry node %v\n", tn)

	grpcServer := tn.Node.GrpcServer
	pb.RegisterTapestryRPCServer(grpcServer, tn)

	// If specified, connect to the provided ID
	if join {
		// If provided ID doesn't exist, return an error
		if _, ok := node.PeerConns[connectTo]; !ok {
			return nil, fmt.Errorf(
				"Error joining Tapestry node with id %v; Unable to find node %v in peerConns",
				connectTo,
				connectTo,
			)
		}

		err = tn.Join(MakeID(connectTo))
		if err != nil {
			tn.log.Printf(err.Error())
			return nil, err
		}
	}

	return tn, nil
}

// removeDuplicatesAndTrimToK is a helper function work as its name. Used in join for backpoint traversal
func (local *TapestryNode) removeDuplicatesAndTrimToK(neighbors []ID) []ID {
	if len(neighbors) <= K {
		return neighbors
	} else {
		sort.Slice(neighbors, func(i, j int) bool {
			// IdA, _ := ParseID(neighbors[i])
			// IdB, _ := ParseID(neighbors[i])
			return local.Id.Closer(neighbors[i], neighbors[j])
		})
		return neighbors[0:K]
	}
}

// Join is invoked when starting the local node, if we are connecting to an existing Tapestry.
//
// - Find the root for our node's ID
// - Call AddNode on our root to initiate the multicast and receive our initial neighbor set. Add them to our table.
// - Iteratively get backpointers from the neighbor set for all levels in range [0, SharedPrefixLength]
// and populate routing table
func (local *TapestryNode) Join(remoteNodeId ID) error {
	local.log.Println("Joining tapestry node", remoteNodeId)

	// Route to our root
	rootIdPtr, err := local.FindRootOnRemoteNode(remoteNodeId, local.Id)
	if err != nil {
		return fmt.Errorf("Error joining existing tapestry node %v, reason: %v", remoteNodeId, err)
	}
	rootId := *rootIdPtr

	// Add ourselves to our root by invoking AddNode on the remote node
	nodeMsg := &pb.NodeMsg{
		Id: local.Id.String(),
	}

	conn := local.Node.PeerConns[local.RetrieveID(rootId)]
	rootNode := pb.NewTapestryRPCClient(conn)
	resp, err := rootNode.AddNode(context.Background(), nodeMsg)
	if err != nil {
		return fmt.Errorf("Error adding ourselves to root node %v, reason: %v", rootId, err)
	}

	// Add the neighbors to our local routing table.
	neighborIds, err := stringSliceToIds(resp.Neighbors)
	if err != nil {
		return fmt.Errorf("Error parsing neighbor IDs, reason: %v", err)
	}

	localIdx := -1 // set to be negative so that if it remains negative after the loop, we know local is not in neighborIds
	for idx, neighborId := range neighborIds {
		if neighborId == local.Id {
			localIdx = idx
		} else {
			local.AddRoute(neighborId)
		}
	}
	if localIdx != -1 { // remove self from neighborIds
		neighborIds = append(neighborIds[:localIdx], neighborIds[localIdx+1:]...)
	}

	// populate new routing table by backpointer traversal
	// local.log.Println("[Join] returned from root.AddNode()")

	level := SharedPrefixLength(rootId, local.Id)
	currNeighbors := neighborIds
	var nextNeighbors []ID

	for level >= 0 {
		currNeighbors = local.removeDuplicatesAndTrimToK(currNeighbors)
		nextNeighbors = currNeighbors

		resultChan := make(chan []ID)
		for _, neighborID := range currNeighbors {
			go func(neighborID ID, level int) {
				neighborConn := local.Node.PeerConns[local.RetrieveID(neighborID)]
				neighborNode := pb.NewTapestryRPCClient(neighborConn)
				backPointers, err := neighborNode.GetBackpointers(context.Background(), &pb.BackpointerRequest{
					From:  local.Id.String(),
					Level: int32(level),
				})
				if err != nil {
					// local.log.Printf("[Join] %v: GetBP on node %v failed. Remove the badnode.\n", local.Id, neighborID)
					local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{neighborID.String()}})
					resultChan <- make([]ID, 0)
				} else {
					// local.log.Printf("[Join] Received back pointers from (id:%v):%v\n", neighborID, backPointers.Neighbors)
					neighbors, _ := stringSliceToIds(backPointers.Neighbors)
					resultChan <- neighbors
				}
			}(neighborID, level)
		}

		for i := 0; i < len(currNeighbors); i++ { // collect results
			neighbors := <-resultChan
			nextNeighbors = append(nextNeighbors, neighbors...)
		}

		nextNeighbors = UniqueResults(nextNeighbors) // remove repetitive
		// local.log.Printf("[Join] Gathered all neighbor in current level(%v):%v\n", level, nextNeighbors)

		localIdx = -1
		for idx, popNodes := range nextNeighbors {
			if popNodes == local.Id {
				localIdx = idx
			} else {
				// local.log.Println("[Join] Add Route on node", local.Id, " Node to be added", popNodes)
				local.AddRoute(popNodes)
			}
		}
		if localIdx != -1 { // remove self from nextNeighbors
			nextNeighbors = append(nextNeighbors[:localIdx], nextNeighbors[localIdx+1:]...)
		}

		// local.log.Println("[Join] Finished AddRoute nextNeighbors, currently on level", level)
		currNeighbors = nextNeighbors
		level--
	}
	// local.log.Println("[Join] Officially finished")
	return nil
}

// AddNode adds node to the tapestry
//
// - Begin the acknowledged multicast
// - Return the neighborset from the multicast
func (local *TapestryNode) AddNode(
	ctx context.Context,
	nodeMsg *pb.NodeMsg,
) (*pb.Neighbors, error) {
	nodeId, err := ParseID(nodeMsg.Id)
	if err != nil {
		return nil, err
	}

	multicastRequest := &pb.MulticastRequest{
		NewNode: nodeMsg.Id,
		Level:   int32(SharedPrefixLength(nodeId, local.Id)),
	}
	return local.AddNodeMulticast(context.Background(), multicastRequest)
}

// AddNodeMulticast sends newNode to need-to-know nodes participating in the multicast.
//   - Perform multicast to need-to-know nodes
//   - Add the route for the new node (use `local.addRoute`)
//   - Transfer of appropriate router info to the new node (use `local.locationsByKey.GetTransferRegistrations`)
//     If error, rollback the location map (add back unsuccessfully transferred objects)
//
// - Propagate the multicast to the specified row in our routing table and await multicast responses
// - Return the merged neighbor set
//
// - note: `local.table.GetLevel` does not return the local node so you must manually add this to the neighbors set
func (local *TapestryNode) AddNodeMulticast(
	ctx context.Context,
	multicastRequest *pb.MulticastRequest,
) (*pb.Neighbors, error) {
	newNodeId, err := ParseID(multicastRequest.NewNode)
	if err != nil {
		return nil, err
	}
	level := int(multicastRequest.Level)

	local.log.Printf("Add node multicast %v at level %v\n", newNodeId, level)

	// base case to quit recursion: reach maximum level
	if level >= DIGITS {
		local.AddRoute(newNodeId) // Add route for new NodeID

		// wrap transfer in go routine since it has RPC calls
		go func(local *TapestryNode, newNodeId ID) {
			// cast the transfer data
			relativeNodes := local.LocationsByKey.GetTransferRegistrations(local.Id, newNodeId)
			needTransfer := make(map[string]*pb.Neighbors)
			for key, node := range relativeNodes {
				needTransfer[key] = &pb.Neighbors{Neighbors: idsToStringSlice(node)}
			}

			newNodeConn := local.Node.PeerConns[local.RetrieveID(newNodeId)]
			newNode := pb.NewTapestryRPCClient(newNodeConn)
			_, err = newNode.Transfer(context.Background(), &pb.TransferData{
				From: local.Id.String(),
				Data: needTransfer,
			})
			if err != nil {
				// local.log.Printf("[AddNodeMultiCast] %v: transfer to node %v failed. Remove the badnode.\n", local.Id, newNodeId)
				local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{newNodeId.String()}})
				// add back unsuccessfully transferred objects
				local.LocationsByKey.RegisterAll(relativeNodes, TIMEOUT)
			}
		}(local, newNodeId)

		return &pb.Neighbors{Neighbors: make([]string, 0)}, nil
	}

	targetIDs := local.Table.GetLevel(level)
	targetIDs = append(targetIDs, local.Id)
	result := pb.Neighbors{Neighbors: []string{}}
	resultChan := make(chan []string)

	for _, id := range targetIDs {
		go func(id ID, level int) {
			// local.log.Printf("[Add node multicast] go routine: level %v, id %v\n", level, id)
			conn := local.Node.PeerConns[local.RetrieveID(id)]
			targetNode := pb.NewTapestryRPCClient(conn)
			nextLevelNodes, err := targetNode.AddNodeMulticast(context.Background(), &pb.MulticastRequest{
				NewNode: newNodeId.String(),
				Level:   int32(level + 1),
			})
			if err != nil {
				// local.log.Printf("[Add node multicast] node %v received error from node %v, need to remove bad nodes\n", local.Id, id)
				local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{id.String()}})
				resultChan <- make([]string, 0)
			} else {
				// local.log.Printf("[Add node multicast] node %v received neighbors from node %v\n", local.Id, id)
				if len(nextLevelNodes.Neighbors) > 0 {
					// local.log.Printf("[Add node multicast] next level neighbors:%v\n", nextLevelNodes.Neighbors)
					resultChan <- nextLevelNodes.Neighbors
				} else {
					resultChan <- make([]string, 0)
				}
			}
		}(id, level)
	}

	for i := 0; i < len(targetIDs); i++ { // collect the results
		nextLevelNeighbors := <-resultChan
		result.Neighbors = append(result.Neighbors, nextLevelNeighbors...)
	}

	// local.log.Printf("[Add node multicast] id %v collected all results. Eventual result = %v\n", local.Id, result.Neighbors)
	result.Neighbors = append(result.Neighbors, idsToStringSlice(targetIDs)...)
	result.Neighbors = UniqueResults(result.Neighbors) // remove repetitive from result
	// local.log.Printf("[Add node multicast] results after merging: %v\n", result.Neighbors)
	return &result, nil
}

// AddBackpointer adds the from node to our backpointers, and possibly add the node to our
// routing table, if appropriate
func (local *TapestryNode) AddBackpointer(
	ctx context.Context,
	nodeMsg *pb.NodeMsg,
) (*pb.Ok, error) {
	id, err := ParseID(nodeMsg.Id)
	if err != nil {
		return nil, err
	}

	if local.Backpointers.Add(id) {
		local.log.Printf("Added backpointer %v\n", id)
	}
	local.AddRoute(id)

	ok := &pb.Ok{
		Ok: true,
	}
	return ok, nil
}

// RemoveBackpointer removes the from node from our backpointers
func (local *TapestryNode) RemoveBackpointer(
	ctx context.Context,
	nodeMsg *pb.NodeMsg,
) (*pb.Ok, error) {
	id, err := ParseID(nodeMsg.Id)
	if err != nil {
		return nil, err
	}

	if local.Backpointers.Remove(id) {
		local.log.Printf("Removed backpointer %v\n", id)
	}

	ok := &pb.Ok{
		Ok: true,
	}
	return ok, nil
}

// GetBackpointers gets all backpointers at the level specified, and possibly adds the node to our
// routing table, if appropriate
func (local *TapestryNode) GetBackpointers(
	ctx context.Context,
	backpointerReq *pb.BackpointerRequest,
) (*pb.Neighbors, error) {
	id, err := ParseID(backpointerReq.From)
	if err != nil {
		return nil, err
	}
	level := int(backpointerReq.Level)

	local.log.Printf("Sending level %v backpointers to %v\n", level, id)
	backpointers := local.Backpointers.Get(level)
	err = local.AddRoute(id)
	if err != nil {
		return nil, err
	}

	resp := &pb.Neighbors{
		Neighbors: idsToStringSlice(backpointers),
	}
	return resp, err
}

// RemoveBadNodes discards all the provided nodes
// - Remove each node from our routing table
// - Remove each node from our set of backpointers
func (local *TapestryNode) RemoveBadNodes(
	ctx context.Context,
	neighbors *pb.Neighbors,
) (*pb.Ok, error) {
	// local.log.Println("[RemoveBadNodes] Removing bad nodes, call on", local.Id)
	badnodes, err := stringSliceToIds(neighbors.Neighbors)
	if err != nil {
		return nil, err
	}

	for _, badnode := range badnodes {
		if local.Table.Remove(badnode) {
			local.log.Printf("Removed bad node %v\n", badnode)
		}
		if local.Backpointers.Remove(badnode) {
			local.log.Printf("Removed bad node backpointer %v\n", badnode)
		}
	}

	resp := &pb.Ok{
		Ok: true,
	}
	return resp, nil
}

// Utility function that adds a node to our routing table.
//
// - Adds the provided node to the routing table, if appropriate.
// - If the node was added to the routing table, notify the node of a backpointer
// - If an old node was removed from the routing table, notify the old node of a removed backpointer
func (local *TapestryNode) AddRoute(remoteNodeId ID) error {
	ok, prevNode := local.Table.Add(remoteNodeId)
	if ok {
		// wrap AddBackpointer into go routine since it has RPC calls
		go func(remoteNodeId ID) {
			remoteConn := local.Node.PeerConns[local.RetrieveID(remoteNodeId)]
			remoteNode := pb.NewTapestryRPCClient(remoteConn)
			_, err := remoteNode.AddBackpointer(context.Background(), &pb.NodeMsg{Id: local.Id.String()})
			if err != nil {
				// local.log.Printf("remotenode %v failed to add back pointer: %v. Removing remotenode.", remoteNodeId, err)
				local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{remoteNodeId.String()}})
			}
		}(remoteNodeId)

		if prevNode != nil {
			local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{prevNode.String()}})

			go func(prevNode *ID) {
				prevConn := local.Node.PeerConns[local.RetrieveID(*prevNode)]
				prevObjNode := pb.NewTapestryRPCClient(prevConn)
				_, err := prevObjNode.RemoveBackpointer(context.Background(), &pb.NodeMsg{Id: local.Id.String()})
				if err != nil {
					local.log.Printf("failed to remove back pointer in removed node, reason %v", err)
				}
			}(prevNode)
		}
	}
	return nil // by now, addroute is not returning any error from BP operations
}
