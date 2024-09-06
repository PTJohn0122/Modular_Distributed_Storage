package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"sync"

	"google.golang.org/grpc"
)

// A node in our system
type Node struct {
	// Unique ID of this node in any modist cluster
	ID uint64

	// The address of this node within the cluster. Can be localhost or a different IP on the
	// network.
	Addr *url.URL

	// The gRPC server that every process on this machine uses
	GrpcServer *grpc.Server
	// The listener on which the gRPC server should be placed
	Listener net.Listener

	// All the other nodes that this ModistNode will try talking to
	PeerNodes map[uint64]*Node
	// The gRPC connections to every other node in PeerNodes
	PeerConns map[uint64]*grpc.ClientConn

	// If part of a simulated network partition, which nodes that this node is allowed to
	// talk to.
	//
	// During testing, we'd like to be able to specify which _groups_ of nodes can communicate
	// with each other (that's easier to write out than what nodes can't talk to each other).
	// An allow-list is easier to reason about than a block-list when programming in this way.
	partitionAllowList map[uint64]struct{}
	partitionAllowMu   sync.RWMutex

	// Observability
	Log *log.Logger
}

// Imagine we're now the orchestrator setting up a bunch of nodes
func Create(addrs []string) []*Node {
	var nodes []*Node

	for _, addr := range addrs {
		nodeID := ID()
		u := &url.URL{Host: addr}

		// Create the gRPC server for this node
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("Node at addr %s failed to start gRPC server: %v", addr, err)
		}

		// Can't start the server here because we need to first register the servers
		var opts []grpc.ServerOption
		grpcServer := grpc.NewServer(opts...)

		logger := log.New(
			os.Stdout,
			fmt.Sprintf("Node %d: ", nodeID),
			log.Lshortfile|log.Lmicroseconds,
		)

		node := &Node{
			ID:   nodeID,
			Addr: u,

			GrpcServer: grpcServer,
			Listener:   lis,

			// We populate these in the next loop
			partitionAllowList: map[uint64]struct{}{},
			PeerNodes:          map[uint64]*Node{},
			PeerConns:          map[uint64]*grpc.ClientConn{},

			Log: logger,
		}

		nodes = append(nodes, node)
	}

	// Populate the peer connections now that all the servers have been setup
	for _, node := range nodes {
		for _, peerNode := range nodes {
			node.PeerNodes[peerNode.ID] = peerNode

			// Since we want a closure over the *value* of node and peerNode within
			// partitionInterceptor, we have to reassign these in here.
			//
			// https://stackoverflow.com/a/26694016
			node := node
			peerNode := peerNode

			partitionInterceptor := func(
				ctx context.Context,
				method string,
				req interface{},
				reply interface{},
				cc *grpc.ClientConn,
				invoker grpc.UnaryInvoker,
				opts ...grpc.CallOption,
			) error {
				canCommunicateWith := node.canCommunicate(peerNode)

				if !canCommunicateWith {
					return fmt.Errorf(
						"node %d cannot send message to node %d because it is partitioned off",
						node.ID,
						peerNode.ID,
					)
				}

				return invoker(ctx, method, req, reply, cc, opts...)
			}

			log.Printf("Node %d creating client connection to host %s", node.ID, peerNode.Addr.Host)
			conn, err := grpc.Dial(
				peerNode.Addr.Host,
				grpc.WithInsecure(),
				grpc.WithUnaryInterceptor(partitionInterceptor),
			)
			if err != nil {
				log.Fatalf("Could not establish conn from %d to %d: %v", node.ID, peerNode.ID, err)
			}

			node.PeerConns[peerNode.ID] = conn
		}
	}

	return nodes
}

// For testing purposes, we can set up the nodes with specific IDs rather than
// random ones. Ideally, we would have one function for this but that would require
// changing all of the places where Create is called.
func CreateWithIds(addrs []string, ids []uint64) []*Node {
	if len(addrs) != len(ids) {
		log.Fatalf(
			"CreateWithIds: length of addrs and ids must match; %v addrs provided and %v ids provided",
			len(addrs),
			len(ids),
		)
	}
	var nodes []*Node

	for i, addr := range addrs {
		nodeID := ids[i]
		u := &url.URL{Host: addr}

		// Create the gRPC server for this node
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("Node at addr %s failed to start gRPC server: %v", addr, err)
		}

		// Can't start the server here because we need to first register the servers
		var opts []grpc.ServerOption
		grpcServer := grpc.NewServer(opts...)

		logger := log.New(
			os.Stdout,
			fmt.Sprintf("Node %d: ", nodeID),
			log.Lshortfile|log.Lmicroseconds,
		)

		node := &Node{
			ID:   nodeID,
			Addr: u,

			GrpcServer: grpcServer,
			Listener:   lis,

			// We populate these in the next loop
			partitionAllowList: map[uint64]struct{}{},
			PeerNodes:          map[uint64]*Node{},
			PeerConns:          map[uint64]*grpc.ClientConn{},

			Log: logger,
		}

		nodes = append(nodes, node)
	}

	// Populate the peer connections now that all the servers have been setup
	for _, node := range nodes {
		for _, peerNode := range nodes {
			node.PeerNodes[peerNode.ID] = peerNode

			// Since we want a closure over the *value* of node and peerNode within
			// partitionInterceptor, we have to reassign these in here.
			//
			// https://stackoverflow.com/a/26694016
			node := node
			peerNode := peerNode

			partitionInterceptor := func(
				ctx context.Context,
				method string,
				req interface{},
				reply interface{},
				cc *grpc.ClientConn,
				invoker grpc.UnaryInvoker,
				opts ...grpc.CallOption,
			) error {
				canCommunicateWith := node.canCommunicate(peerNode)

				if !canCommunicateWith {
					return fmt.Errorf(
						"node %d cannot send message to node %d because it is partitioned off",
						node.ID,
						peerNode.ID,
					)
				}

				return invoker(ctx, method, req, reply, cc, opts...)
			}

			log.Printf("Node %d creating client connection to host %s", node.ID, peerNode.Addr.Host)
			conn, err := grpc.Dial(
				peerNode.Addr.Host,
				grpc.WithInsecure(),
				grpc.WithUnaryInterceptor(partitionInterceptor),
			)
			if err != nil {
				log.Fatalf("Could not establish conn from %d to %d: %v", node.ID, peerNode.ID, err)
			}

			node.PeerConns[peerNode.ID] = conn
		}
	}

	return nodes
}
