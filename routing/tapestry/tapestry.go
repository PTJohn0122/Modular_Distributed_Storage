package tapestry

import (
	"context"
	"fmt"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"strings"
	"sync"
)

type State struct {
	tapestryNode *TapestryNode
	mu           sync.Mutex

	// Observability
	log *log.Logger

	// The public-facing API that this router must implement
	pb.RouterServer
}

type Args struct {
	Node      *node.Node
	Join      bool   // True if we are connecting to an existing Tapestry mesh
	ConnectTo uint64 // If we are connecting to an existing Tapestry mesh, the ID of the node to connect to
}

// Configure is called by the orchestrator to start this node
//
// The "args" are any to support any router that might need arbitrary
// set of configuration values.
func Configure(args any) *State {
	a := args.(Args)

	node := a.Node

	tn, err := StartTapestryNode(node, a.ConnectTo, a.Join)
	if err != nil {
		node.Log.Printf(err.Error())
		return nil
	}

	s := &State{
		tapestryNode: tn,
		log:          node.Log,
	}

	// We registered TapestryRPCServer when calling NewTapestryNode above so we only need to
	// register RouterServer here
	s.log.Printf("starting gRPC server at %s", a.Node.Addr.Host)
	grpcServer := a.Node.GrpcServer
	pb.RegisterRouterServer(grpcServer, s)
	go grpcServer.Serve(a.Node.Listener)

	return s
}

func (s *State) Lookup(
	ctx context.Context,
	r *pb.RouteLookupRequest,
) (*pb.RouteLookupReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Note: Separated by comma
	addr, err := s.tapestryNode.Get(fmt.Sprint(r.Id))
	if err != nil {
		return nil, err
	}

	addrs := strings.Split(string(addr), ",")
	fmt.Println(addrs)

	resp := &pb.RouteLookupReply{
		Addrs: addrs,
	}
	return resp, nil
}

func (s *State) Publish(
	ctx context.Context,
	r *pb.RoutePublishRequest,
) (*pb.RoutePublishReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	addr, err := s.tapestryNode.Get(fmt.Sprint(r.Id))
	if err != nil {
		addr = []byte{}
	}

	if len(addr) > 0 {
		addr = append(addr, ',')
	}

	addr = append(addr, []byte(r.Addr)...)
	err = s.tapestryNode.Store(fmt.Sprint(r.Id), []byte(addr))
	if err != nil {
		return nil, err
	}
	return &pb.RoutePublishReply{}, nil
}

func (s *State) Unpublish(
	ctx context.Context,
	r *pb.RouteUnpublishRequest,
) (*pb.RouteUnpublishReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	addr, err := s.tapestryNode.Get(fmt.Sprint(r.Id))
	if err != nil {
		return nil, err
	}

	addrs := strings.Split(fmt.Sprint(addr), ",")
	for i, a := range addrs {
		if a == r.Addr {
			addrs = append(addrs[:i], addrs[i+1:]...)
			break
		}
	}

	err = s.tapestryNode.Store(fmt.Sprint(r.Id), []byte(strings.Join(addrs, ",")))
	if err != nil {
		return nil, err
	}
	return &pb.RouteUnpublishReply{}, nil
}
