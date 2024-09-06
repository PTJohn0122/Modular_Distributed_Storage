package localrouter

import (
	"context"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sync"

	"golang.org/x/exp/slices"
)

type State struct {
	node *node.Node

	locs map[uint64][]string
	mu   sync.RWMutex

	// The public-facing API that this router must implement
	pb.RouterServer
}

type Args struct {
	Node *node.Node
}

func Configure(args any) pb.RouterServer {
	a := args.(Args)

	s := &State{
		locs: map[uint64][]string{},
		node: a.Node,
	}

	grpcServer := a.Node.GrpcServer
	pb.RegisterRouterServer(grpcServer, s)

	go func() {
		err := grpcServer.Serve(a.Node.Listener)
		if err != nil {
			a.Node.Log.Printf("Unable to serve a single routing server: %v", err)
		}
	}()

	return s
}

func (s *State) Lookup(ctx context.Context, r *pb.RouteLookupRequest) (*pb.RouteLookupReply, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := &pb.RouteLookupReply{
		Addrs: s.locs[r.Id],
	}
	return resp, nil
}

func (s *State) Publish(ctx context.Context, r *pb.RoutePublishRequest) (*pb.RoutePublishReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.locs[r.Id] = append(s.locs[r.Id], r.Addr)

	return &pb.RoutePublishReply{}, nil
}

func (s *State) Unpublish(ctx context.Context, r *pb.RouteUnpublishRequest) (*pb.RouteUnpublishReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := slices.Index(s.locs[r.Id], r.Addr)
	if index >= 0 {
		s.locs[r.Id] = slices.Delete(s.locs[r.Id], index, index+1)
	}

	return &pb.RouteUnpublishReply{}, nil
}
