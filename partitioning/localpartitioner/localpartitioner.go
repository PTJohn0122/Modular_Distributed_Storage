package localpartitioner

import (
	"context"
	"modist/orchestrator/node"
	"modist/partitioning"
	pb "modist/proto"
	"sync"
)

type State struct {
	node     *node.Node
	assigner partitioning.Assigner

	// Delay responses when performing reconfigurations
	mu sync.RWMutex

	pb.PartitionerServer
}

type Args struct {
	Node     *node.Node
	Assigner partitioning.Assigner
}

func Configure(args any) pb.PartitionerServer {
	a := args.(Args)

	s := &State{
		node:     a.Node,
		assigner: a.Assigner,
	}

	grpcServer := a.Node.GrpcServer
	pb.RegisterPartitionerServer(grpcServer, s)

	go func() {
		err := grpcServer.Serve(a.Node.Listener)
		if err != nil {
			a.Node.Log.Printf("Unable to serve a single partitioner server: %v", err)
		}
	}()

	return s
}

func (s *State) Lookup(ctx context.Context, r *pb.PartitionLookupRequest) (*pb.PartitionLookupReply, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, rewrittenKey, err := s.assigner.Lookup(r.Key)
	if err != nil {
		return nil, err
	}

	resp := &pb.PartitionLookupReply{
		RewrittenKey:   rewrittenKey,
		ReplicaGroupId: id,
	}
	return resp, nil
}

func (s *State) RegisterReplicaGroup(ctx context.Context, r *pb.RegisterReplicaGroupRequest) (*pb.RegisterReplicaGroupReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Repartition as necessary and inform all replicas
	reassignments := s.assigner.AddReplicaGroup(r.Id)

	_ = reassignments

	return &pb.RegisterReplicaGroupReply{}, nil
}

func (s *State) UnregisterReplicaGroup(ctx context.Context, r *pb.UnregisterReplicaGroupRequest) (*pb.UnregisterReplicaGroupReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Repartition as necessary and inform all replicas
	reassignments := s.assigner.RemoveReplicaGroup(r.Id)

	_ = reassignments

	return &pb.UnregisterReplicaGroupReply{}, nil
}
