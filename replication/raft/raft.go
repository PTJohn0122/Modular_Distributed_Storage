package raft

import (
	"bytes"
	"context"
	json "encoding/json"
	"errors"
	"fmt"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sync"
	"time"
)

// RETRIES is the number of retries upon failure. By default, we have 3
const RETRIES = 3

// RETRY_TIME is the amount of time to wait between retries
const RETRY_TIME = 500 * time.Millisecond // used to be 100

type State struct {
	// In-memory key value store
	store map[string]string
	mu    sync.RWMutex

	// Channels given back by the underlying Raft implementation
	proposeC chan<- []byte
	commitC  <-chan *commit

	// Channel to passed the committed data back to replicateKey
	dataC chan []byte

	// Observability
	log *log.Logger

	// The public-facing API that this replicator must implement
	pb.ReplicatorServer
}

type Args struct {
	Node   *node.Node
	Config *Config
}

type KV struct {
	Key   string
	Value string
}

// Configure is called by the orchestrator to start this node
//
// The "args" are any to support any replicator that might need arbitrary
// set of configuration values.
//
// You are free to add any fields to the State struct that you feel may help
// with implementing the ReplicateKey or GetReplicatedKey functions. Likewise,
// you may call any additional functions following the second TODO. Apart
// from the two TODOs, DO NOT EDIT any other part of this function.
func Configure(args any) *State {
	a := args.(Args)

	node := a.Node
	config := a.Config

	proposeC := make(chan []byte)
	commitC := NewRaftNode(node, config, proposeC)

	s := &State{
		store:    make(map[string]string),
		proposeC: proposeC,
		commitC:  commitC,

		log: node.Log,

		// TODO(students): [Raft] Initialize any additional fields and add to State struct
		dataC: make(chan []byte),
	}

	// We registered RaftRPCServer when calling NewRaftNode above so we only need to
	// register ReplicatorServer here
	s.log.Printf("starting gRPC server at %s", node.Addr.Host)
	grpcServer := node.GrpcServer
	pb.RegisterReplicatorServer(grpcServer, s)
	go grpcServer.Serve(node.Listener)

	// TODO(students): [Raft] Call helper functions if needed
	go func() {
		for data := range commitC {
			var committedKV KV
			err := json.Unmarshal(*data, &committedKV)
			if err != nil {
				s.log.Printf("unmarshaling err: %s", err)
				continue
			}
			s.log.Printf("unmarshaled KV from commitC: %v\n", committedKV)
			s.mu.Lock()
			s.store[committedKV.Key] = committedKV.Value
			s.mu.Unlock()
			fmt.Printf("=  =  =  =  =  =  =  =  =  = current state machine store is %v =  =  =  =  =  = \n", s.store)
			s.dataC <- *data
		}
	}()

	// for {
	// 	select {
	// 	case data, ok := <-commitC:
	// 		if ok != true { // when commitC is closed
	// 			return s
	// 		}
	// 		var committedKV KV
	// 		err := json.Unmarshal(*data, &committedKV)
	// 		if err != nil {
	// 			s.log.Printf("unmarshaling err: %s", err)
	// 			continue
	// 		}
	// 		s.log.Printf("unmarshaled KV from commitC: %v\n", committedKV)
	// 		s.mu.Lock()
	// 		s.store[committedKV.Key] = committedKV.Value
	// 		s.mu.Unlock()
	// 		s.dataC <- *data
	// 		continue
	// 	default:
	// 	}
	// }

	return s
}

// ReplicateKey replicates the (key, value) given in the PutRequest by relaying it to the
// underlying Raft cluster/implementation.
//
// You should first marshal the KV struct using the encoding/json library. Then, put the
// marshalled struct in the proposeC channel and wait until the Raft cluster has confirmed that
// it has been committed. Think about how you can use the commitC channel to find out when
// something has been committed.
//
// If the proposal has not been committed after RETRY_TIME, you should retry it. You should retry
// the proposal a maximum of RETRIES times, at which point you can return a nil reply and an error.
func (s *State) ReplicateKey(ctx context.Context, r *pb.PutRequest) (*pb.PutReply, error) {
	marshaledKV, err := json.Marshal(KV{r.GetKey(), r.GetValue()})
	if err != nil {
		return nil, err
	}

	retryCount := 0
	retryTimer := time.NewTimer(RETRY_TIME)
	defer retryTimer.Stop()

	s.proposeC <- marshaledKV
	// s.log.Printf("[Replicate %v]: {%v} sent to proposeC\n", r, marshaledKV)
	fmt.Printf("[Replicate %v]: {%v} sent to proposeC\n", r, marshaledKV)
	for {
		select {
		case <-retryTimer.C:
			if retryCount == RETRIES {
				return nil, errors.New("maximum retries reached. proposal not committed")
			}
			retryCount++
			retryTimer.Stop()
			retryTimer.Reset(RETRY_TIME)
			s.proposeC <- marshaledKV
			s.log.Printf("timed out. {%v} resent to proposeC\n", marshaledKV)

		case data := <-s.dataC:
			s.log.Printf("[Replicate %v]: read from dataC: %v\n", r, data)
			if bytes.Equal(data, marshaledKV) {
				s.log.Printf("[Replicate %v]: data matched. returning\n", r)
				return &pb.PutReply{Clock: nil}, nil
			}
		}
	}
}

// GetReplicatedKey reads the given key from s.store. The implementation of
// this method should be pretty short (roughly 8-12 lines).
func (s *State) GetReplicatedKey(ctx context.Context, r *pb.GetRequest) (*pb.GetReply, error) {
	s.mu.Lock()
	val := s.store[r.GetKey()]
	s.mu.Unlock()

	return &pb.GetReply{
		Value: val,
		Clock: nil,
	}, nil
}
