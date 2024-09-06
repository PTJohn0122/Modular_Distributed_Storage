package raft

import (
	"context"
	"errors"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sync"
	"time"
)

// NodeState represents one of four possible states a Raft node can be in.
type NodeState uint64

// Enum for node states
const (
	FollowerState NodeState = iota
	CandidateState
	LeaderState
	ExitState
)

// string representation of node states
var nodeStateMap = [...]string{
	"FollowerState",
	"CandidateState",
	"LeaderState",
	"ExitState",
}

func (state NodeState) String() string {
	return nodeStateMap[state]
}

// commit represents a single log entry
type commit []byte

// None is a placeholder Node ID used when there is no leader
const None uint64 = 0

type RaftNode struct {
	// Node that this raft node is part of
	node *node.Node

	proposeC chan []byte    // Proposed messages (k, v)
	commitC  chan<- *commit // Entries committed to log (k, v)		// Note: chan<- indicates write-only
	stopC    chan struct{}  // Signals that client has closed proposal channel

	// stable store (written to disk, use helper methods)
	stableStore StableStore

	// volatile state on all servers
	state       NodeState
	commitIndex uint64
	lastApplied uint64

	leader uint64 // ID of leader node (set to 0 upon initialization)

	// leader specific volatile state
	nextIndex  map[uint64]uint64 // node ID -> index of next log entry to be sent to server
	matchIndex map[uint64]uint64 // node ID -> index of highest log entry replicated on server
	leaderMu   sync.Mutex

	// channels to send / receive various RPC messages (used in state functions)
	appendEntriesC chan AppendEntriesMsg
	requestVoteC   chan RequestVoteMsg

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	// Observability
	log *log.Logger

	// These functions are the internal, private RPCs for a node using Raft for replication
	pb.RaftRPCServer
}

// NewRaftNode constructs a new raft node and returns a committed log entry channel and
// error channel. Proposals for log updates are sent over the provided proposal channel.
// To shutdown, close proposeC.
func NewRaftNode(
	node *node.Node,
	c *Config,
	proposeC chan []byte,
) <-chan *commit {

	commitC := make(chan *commit)

	// Start the raft node
	StartRaftNode(node, c, proposeC, commitC)
	return commitC
}

// StartRaftNode returns a new RaftNode given a Node, a configuration,
// and the necessary channels. All log entries are replayed over the commit channel
// before new log entries are accepted.
func StartRaftNode(
	node *node.Node,
	c *Config,
	proposeC chan []byte,
	commitC chan *commit,
) *RaftNode {

	rn := SetupRaftNode(node, c, proposeC, commitC)
	rn.InitStableStore()

	// Replay any existing log entries over the commit channel
	for i := uint64(0); i <= rn.LastLogIndex(); i++ {
		data := commit(rn.GetLog(i).GetData())
		if data != nil {
			rn.log.Printf("replaying entry #%d", i)
			commitC <- &data
		}
	}

	go rn.Run(context.Background(), rn.doFollower)
	return rn
}

// SetupRaftNode is separated from StartRaftNode for testing purposes.
// It allows us to start the node in any state we want
func SetupRaftNode(
	node *node.Node,
	c *Config,
	proposeC chan []byte,
	commitC chan *commit,
) *RaftNode {

	rn := &RaftNode{
		node:             node,
		proposeC:         proposeC,
		commitC:          commitC,
		stopC:            make(chan struct{}),
		stableStore:      c.Storage,
		commitIndex:      0,
		lastApplied:      0,
		state:            FollowerState,
		nextIndex:        make(map[uint64]uint64),
		matchIndex:       make(map[uint64]uint64),
		appendEntriesC:   make(chan AppendEntriesMsg),
		requestVoteC:     make(chan RequestVoteMsg),
		electionTimeout:  c.ElectionTimeout,
		heartbeatTimeout: c.HeartbeatTimeout,
		log:              node.Log,
	}

	grpcServer := rn.node.GrpcServer
	pb.RegisterRaftRPCServer(grpcServer, rn)

	return rn
}

// stateFunction is a function defined on a Raft node, that while executing,
// handles the logic of the current state. When the time comes to transition to
// another state, the function returns the next function to execute.
type stateFunction func() stateFunction

// Run calls the appropriate state function for the RaftNode
func (rn *RaftNode) Run(ctx context.Context, curr stateFunction) {
	for curr != nil {
		select {
		case <-ctx.Done():
			curr = nil
		default:
			curr = curr()
		}
	}
}

func (rn *RaftNode) Stop() {
	rn.log.Printf("shutting down node")

	close(rn.commitC)

	if state := rn.state; !(state == ExitState) {
		rn.stopC <- struct{}{}
	}
	rn.state = ExitState
	rn.stableStore.Close()
}

var ErrShutdown = errors.New("node has shutdown")

// Propose is invoked on a leader node by a remote follower node to
// forward a client's proposal to the leader
func (rn *RaftNode) Propose(
	ctx context.Context,
	req *pb.ProposalRequest,
) (*pb.ProposalReply, error) {

	rn.log.Printf("received ProposalRequest from node %v", req.From)
	rn.proposeC <- req.Data

	return &pb.ProposalReply{}, nil
}

// AppendEntriesMsg is used for notifying candidates of a new leader and transferring logs
type AppendEntriesMsg struct {
	request *pb.AppendEntriesRequest
	reply   chan pb.AppendEntriesReply
}

// AppendEntries is invoked on us by a remote node, and sends the request and a
// reply channel to the stateFunction.
func (rn *RaftNode) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesReply, error) {

	// Uncomment below line to print out all AppendEntriesRequests
	// rn.log.Printf("received AppendEntriesRequest from node %v", req.From)

	// Ensures that goroutine is not blocking on sending msg to reply channel
	reply := make(chan pb.AppendEntriesReply, 1)
	rn.appendEntriesC <- AppendEntriesMsg{req, reply}
	select {
	case <-ctx.Done():
		return nil, ErrShutdown
	case msg := <-reply:
		return &msg, nil
	}
}

// RequestVoteMsg is used for raft elections
type RequestVoteMsg struct {
	request *pb.RequestVoteRequest
	reply   chan pb.RequestVoteReply
}

// RequestVote is invoked on us by a remote node, and sends the request and a
// reply channel to the stateFunction.
func (rn *RaftNode) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteReply, error) {

	// Uncomment below line to print out all RequestVoteRequests
	// rn.log.Printf("received RequestVoteRequest from node %v", req.From)

	reply := make(chan pb.RequestVoteReply, 1)
	rn.requestVoteC <- RequestVoteMsg{req, reply}
	select {
	case <-ctx.Done():
		return nil, ErrShutdown
	case msg := <-reply:
		return &msg, nil
	}
}
