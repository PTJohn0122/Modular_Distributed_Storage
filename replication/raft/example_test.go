package raft

import (
	"context"
	"fmt"
	"math/rand"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/runtime/protoimpl"
)

// ------------------------------------ HELPER FUNCTIONS ------------------------------------

type testMemoryStorageOptions func(*MemoryStore)

func newTestMemoryStorage(opts ...testMemoryStorageOptions) *MemoryStore {
	ms := NewMemoryStore()
	for _, o := range opts {
		o(ms)
	}
	return ms
}

func newTestConfig(election, heartbeat time.Duration, storage StableStore) *Config {
	return &Config{
		ElectionTimeout:  election,
		HeartbeatTimeout: heartbeat,
		Storage:          storage,
	}
}

type RaftNodeInfo struct {
	raftNode *RaftNode
	commitC  <-chan *commit
}

func newTestRaftNode(
	node *node.Node,
	election, heartbeat time.Duration,
	storage StableStore,
) *RaftNodeInfo {

	cfg := newTestConfig(election, heartbeat, storage)
	proposeC := make(chan []byte)
	commitC := make(chan *commit, 10)

	rn := SetupRaftNode(node, cfg, proposeC, commitC)
	rn.InitStableStore()

	// Normally, we start the grpcServer in the Configure function
	grpcServer := rn.node.GrpcServer
	go grpcServer.Serve(rn.node.Listener)

	// Uncomment the line below to discard the logs for each raftNode
	// rn.log = log.New(io.Discard, "", 0)

	return &RaftNodeInfo{rn, commitC}
}

// Cannot reuse port numbers so we'll use a global variable to keep track
var port = 8000

func generateRaftAddrs(k int) []string {
	var peers []string
	for i := 0; i < k; i++ {
		peers = append(peers, fmt.Sprintf("localhost:%d", port))
		port++
	}
	return peers
}

// Creates the raft cluster but doesn't start any of the nodes yet
func createRaftCluster(
	clusterSize int,
	election, heartbeat time.Duration,
) []*RaftNodeInfo {

	addrs := generateRaftAddrs(clusterSize)
	nodes := node.Create(addrs)

	var cluster []*RaftNodeInfo
	for _, node := range nodes {
		rnInfo := newTestRaftNode(node, election, heartbeat, newTestMemoryStorage())
		cluster = append(cluster, rnInfo)
	}

	return cluster
}

// Starts the raft cluster with all nodes running doFollower function
func startRaftCluster(ctx context.Context, cluster []*RaftNodeInfo) {
	for _, rnInfo := range cluster {
		rn := rnInfo.raftNode
		go rn.Run(ctx, rn.doFollower)
	}
}

// We use cmpopts.EquateEmpty, which makes a nil map and an empty one
// equal, because other packages for deep equality comparisons behave this way.
var CompareOptsForProtos = []cmp.Option{cmpopts.IgnoreTypes(
	protoimpl.MessageState{},
	protoimpl.SizeCache(0),
	protoimpl.UnknownFields{},
),
	cmpopts.EquateEmpty(),
}

// Compare two objects for equality (works with protobuf also)
func DeepEqual(x, y interface{}) bool {
	return cmp.Equal(x, y, CompareOptsForProtos...)
}

const smallPause = 30 * time.Millisecond

type appendEntriesSlice []pb.AppendEntriesRequest

func (s appendEntriesSlice) Len() int           { return len(s) }
func (s appendEntriesSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s appendEntriesSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type requestVoteSlice []pb.RequestVoteRequest

func (s requestVoteSlice) Len() int           { return len(s) }
func (s requestVoteSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s requestVoteSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// ------------------------------------ UNIT TESTS ------------------------------------

// TestLeaderHeartbeat tests whether the leader sends a heartbeat with
// Term = 1, PrevLogIndex = 0, PrevLogTerm = 0, and empty/single entries to all followers.
// Reference: section 5.2
func TestLeaderHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster := createRaftCluster(
		3,
		time.Second*1,
		time.Second*1,
	)

	var msgs []pb.AppendEntriesRequest
	var appendMu sync.Mutex

	// We'll override the default follower behavior to collect all the appendEntries
	// messages sent by the leader
	fakeDoFollower := func(ctx context.Context, rn *RaftNode) {
		select {
		case msg := <-rn.appendEntriesC:
			appendMu.Lock()
			msgs = append(msgs, *msg.request)
			appendMu.Unlock()
		case <-ctx.Done():
			return
		}
	}

	// Start all nodes except first one as fake followers
	for i := 1; i < len(cluster); i++ {
		rn := cluster[i].raftNode
		go fakeDoFollower(ctx, rn)
	}

	// Start first node as leader
	leader := cluster[0].raftNode
	leader.SetCurrentTerm(leader.GetCurrentTerm() + 1)
	go leader.Run(ctx, leader.doLeader)

	// Sleep to allow time to reply to appendEntriesRequest
	time.Sleep(2 * smallPause)

	nodeIDs := []uint64{cluster[1].raftNode.node.ID, cluster[2].raftNode.node.ID}

	wmsgs1 := []pb.AppendEntriesRequest{
		{From: leader.node.ID, To: nodeIDs[0], Term: 1},
		{From: leader.node.ID, To: nodeIDs[1], Term: 1},
	}
	wents := []*pb.LogEntry{{Index: 1, Term: 1}}
	wmsgs2 := []pb.AppendEntriesRequest{
		{From: leader.node.ID, To: nodeIDs[0], Term: 1, Entries: wents},
		{From: leader.node.ID, To: nodeIDs[1], Term: 1, Entries: wents},
	}
	sort.Sort(appendEntriesSlice(msgs))
	sort.Sort(appendEntriesSlice(wmsgs1))
	sort.Sort(appendEntriesSlice(wmsgs2))
	if !DeepEqual(msgs, wmsgs1) && !DeepEqual(msgs, wmsgs2) {
		t.Errorf("AppendEntriesRequest msgs = %v, want %v or %v", msgs, wmsgs1, wmsgs2)
	}
}

// TestStateTransition tests whether a node has the expected term and leader ID when
// transitioning from every state to every allowed state
func TestStateTransition(t *testing.T) {
	// table-driven tests are the recommended way of testing the same functionality using different
	// combinations.
	// see https://go.dev/blog/subtests for more information
	tests := []struct {
		from   NodeState
		to     NodeState
		wallow bool
		wterm  uint64
		wlead  uint64
	}{
		{FollowerState, FollowerState, true, 1, None},
		{FollowerState, CandidateState, true, 1, None},

		{CandidateState, FollowerState, true, 0, None},
		{CandidateState, LeaderState, true, 0, 1},

		{LeaderState, FollowerState, true, 1, None},
		{LeaderState, LeaderState, true, 0, 1},
	}

	for i, tt := range tests {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if tt.wallow {
						t.Errorf("%d: allow = %v, want %v", i, false, true)
					}
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cluster := createRaftCluster(
				1,
				1*time.Second,
				100*time.Millisecond,
			)

			n1 := cluster[0].raftNode
			n1.state = tt.from

			switch tt.to {
			case FollowerState:
				n1.SetCurrentTerm(tt.wterm)
				n1.leader = tt.wlead
				go n1.Run(ctx, n1.doFollower)
			case CandidateState:
				go n1.doCandidate()
			case LeaderState:
				go n1.Run(ctx, n1.doLeader)
			}

			// Sleep to allow time for node to initialize
			time.Sleep(smallPause)

			// Modify wlead to be node's ID
			if tt.wlead != None {
				tt.wlead = n1.node.ID
			}

			if n1.GetCurrentTerm() != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, n1.GetCurrentTerm(), tt.wterm)
			}
			if n1.leader != tt.wlead {
				t.Errorf("#%d: lead = %d, want %d", i, n1.leader, tt.wlead)
			}
		})
	}
}

// ------------------------------------ INTEGRATION TESTS ------------------------------------

func replicateRandomly(t *testing.T, replicators []*State, key string, val string) {
	replicatorNum := rand.Intn(len(replicators))
	replicator := replicators[replicatorNum]
	_, err := replicator.ReplicateKey(context.Background(), &pb.PutRequest{
		Key: key, Value: val,
	})
	if err != nil {
		t.Fatalf("Error while replicating key %s to node %d: %s", key, replicatorNum, err)
	}
}

func getReplicatedKeyRandomly(t *testing.T, replicators []*State, key string) string {
	replicatorNum := rand.Intn(len(replicators))
	replicator := replicators[replicatorNum]
	val, err := replicator.GetReplicatedKey(context.Background(), &pb.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Error while getting key %s from node %d: %s", key, replicatorNum, err)
	}
	return val.GetValue()
}

// TestBasicStoreKV tests that a raft cluster is able to correctly store 300 different keys
// and then retrieve them
func TestBasicStoreKV(t *testing.T) {
	addrs := generateRaftAddrs(5)
	nodes := node.Create(addrs)

	et := time.Millisecond * 150
	ht := time.Millisecond * 50

	var replicators []*State
	for _, node := range nodes {
		config := &Config{
			ElectionTimeout:  et,
			HeartbeatTimeout: ht,
			Storage:          NewMemoryStore(),
		}

		replicator := Configure(Args{
			Node:   node,
			Config: config,
		})
		replicators = append(replicators, replicator)
	}

	// Allow leader to be elected
	time.Sleep(2 * et)

	store := make(map[string]string)

	for i := 0; i < 300; i++ {
		key := fmt.Sprint(i)
		val := fmt.Sprintf("val is %d", 7*i)

		store[key] = val
		replicateRandomly(t, replicators, key, val)
	}

	// Sleep to allow last log time to replicate
	time.Sleep(2 * ht)

	for i := 0; i < 300; i++ {
		key := fmt.Sprint(i)
		val := getReplicatedKeyRandomly(t, replicators, key)

		if val != store[key] {
			t.Errorf("val = %v, want %v", val, store[key])
		}
	}
}
