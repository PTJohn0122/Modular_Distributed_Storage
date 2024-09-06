package leaderless

import (
	"context"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"modist/replication/conflict"
	"modist/store"
	"strconv"
	"testing"
	"time"
)

func testCreatePhysicalClockArgs(node *node.Node, w, r int) Args[conflict.PhysicalClock] {
	a := Args[conflict.PhysicalClock]{
		Node:             node,
		W:                w,
		R:                r,
		ConflictResolver: &conflict.PhysicalClockConflictResolver{},
		LocalStore:       &store.Memory[*conflict.KV[conflict.PhysicalClock]]{},
	}
	return a
}

// Test that a cluster with 3 nodes can replicate a key and let us read it back
func TestBasicLeaderless(t *testing.T) {
	nodes := node.Create([]string{"localhost:1234", "localhost:1235", "localhost:1236"})
	var replicators []*State[conflict.PhysicalClock]

	for _, node := range nodes {
		replicator := Configure[conflict.PhysicalClock](
			testCreatePhysicalClockArgs(node, 2, 2),
		)
		replicators = append(replicators, replicator)
	}

	key := "foo"
	value := "bar"

	firstReplicator := replicators[0]

	response, err := firstReplicator.ReplicateKey(context.Background(), &pb.PutRequest{
		Key: key, Value: value, Clock: &pb.Clock{Timestamp: 1}})
	if err != nil {
		t.Fatalf("Error while replicating key to node 0: %v", err)
	}

	log.Printf("response clock is %v", response.GetClock())
	kv, err := firstReplicator.GetReplicatedKey(context.Background(),
		&pb.GetRequest{Key: key, Metadata: &pb.GetMetadata{Clock: response.GetClock()}})
	if err != nil {
		t.Fatalf("Error while getting key from node 1: %v", err)
	}

	if kv.GetValue() != value {
		t.Fatalf("Value mismatch: expected %v, got %v", value, kv.GetValue())
	}
}

// Read repair brings nodes that have falled behind up-to-date when we do reads. To test that read
// repair works, we can intentionally make a node x fall behind (partition it), do quorum reads
// involving x, and then read only from x. We then make sure that x is as up-to-date as any other
// replica.
func TestBasicReadRepair(t *testing.T) {
	nodes := node.Create([]string{"localhost:4001", "localhost:4002", "localhost:4003"})
	var replicators []*State[conflict.PhysicalClock]

	for _, node := range nodes {
		replicator := Configure[conflict.PhysicalClock](testCreatePhysicalClockArgs(node, 2, 2))
		replicators = append(replicators, replicator)
	}

	// make replicators[2] fall behind by never including it in any writes
	// NOTE: This can also be done using node.StartPartition(...)
	replicators[0].replicaChooser = func(numreplicas int, exclude []uint64) ([]uint64, error) {
		return []uint64{replicators[1].node.ID}, nil
	}

	replicators[1].replicaChooser = func(numreplicas int, exclude []uint64) ([]uint64, error) {
		return []uint64{replicators[0].node.ID}, nil
	}

	// write lots of data to replicators[0] and replicators[1]
	numItrs := 10
	for i := 0; i < numItrs; i++ {
		pr := &pb.PutRequest{Key: strconv.Itoa(i), Value: strconv.Itoa(numItrs - i), Clock: &pb.Clock{Timestamp: 1}}
		replicators[i%2].ReplicateKey(context.Background(), pr)
	}

	// force the reads from replicators[0] or [1] to include replicators[2]
	replicators[1].replicaChooser = func(numReplicas int, exclude []uint64) ([]uint64, error) {
		return []uint64{replicators[2].node.ID}, nil
	}

	for i := 0; i < numItrs; i++ {
		log.Printf("requesting key")
		replicators[1].GetReplicatedKey(context.Background(),
			&pb.GetRequest{Key: strconv.Itoa(i), Metadata: &pb.GetMetadata{Clock: &pb.Clock{Timestamp: 1}}})
	}

	// finally, ensure that replicators[2] has every key itself (by setting r = 1)
	for i := 0; i < numItrs; i++ {
		got, ok := replicators[2].localStore.Get(strconv.Itoa(i))
		expected := strconv.Itoa(numItrs - i)

		if !ok {
			t.Errorf("key %d not found in local store", i)
		} else if got.Value != expected {
			t.Errorf("value mismatch: expected %s for key %d, got %s", expected, i, got)
		}
	}
}

func TestHandlePeerWrite(t *testing.T) {
	nodes := node.Create([]string{"localhost:1250"})
	a := testCreatePhysicalClockArgs(nodes[0], 1, 1)
	s := &State[conflict.PhysicalClock]{
		node: a.Node,

		localStore:       a.LocalStore,
		conflictResolver: a.ConflictResolver,

		W: a.W,
		R: a.R,

		log: a.Node.Log,
	}
	s.replicaChooser = s.selectKRandomPeerNodeIDs
	s.onReplicatorStart = func(node *node.Node) {
		s.conflictResolver.ReplicatorDidStart(node)
	}
	s.onMessageReceived = func(clock conflict.PhysicalClock) {
		s.conflictResolver.OnMessageReceive(clock)
	}
	s.onMessageSend = func() {
		s.conflictResolver.OnMessageSend()
	}
	s.onReplicatorStart(s.node)

	fstClock := s.conflictResolver.NewClock()
	time.Sleep(2 * time.Second)
	sndClock := s.conflictResolver.NewClock()
	key := "foo"
	kv := conflict.KVFromParts[conflict.PhysicalClock](key, "bar", fstClock)

	// 1st case: no entry in localStore
	rep, _ := s.HandlePeerWrite(context.Background(), kv.Proto())
	if !rep.GetAccepted() {
		t.Errorf("[HandlePeerWrite] new entry should be accepted")
	}

	// 2nd case: concurrent update
	kv2 := conflict.KVFromParts[conflict.PhysicalClock](key, "dar", fstClock)
	rep, _ = s.HandlePeerWrite(context.Background(), kv2.Proto())
	if !rep.GetAccepted() {
		t.Errorf("[HandlePeerWrite] concurrent update with larger lexically value should be accepted")
	}

	// 4th case: outdated update
	kv4 := conflict.KVFromParts[conflict.PhysicalClock](key, "bar", s.conflictResolver.ZeroClock())
	rep, _ = s.HandlePeerWrite(context.Background(), kv4.Proto())
	if rep.GetAccepted() {
		t.Errorf("[HandlePeerWrite] outdated update should not be accepted")
	}
	if !kv2.Equals(conflict.KVFromProto[conflict.PhysicalClock](rep.GetResolvableKv())) {
		t.Errorf("[HandlePeerWrite] wrong kv returned")
	}

	// 3rd case: newer update
	kv3 := conflict.KVFromParts[conflict.PhysicalClock](key, "bar", sndClock)
	rep, _ = s.HandlePeerWrite(context.Background(), kv3.Proto())
	if !rep.GetAccepted() {
		t.Errorf("[HandlePeerWrite] new update should be accepted")
	}

	// 4th case again (to check the newest kv): outdated update
	kv4 = conflict.KVFromParts[conflict.PhysicalClock](key, "bar", fstClock)
	rep, _ = s.HandlePeerWrite(context.Background(), kv4.Proto())
	if rep.GetAccepted() {
		t.Errorf("[HandlePeerWrite] outdated update should not be accepted")
	}
	if !kv3.Equals(conflict.KVFromProto[conflict.PhysicalClock](rep.GetResolvableKv())) {
		t.Errorf("[HandlePeerWrite] wrong kv returned")
	}
}

// func TestReplicateKey(t *testing.T) {
// 	nodes := node.Create([]string{"localhost:1237", "localhost:1238", "localhost:1239"})
// 	var replicators []*State[conflict.PhysicalClock]

// 	for _, node := range nodes {
// 		replicator := Configure[conflict.PhysicalClock](
// 			testCreatePhysicalClockArgs(node, 2, 2),
// 		)
// 		replicators = append(replicators, replicator)
// 	}

// 	func (s *State[T]) testReplicaChooser(k int, exclude []uint64) ([]uint64, error) {
// 		return [], nil
// 	}

// 	key := "foo"
// 	value := "bar"

// 	firstReplicator := replicators[0]

// 	response, err := firstReplicator.ReplicateKey(context.Background(), &pb.PutRequest{
// 		Key: key, Value: value, Clock: &pb.Clock{Timestamp: 1}})
// 	if err != nil {
// 		t.Fatalf("Error while replicating key to node 0: %v", err)
// 	}

// 	log.Printf("response clock is %v", response.GetClock())
// }

// TODO: test to write:
// override replica chooser
// unconnect some chosen replicas, observe dispatchToPeers giving up
// reconnect, redo
