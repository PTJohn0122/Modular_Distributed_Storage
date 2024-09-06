package conflict

import (
	"errors"
	"fmt"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sync"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

// VersionVectorClock is the Clock that we use to implement causal consistency.
type VersionVectorClock struct {
	// Map from node ID to the associated counter. If a node ID isn't in the map, then its counter
	// is considered to be 0 (we don't automatically populate node IDs to save memory).
	vector map[uint64]uint64
}

// NewVersionVectorClock returns an initialized VersionVectorClock.
func NewVersionVectorClock() VersionVectorClock {
	return VersionVectorClock{vector: make(map[uint64]uint64)}
}

// Proto converts a VersionVectorClock into a clock that can be sent in an RPC.
func (v VersionVectorClock) Proto() *pb.Clock {
	p := &pb.Clock{
		Vector: v.vector,
	}
	return p
}

func (v VersionVectorClock) String() string {
	return fmt.Sprintf("%v", v.vector)
}

func (v VersionVectorClock) Equals(other Clock) bool {
	otherVector := other.(VersionVectorClock)
	return maps.Equal(v.vector, otherVector.vector)
}

// HappensBefore returns whether v happens before other. With version vectors, this happens when
// two conditions are met:
//   - For every nodeID in v, other has a counter greater than or equal to v's counter for that
//     node
//   - The vectors are not equal.
//
// Remember that nodeIDs that are not in a vector have an implicit counter of 0.
func (v VersionVectorClock) HappensBefore(other Clock) bool {
	otherVector := other.(VersionVectorClock)
	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	// empty vector is earlest
	allEqual := true
	allSmall := true

	// NOTE: iterate both vectors, because node ids in each vector might be different
	for i := range v.vector {
		// fmt.Println("vector values at", i, v.vector[uint64(i)], otherVector.vector[uint64(i)])
		if v.vector[i] > otherVector.vector[i] {
			allSmall = false
		} else if v.vector[i] < otherVector.vector[i] {
			allEqual = false
		}
	}

	for j := range otherVector.vector {
		if v.vector[j] > otherVector.vector[j] {
			allSmall = false
		} else if v.vector[j] < otherVector.vector[j] {
			allEqual = false
		}
	}

	return !allEqual && allSmall
}

// Version vector implementation of a ConflictResolver. Might need to keep some state in here
// so that we can always give an up-to-date version vector.
type VersionVectorConflictResolver struct {
	// The node ID on which this conflict resolver is running. Used so that when a message is
	// received, vector[nodeID] can be incremented.
	nodeID uint64

	// mu guards vector
	mu sync.Mutex
	// This node's current clock
	vector map[uint64]uint64
}

// NewVersionVectorConflictResolver() returns an initialized VersionVectorConflictResolver{}
func NewVersionVectorConflictResolver() *VersionVectorConflictResolver {
	return &VersionVectorConflictResolver{vector: make(map[uint64]uint64)}
}

// ReplicatorDidStart initializes the VersionVectorConflictResolver using node metadata
func (v *VersionVectorConflictResolver) ReplicatorDidStart(node *node.Node) {
	v.nodeID = node.ID
	v.vector[v.nodeID] = 0

	log.Printf("version vector conflict resolver initializing itself")
}

// Finds the max of two ordered entities, x and y. constraints.Ordered is an alias for Integers
// and Floats.
func max[T constraints.Ordered](x T, y T) T {
	if x > y {
		return x
	}
	return y
}

// OnMessageReceive is called whenever the underlying node receives an RPC with a clock. As per
// the version-vector algorithm, this function does the following:
//   - Sets the current node's clock to be the element-wise max of itself and the given clock
//   - Increments its own nodeID in vector
//
// Remember thread-safety when modifying fields of v, since multiple messages could be received at
// the same time!
func (v *VersionVectorConflictResolver) OnMessageReceive(clock VersionVectorClock) {

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	v.mu.Lock()
	for n := range v.vector {
		v.vector[uint64(n)] = max(v.vector[uint64(n)], clock.vector[uint64(n)])
	}
	for n := range clock.vector {
		v.vector[uint64(n)] = max(v.vector[uint64(n)], clock.vector[uint64(n)])
	}
	v.vector[v.nodeID] += 1
	v.mu.Unlock()
}

// OnMessageSend is called before an RPC is sent to any other node. The version vector should be
// incremented for the local node.
func (v *VersionVectorConflictResolver) OnMessageSend() {
	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	v.mu.Lock()
	v.vector[v.nodeID] += 1
	v.mu.Unlock()
}

func (v *VersionVectorConflictResolver) OnEvent() {
	panic("disregard; not yet implemented in modist")
}

// NewClock creates a new VersionVectorClock by using v's vector.
//
// Note that maps in Golang are implicit pointers, so you should deep-copy the map before
// returning it.
func (v *VersionVectorConflictResolver) NewClock() VersionVectorClock {

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	newVector := NewVersionVectorClock()
	for n := range v.vector {
		newVector.vector[n] = v.vector[n]
	}
	return VersionVectorClock{vector: newVector.vector}
}

// ZeroClock returns a clock that happens before (or is concurrent with) all other clocks.
func (v *VersionVectorConflictResolver) ZeroClock() VersionVectorClock {
	return VersionVectorClock{vector: map[uint64]uint64{}}
}

// ResolveConcurrentEvents is run when we have several key-value pairs with the same keys, all
// with concurrent clocks (i.e. no version vector happens before any other version vector). To
// converge to one value, we must choose a "winner" among these key-value pairs. Like in
// physical.go, we choose the key-value with the highest lexicographic value.
//
// Additionally, the returned key-value must have a clock that is higher than all the given
// key-value pairs. You can construct a new clock for the returned key by merging the clocks of
// the conflicts slice together (merging two version vectors means computing the element-wise
// max).
//
// You should return an error if no conflicts are given.
func (v *VersionVectorConflictResolver) ResolveConcurrentEvents(conflicts ...*KV[VersionVectorClock]) (*KV[VersionVectorClock], error) {

	// TODO(students): [Clocks & Conflict Resolution] Implement me!
	if len(conflicts) == 0 {
		return nil, errors.New("no conflicts given")
	}
	largest := conflicts[0]
	for _, kv := range conflicts {
		if kv != nil && (largest == nil || kv.Value > largest.Value) {
			largest = kv
		}
	}
	// merging
	newClock := v.NewClock()
	for c := range conflicts {
		for n := range conflicts[c].Clock.vector {
			// fmt.Println("[In nested]", newClock.vector[n], conflicts[c].Clock.vector[n])
			newClock.vector[n] = max(newClock.vector[n], conflicts[c].Clock.vector[n])
		}
	}

	largest.Clock = newClock
	return largest, nil
}
