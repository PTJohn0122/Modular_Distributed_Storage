package conflict

import (
	"errors"
	"fmt"
	pb "modist/proto"
	"time"
)

// PhysicalClock is the Clock that we use to implement eventual consistency. The timestamp is a
// UNIX timestamp.
type PhysicalClock struct {
	timestamp uint64
}

// Proto converts a PhysicalClock into a clock that can be sent in an RPC.
func (c PhysicalClock) Proto() *pb.Clock {
	p := &pb.Clock{
		Timestamp: c.timestamp,
	}
	return p
}

func (c PhysicalClock) String() string {
	return fmt.Sprint(c.timestamp)
}

func (c PhysicalClock) Equals(other Clock) bool {
	otherClock := other.(PhysicalClock)
	return c.timestamp == otherClock.timestamp
}

// HappensBefore determines whether the PhysicalClock c occurs *strictly* before other.
func (c PhysicalClock) HappensBefore(other Clock) bool {
	otherClock := other.(PhysicalClock)
	return c.timestamp < otherClock.timestamp
}

// PhysicalClockConflictResolver implements a Resolver, meaning that it can generate new clocks
// (of type PhysicalClock) and that it "resolves" key-value pairs whose clocks are concurrent.
type PhysicalClockConflictResolver struct {
	// This embedded DefaultLifecycle struct provides default values for the lifecycle "hooks",
	// which are functions that are run when certain events happen on Resolver's node. These are
	// needed only for vector-based clocks, so we just provide "dummy"/default function values here.
	DefaultLifecycle[PhysicalClock]
}

// NewClock() returns a PhysicalClock using the current system timestamp, in nanoseconds.
func (c *PhysicalClockConflictResolver) NewClock() PhysicalClock {
	return PhysicalClock{timestamp: uint64(time.Now().UnixNano())}
}

// ZeroClock returns a clock that happens before (or is concurrent with) all other clocks.
func (c *PhysicalClockConflictResolver) ZeroClock() PhysicalClock {
	return PhysicalClock{timestamp: 0}
}

// ResolveConcurrentEvents is run when we have several key-value pairs all with the same keys, all
// with clocks that are concurrent. Recall that two clocks are concurrent if neither happens
// before the other. With our PhysicalClocks, when might this happen?
//
// Even if two events have the same PhysicalClock, we want eventual consistency, so we need to
// pick a "winning" key-value pair. Here, we pick the key-value that has the lexicographic
// largest value.
//
// You should return an error if no conflicts are given.
func (c *PhysicalClockConflictResolver) ResolveConcurrentEvents(conflicts ...*KV[PhysicalClock]) (*KV[PhysicalClock], error) {
	if len(conflicts) == 0 {
		return nil, errors.New("no conflicts given")
	}
	largest := conflicts[0]
	for _, kv := range conflicts {
		if kv != nil && (largest == nil || kv.Value > largest.Value) {
			largest = kv
		}
	}
	return largest, nil
}
