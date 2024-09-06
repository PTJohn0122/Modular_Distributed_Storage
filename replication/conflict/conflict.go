package conflict

import (
	"fmt"
	pb "modist/proto"
)

// KVFromParts creates a new KV from a key, value, and clock.
func KVFromParts[T Clock](key string, value string, clock T) *KV[T] {
	return &KV[T]{
		Key:   key,
		Value: value,
		Clock: clock,
	}
}

// KVFromProto creates a new KV from a proto representation.
func KVFromProto[T Clock](p *pb.ResolvableKV) *KV[T] {
	return &KV[T]{
		Key:   p.Key,
		Value: p.Value,
		Clock: ClockFromProto[T](p.GetClock()),
	}
}

// KV is a key-value pair with an associated time (represented as some clock).
// The attached time is used in determining orderings between KVs when choosing
// which to read or write.
type KV[T Clock] struct {
	Key   string
	Value string
	Clock T
}

// Proto returns a proto representation of a KV that can be sent in an RPC.
func (kv *KV[T]) Proto() *pb.ResolvableKV {
	p := &pb.ResolvableKV{
		Key:   kv.Key,
		Value: kv.Value,
		Clock: kv.Clock.Proto(),
	}
	return p
}

// String returns a readable representation of the KV.
func (kv *KV[T]) String() string {
	return fmt.Sprintf("(%s, %s, %s)", kv.Key, kv.Value, kv.Clock)
}

// Equals returns true if and only if the two KVs are equal.
func (kv *KV[T]) Equals(other *KV[T]) bool {
	if kv == nil && other != nil || kv != nil && other == nil {
		return false
	} else if kv == nil && other == nil {
		return true
	}

	return kv.Key == other.Key && kv.Value == other.Value && kv.Clock.Equals(other.Clock)
}

// A Clock of a generic type, usually an implementation-specific key-value pair, provides
// a way to know whether an event came before another. From HappensBefore, concurrency of events
// can be determined (neither happens before the other).
//
// When events are concurrent, they can be resolved automatically through ResolveConcurrentEvents
// or manually through the application layer (in which case, ResolveConcurrentEvents cannot be
// used).
type Clock interface {
	// Proto converts a Clock into a proto representation that can be sent in an RPC.
	Proto() *pb.Clock

	// String returns a readable representation of the Clock.
	String() string

	// HappensBefore returns whether eventA definitively happened before eventB.
	// This may be because eventB is causally related to eventA, or because eventA took place,
	// in physical time, far before eventB. Concurrent events can be determined if
	// !a.HappensBefore(b) && !b.HappensBefore(a).
	//
	// For example, if T is a key-value pair with a UNIX timestamp, the conflict resolution policy
	// could be eventA.timestamp < eventB.timestamp.
	HappensBefore(other Clock) bool

	// Equals returns true if and only if the two clocks are equal.
	Equals(other Clock) bool
}

type Resolver[T Clock] interface {
	// A Resolver may update its state when messages are received and sent. They can hook into
	// these "lifecycle" events of a replicator by specifying functions here.
	ReplicatorLifecycle[T]

	// NewClock returns the current time.
	NewClock() T

	// ZeroClock returns a clock that happens before (or is concurrent with) all other clocks.
	ZeroClock() T

	// If none of the given events happen before any other events, then we need to invoke
	// resolution logic to determine which value "wins". In some databases, concurrent events
	// are resolved by the client, but in others, the resolution logic is known up-front.
	// ResolveConcurrentEvents is used in systems that have automatic conflict resolution logic.
	//
	// For example, if T is a key-value pair with a UNIX timestamp, events are considered
	// concurrent if their timestamps are all the same. In that case, we can choose the event
	// whose value is lexicographically the largest.
	ResolveConcurrentEvents(events ...*KV[T]) (*KV[T], error)
}

// ClockFromProto creates a new Clock from a proto representation.
//
// The type T must be the Go analog for the type of clock in the proto.
func ClockFromProto[T Clock](p *pb.Clock) T {
	if p == nil {
		panic("clock cannot be nil")
	}

	var t any = *new(T)
	switch x := t.(type) {
	case PhysicalClock:
		_ = x
		var c any = PhysicalClock{timestamp: p.Timestamp}
		return c.(T)
	case VersionVectorClock:
		_ = x
		if p.Vector != nil {
			var c any = VersionVectorClock{vector: p.Vector}
			return c.(T)
		} else {
			var c any = VersionVectorClock{vector: map[uint64]uint64{}}
			return c.(T)
		}
	}

	panic("unsupported clock type encountered")
}
