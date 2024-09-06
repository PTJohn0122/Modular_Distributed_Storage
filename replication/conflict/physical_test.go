package conflict

import (
	"testing"
	"time"
)

func testCreatePhysicalClockConflictResolver() *PhysicalClockConflictResolver {
	return &PhysicalClockConflictResolver{}
}

func (c *PhysicalClockConflictResolver) testCreatePhysicalClock() PhysicalClock {
	return PhysicalClock{timestamp: uint64(time.Now().UnixNano())}
}

func (c *PhysicalClockConflictResolver) testCreatePhysicalClockGivenTimestamp(timestamp uint64) PhysicalClock {
	return PhysicalClock{timestamp: timestamp}
}

func TestPhysicalConcurrentEventsHappenBefore(t *testing.T) {
	r := testCreatePhysicalClockConflictResolver()
	c0 := r.ZeroClock()
	c1 := r.testCreatePhysicalClockGivenTimestamp(10)
	c2 := r.testCreatePhysicalClockGivenTimestamp(20)
	c3 := r.testCreatePhysicalClockGivenTimestamp(30)
	c4 := r.NewClock()

	if !c0.HappensBefore(c1) {
		t.Errorf("c0(the zero clock) should happen before all other clocks")
	}
	if !c1.HappensBefore(c2) {
		t.Errorf("c1 should happen before c2")
	}
	if !c1.HappensBefore(c3) {
		t.Errorf("c1 should happen before c3")
	}
	if !c2.HappensBefore(c3) {
		t.Errorf("c2 should happen before c3")
	}
	if !c3.HappensBefore(c4) {
		t.Errorf("c3 should happen before c4 (recent time)")
	}
}

func TestPhysicalConcurrentEventsHappenConcurrently(t *testing.T) {
	r := testCreatePhysicalClockConflictResolver()
	c1 := r.testCreatePhysicalClockGivenTimestamp(10)
	c2 := r.testCreatePhysicalClockGivenTimestamp(10)

	if c1.HappensBefore(c2) {
		t.Errorf("c1 and c2 should be concurrent")
	}
	if c2.HappensBefore(c1) {
		t.Errorf("c1 and c2 should be concurrent")
	}
	if !c1.Equals(c2) {
		t.Errorf("c1 and c2 should be concurrent")
	}
}

func TestPhysicalClockConflictResolvingNoConflict(t *testing.T) {
	r := testCreatePhysicalClockConflictResolver()
	_, err := r.ResolveConcurrentEvents()
	if err == nil {
		t.Errorf("should return error when no conflict is given")
	}
}

func TestPhysicalClockConflictResolving(t *testing.T) {
	r := testCreatePhysicalClockConflictResolver()
	c1 := r.testCreatePhysicalClockGivenTimestamp(10)
	c2 := r.testCreatePhysicalClockGivenTimestamp(10)
	c3 := r.testCreatePhysicalClockGivenTimestamp(10)

	kv1 := KVFromParts("key", "aaa", c1)
	kv2 := KVFromParts("key", "aaaaaaa", c2)
	kv3 := KVFromParts("key", "correct", c3)

	ret, err := r.ResolveConcurrentEvents(kv1, kv2, kv3)
	if err != nil {
		t.Errorf("fail to resolve concurrent events")
	}
	if ret.Value != "correct" {
		t.Errorf("did not pick the key-value that has the lexicographic largest value")
	}
}
