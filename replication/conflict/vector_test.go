package conflict

import (
	"testing"
)

func TestVectorConcurrentEventsDoNotHappenBefore(t *testing.T) {
	v1 := NewVersionVectorClock()
	v2 := NewVersionVectorClock()

	// check diff length, [0:0, 3:2] vs [0:1], should be concurrent
	v1.vector[0] = 0
	v1.vector[3] = 2
	v2.vector[0] = 1
	if v2.HappensBefore(v1) {
		t.Errorf("v2 does not happen before v1")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 does not happen before v2")
	}

	// check equal vectots, [0:0, 3:2] vs [0:0, 3:2], should be concurrent
	v2.vector[0] = 0
	v2.vector[3] = 2
	if v2.HappensBefore(v1) {
		t.Errorf("v2 does not happen before v1")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 does not happen before v2")
	}

	// check one vector greater, [0:0, 3:0] vs [0:0, 3:1], should return v2 -> v1
	v1.vector[3] = 0
	v2.vector[3] = 1
	if !v1.HappensBefore(v2) {
		t.Errorf("v1 does not happen before v2")
	}
	if v2.HappensBefore(v1) {
		t.Errorf("v2 does not happen before v1")
	}

	// check diff vector clock, [0:0, 2:1, 3:1] vs [0:0, 3:2, 4:1], should be concurrent
	v1.vector[2] = 1
	v2.vector[4] = 1
	if v2.HappensBefore(v1) {
		t.Errorf("v2 does not happen before v1")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 does not happen before v2")
	}
}

func TestMessageReceive(t *testing.T) {
	vr1 := NewVersionVectorConflictResolver()
	fakeclock := NewVersionVectorClock()
	vr1.vector[0] = 0
	fakeclock.vector[0] = 5
	vr1.OnMessageReceive(fakeclock)
	if vr1.vector[0] != 6 {
		t.Error("vr1 vector didn't increment correctly")
	}
	fakeclock.vector[0] = 3
	vr1.OnMessageReceive(fakeclock)
	if vr1.vector[0] == 3 {
		t.Error("vr1 vector copied wrong value")
	}
}

func TestMessageSend(t *testing.T) {
	vr1 := NewVersionVectorConflictResolver()
	vr1.vector[0] = 0
	vr1.OnMessageSend()
	if vr1.vector[0] != 1 {
		t.Error("vr1 vector didn't increment correctly")
	}
}

func TestNewClock(t *testing.T) {
	vr1 := NewVersionVectorConflictResolver()
	vr1.vector[0] = 1
	vr1.vector[1] = 1
	vr1.vector[2] = 1
	v2 := vr1.NewClock()
	for v := range vr1.vector {
		if v2.vector[v] != vr1.vector[v] {
			t.Error("v2 vector clock is different from vr1")
		}
	}
	vr1.vector[0] = 2
	if v2.vector[0] == 2 {
		t.Error("performed shallow copy for v2")
	}
}

func TestResolveConflict(t *testing.T) {
	vr1 := NewVersionVectorConflictResolver()
	vr1.vector[0] = 0
	vr1.vector[1] = 6
	vr1.vector[2] = 1
	vr1.vector[3] = 2
	_, err1 := vr1.ResolveConcurrentEvents()
	if err1 == nil {
		t.Error("no error prompted when no conflict")
	}
	v1 := vr1.ZeroClock()
	v2 := vr1.NewClock()
	v2.vector[2] = 5
	v3 := vr1.NewClock()
	v3.vector[2] = 3
	v3.vector[3] = 8

	kv1 := KVFromParts("some key", "abcde", v1)
	kv2 := KVFromParts("some key", "fghjk", v2)
	kv3 := KVFromParts("some key", "lmnop", v3)
	kvf, err := vr1.ResolveConcurrentEvents(kv1, kv2, kv3)
	// fmt.Println(kvf.Clock.vector)
	if err != nil {
		t.Error("Fail to reslove conflict")
	}
	if kvf.Value != "lmnop" {
		t.Error("Returned final KV don't have highest lexicographic value")
	}
	if kvf.Clock.vector[0] != 0 || kvf.Clock.vector[1] != 6 || kvf.Clock.vector[2] != 5 || kvf.Clock.vector[3] != 8 {
		t.Error("[resolve conflict] error when merging clock")
	}
}

func TestHappenBeforeHeteroNodeIDCase(t *testing.T) {
	v1 := NewVersionVectorClock()
	v2 := NewVersionVectorClock()
	v1.vector[5] = 10
	v2.vector[4] = 0
	if !v2.HappensBefore(v1) {
		t.Errorf("v2 happens before v1, since v1 is effectively empty")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 should not happen before v2, since v1 is effectively empty")
	}
}

func TestHappenBeforeEmptyCase(t *testing.T) {
	v1 := NewVersionVectorClock()
	v2 := NewVersionVectorClock()
	v1.vector[0] = 0
	v1.vector[1] = 0
	v1.vector[2] = 0
	if v2.HappensBefore(v1) {
		t.Errorf("v1 happens before v2, since v1 is effectively empty")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 happens before v2, since v1 is effectively empty")
	}
}
