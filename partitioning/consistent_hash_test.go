package partitioning

import (
	"encoding/binary"
	"testing"

	"golang.org/x/exp/slices"
)

// checkLookup performs a lookup of key using the provided consistent hash partitioner,
// ensuring that there is no error, the returned id matches what is expected, and the
// rewritten key is a hash of the looked-up key.
func checkLookup(t *testing.T, msg string, c *ConsistentHash, key string, id uint64) {
	t.Helper()

	gotID, gotRewrittenKey, err := c.Lookup(key)
	rewrittenKey := hashToString(c.keyHash(key))
	if err != nil {
		t.Errorf("%s: Returned an error: %v", msg, err)
	} else if gotID != id {
		t.Errorf("%s: Returned the wrong shard: expected %d, got %d\nThe hashed key is %s\n Here are the virtual nodes in the assigner: %+v\n\n", msg, id, gotID, rewrittenKey, c.virtualNodes)
	} else if gotRewrittenKey != rewrittenKey {
		t.Errorf("%s: Returned the wrong rewritten key: expected %s, got %s", msg, rewrittenKey, gotRewrittenKey)
	}
}

// identityHasher returns the last 32 bytes of the input as a 32-byte array, padding with
// zeroes if necessary.
func identityHasher(b []byte) [32]byte {
	var out [32]byte

	bIndex := len(b) - 1
	for i := len(out) - 1; i >= 0; i-- {
		if bIndex < 0 {
			continue
		}
		out[i] = b[bIndex]
		bIndex--
	}
	return out
}

func newVirtualNode(c *ConsistentHash, id uint64, virtualNum int) virtualNode {
	return virtualNode{
		id:   id,
		num:  virtualNum,
		hash: c.virtualNodeHash(id, virtualNum),
	}
}

func Test_Lookup_SimpleIdentity(t *testing.T) {
	c := NewConsistentHash(2)
	c.hasher = identityHasher

	c.virtualNodes = []virtualNode{
		newVirtualNode(c, 1, 0),
		newVirtualNode(c, 1, 1),
		newVirtualNode(c, 50, 0),
		newVirtualNode(c, 50, 1),
	}
	slices.SortFunc(c.virtualNodes, virtualNodeLess)

	byteKey := make([]byte, 8)

	binary.BigEndian.PutUint64(byteKey, 2)
	checkLookup(t, "Lookup(2)", c, string(byteKey), 1)

	binary.BigEndian.PutUint64(byteKey, 10)
	checkLookup(t, "Lookup(10)", c, string(byteKey), 50)

	binary.BigEndian.PutUint64(byteKey, 50)
	checkLookup(t, "Lookup(50)", c, string(byteKey), 50)

	binary.BigEndian.PutUint64(byteKey, 51)
	checkLookup(t, "Lookup(51)", c, string(byteKey), 50)
}

func Test_AddReplicaGroup(t *testing.T) {
	c := NewConsistentHash(3)
	c.hasher = identityHasher

	// testing adding by group
	c.AddReplicaGroup(1)
	if len(c.virtualNodes) != 1*c.virtualNodesPerGroup {
		t.Error("Number of added nodes in one group is wrong")
	}
	c.AddReplicaGroup(1)
	if len(c.virtualNodes) != 1*c.virtualNodesPerGroup {
		t.Error("Group with same key is added again")
	}

	// test lookup before adding more
	byteKey := make([]byte, 8)
	binary.BigEndian.PutUint64(byteKey, 2)
	checkLookup(t, "Lookup(2)", c, string(byteKey), 1)

	c.AddReplicaGroup(10)
	c.AddReplicaGroup(20)

	if len(c.virtualNodes) != 3*c.virtualNodesPerGroup {
		t.Error("Total Number of addded node in groups is wrong")
	}

	// test lookup after adding more
	binary.BigEndian.PutUint64(byteKey, 8)
	checkLookup(t, "Lookup(8)", c, string(byteKey), 10)

	// check left side of largest node
	binary.BigEndian.PutUint64(byteKey, 22)
	checkLookup(t, "Lookup(22)", c, string(byteKey), 20)

	// check super large number to prove it's a circle
	binary.BigEndian.PutUint64(byteKey, 2200)
	checkLookup(t, "Lookup(2200)", c, string(byteKey), 1)
}

func Test_RemoveReplicaGroup(t *testing.T) {
	c := NewConsistentHash(3)
	c.hasher = identityHasher
	c.AddReplicaGroup(2)
	c.AddReplicaGroup(1)
	c.AddReplicaGroup(5)

	// check lookup before removal
	byteKey := make([]byte, 8)
	binary.BigEndian.PutUint64(byteKey, 2)
	checkLookup(t, "Lookup(2)", c, string(byteKey), 2)

	rm := c.RemoveReplicaGroup(2)
	// check length of removed and remining
	if len(rm) != c.virtualNodesPerGroup {
		t.Error("Number of removed nodes doesn't equal to the number of nodes in a group")
	}
	// added 3 groups that each has 3 nodes, removed 1 group that has 3 node
	if len(c.virtualNodes) != 3*3-3 {
		t.Error("Number of removed nodes is wrong")
	}

	// check lookup after removal
	checkLookup(t, "Lookup(2)", c, string(byteKey), 1)

}

func Test_RemoveReplicaGroup_Empty(t *testing.T) {
	c := NewConsistentHash(5)
	rm := c.RemoveReplicaGroup(1)
	if len(rm) != 0 {
		t.Error("Remove nodes from empty hash chain is not nil")
	}
}

func Test_AddRemoveReplicaGroup(t *testing.T) {
	c := NewConsistentHash(3)
	c.hasher = identityHasher
	c.AddReplicaGroup(1)
	c.AddReplicaGroup(2)
	c.AddReplicaGroup(3)
	if len(c.virtualNodes) != 9 {
		t.Errorf("wrong virtual node number")
	}

	byteKey := make([]byte, 8)
	binary.BigEndian.PutUint64(byteKey, 2)
	checkLookup(t, "Lookup(10)", c, string(byteKey), 1)
	checkLookup(t, "Lookup(10)", c, string(byteKey), 1)

	c.RemoveReplicaGroup(3)
	if len(c.virtualNodes) != 6 {
		t.Errorf("wrong virtual node number")
	}
	c.RemoveReplicaGroup(5)
	if len(c.virtualNodes) != 6 {
		t.Errorf("wrong virtual node number")
	}

	c.RemoveReplicaGroup(1)
	checkLookup(t, "Lookup(10)", c, string(byteKey), 2)
}
