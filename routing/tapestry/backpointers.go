/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines Backpointers struct and implements accessors and
 *  mutators for Backpointers objects.
 */

package tapestry

import (
	"sync"
)

// Backpointers are stored by level, like the routing table
// A backpointer at level n indicates that the backpointer shares a prefix of length n with this node
// Access to the backpointers is managed by a lock
type Backpointers struct {
	localId ID               // the ID of the local tapestry node
	sets    [DIGITS]*NodeSet // backpointers
}

// NodeSet represents a set of nodes.
// The implementation is just a wrapped map with a mutex.
type NodeSet struct {
	data  map[ID]bool
	mutex sync.Mutex
}

// NewBackpointers creates and returns a new backpointer set.
func NewBackpointers(me ID) *Backpointers {
	b := new(Backpointers)
	b.localId = me
	for i := 0; i < DIGITS; i++ {
		b.sets[i] = NewNodeSet()
	}
	return b
}

// Add a backpointer for the provided node ID
// Returns true if a new backpointer was added.
func (b *Backpointers) Add(remoteNodeId ID) bool {
	if b.localId != remoteNodeId {
		return b.level(remoteNodeId).Add(remoteNodeId)
	}
	return false
}

// Remove a backpointer for the provided node ID, if it existed
// Returns true if the backpointer existed and was subsequently removed.
func (b *Backpointers) Remove(remoteNodeId ID) bool {
	if b.localId != remoteNodeId {
		return b.level(remoteNodeId).Remove(remoteNodeId)
	}
	return false
}

// Get all backpointers at the provided level.
func (b *Backpointers) Get(level int) []ID {
	if level >= DIGITS || level < 0 {
		return []ID{}
	}
	return b.sets[level].Nodes()
}

// Gets the node set for the level that the specified node ID should occupy.
func (b *Backpointers) level(remoteNodeId ID) *NodeSet {
	return b.sets[SharedPrefixLength(b.localId, remoteNodeId)]
}

// NewNodeSet creates a new node set.
func NewNodeSet() *NodeSet {
	s := new(NodeSet)
	s.data = make(map[ID]bool)
	return s
}

// Add the given node ID to the node set if it isn't already in the set.
// Returns true if the node ID was added; false if it already existed
func (s *NodeSet) Add(remoteNodeId ID) bool {
	s.mutex.Lock()
	_, exists := s.data[remoteNodeId]
	s.data[remoteNodeId] = true
	s.mutex.Unlock()
	return !exists
}

// AddAll adds all of the node IDs to the node set.
func (s *NodeSet) AddAll(remoteNodeIds []ID) {
	s.mutex.Lock()
	for _, remoteNodeId := range remoteNodeIds {
		s.data[remoteNodeId] = true
	}
	s.mutex.Unlock()
}

// Remove the given node ID from the node set if it's currently in the set
// Returns true if the node ID was removed; false if it was not in the set.
func (s *NodeSet) Remove(remoteNodeId ID) bool {
	s.mutex.Lock()
	_, exists := s.data[remoteNodeId]
	delete(s.data, remoteNodeId)
	s.mutex.Unlock()
	return exists
}

// Contains tests whether the specified node ID is contained in the set
func (s *NodeSet) Contains(remoteNodeId ID) (b bool) {
	s.mutex.Lock()
	b = s.data[remoteNodeId]
	s.mutex.Unlock()
	return
}

// Size returns the size of the set
func (s *NodeSet) Size() int {
	s.mutex.Lock()
	size := len(s.data)
	s.mutex.Unlock()
	return size
}

// Nodes gets all node IDs in the set as a slice
func (s *NodeSet) Nodes() []ID {
	s.mutex.Lock()
	nodeIds := make([]ID, 0, len(s.data))
	for remoteNodeId := range s.data {
		nodeIds = append(nodeIds, remoteNodeId)
	}
	s.mutex.Unlock()
	return nodeIds
}
