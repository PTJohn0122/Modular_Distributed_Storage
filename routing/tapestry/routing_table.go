/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package tapestry

import (
	"fmt"
	"os"
	"sync"
)

// RoutingTable has a number of levels equal to the number of digits in an ID
// (default 40). Each level has a number of slots equal to the digit base
// (default 16). A node that exists on level n thereby shares a prefix of length
// n with the local node. Access to the routing table is protected by a mutex.
type RoutingTable struct {
	localId ID                 // The ID of the local tapestry node
	Rows    [DIGITS][BASE][]ID // The rows of the routing table (stores IDs of remote tapestry nodes)
	mutex   sync.Mutex         // To manage concurrent access to the routing table (could also have a per-level mutex)
}

// NewRoutingTable creates and returns a new routing table, placing the local node at the
// appropriate slot in each level of the table.
func NewRoutingTable(me ID) *RoutingTable {
	t := new(RoutingTable)
	t.localId = me

	// Create the node lists with capacity of SLOTSIZE
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			t.Rows[i][j] = make([]ID, 0, SLOTSIZE) // a ID slice of length 0 and capacity SLOTSIZE
		}
	}

	// Make sure each row has at least one node in it
	for i := 0; i < DIGITS; i++ {
		slot := t.Rows[i][t.localId[i]]
		t.Rows[i][t.localId[i]] = append(slot, t.localId)
	}
	// fmt.Println(t.Rows)
	return t
}

// Add adds the given node to the routing table.
//
// Note you should not add the node to preceding levels. You need to add the node
// to one specific slot in the routing table (or replace an element if the slot is full
// at SLOTSIZE).
//
// Returns true if the node did not previously exist in the table and was subsequently added.
// Returns the previous node in the table, if one was overwritten.
func (t *RoutingTable) Add(remoteNodeId ID) (added bool, previous *ID) {
	// fmt.Println("[Add()], new ID to add =", remoteNodeId)
	t.mutex.Lock()
	defer t.mutex.Unlock() // call Unlock() after curr function finish execution

	level := SharedPrefixLength(t.localId, remoteNodeId)
	if level >= DIGITS {
		return false, nil
	}
	col := remoteNodeId[level]

	if len(t.Rows[level][col]) > 0 {
		// check if exist might remove
		for idx := range t.Rows[level][col] {
			if t.Rows[level][col][idx] == remoteNodeId {
				return false, nil
			}
		}
		// if full
		if len(t.Rows[level][col]) == SLOTSIZE {
			furthestIdx := 0
			for idx := 1; idx < SLOTSIZE; idx++ { // find the furthest among all nodes in this cell
				if t.localId.Closer(t.Rows[level][col][furthestIdx], t.Rows[level][col][idx]) {
					furthestIdx = idx
				}
			}
			if t.localId.Closer(remoteNodeId, t.Rows[level][col][furthestIdx]) {
				previousId := t.Rows[level][col][furthestIdx]
				t.Rows[level][col][furthestIdx] = remoteNodeId
				return true, &previousId
			}
			return false, nil
		}
	}

	t.Rows[level][col] = append(t.Rows[level][col], remoteNodeId)
	return true, nil
}

// Remove removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
// Return false if a node tries to remove itself from the table.
func (t *RoutingTable) Remove(remoteNodeId ID) (wasRemoved bool) {
	// fmt.Println("[Remove()] id =", remoteNodeId)
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if remoteNodeId == t.localId {
		return false
	}

	level := SharedPrefixLength(t.localId, remoteNodeId)
	col := remoteNodeId[level]

	for idx := range t.Rows[level][col] {
		if t.Rows[level][col][idx] == remoteNodeId {
			// fmt.Println("[Remove()] find the target id in slot, targte id =", remoteNodeId, "slot =", t.Rows[level][col])
			t.Rows[level][col] = append(t.Rows[level][col][:idx], t.Rows[level][col][idx+1:]...) // Question: might overflow
			// fmt.Println("[Remove()] after remove the slot =", t.Rows[level][col])
			return true
		}
	}
	return false
}

// GetLevel gets ALL nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodeIds []ID) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	nodesList := make([]ID, 0)
	tableLevel := t.Rows[level]

	for i := range tableLevel {
		for j := range tableLevel[i] {
			currId := tableLevel[i][j]
			if currId != t.localId {
				nodesList = append(nodesList, currId)
			}
		}
	}
	return nodesList
}

// FindNextHop searches the table for the closest next-hop node for the provided ID starting at the given level.
func (t *RoutingTable) FindNextHop(id ID, level int32) ID {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	for level < DIGITS {
		checkedCol := 0
		col := id[level]

		for len(t.Rows[level][col]) == 0 { // empty cell. keep moving right until a non-empty cell is found.
			checkedCol++
			if checkedCol == BASE {
				// no non-empty cell found. Do error handling here.
				fmt.Printf("No next hop found")
				os.Exit(1)
			}
			col = (col + 1) % BASE
		}

		if t.Rows[level][col][0] != t.localId {
			return t.Rows[level][col][0]
		}
		// the first entry in the non-empty cell is local. Check next level
		level++
	}
	return t.localId
}
