/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines the LocationMap type and methods for manipulating it.
 */

package tapestry

import (
	"fmt"
	"sync"
	"time"
)

// LocationMap is a struct containing objects being advertised to the tapestry.
// Object mappings are stored in the root node. An object can be advertised by multiple nodes.
// Objects time out after some amount of time if the advertising node is not heard from.
type LocationMap struct {
	Data  map[string]map[ID]*time.Timer // Multimap: stores multiple nodes per key, and each node has a timeout
	mutex sync.Mutex                    // To manage concurrent access to the location map
}

// NewLocationMap creates a new objectstore.
func NewLocationMap() *LocationMap {
	m := new(LocationMap)
	m.Data = make(map[string]map[ID]*time.Timer)
	return m
}

// Register registers the specified node as having advertised the key.
// Times out after the specified duration.
func (store *LocationMap) Register(key string, node ID, timeout time.Duration) bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	// Get the value set for the object
	_, exists := store.Data[key]
	if !exists {
		store.Data[key] = make(map[ID]*time.Timer)
	}

	// Add the value to the value set
	timer, exists := store.Data[key][node]
	if !exists {
		store.Data[key][node] = store.newTimeout(key, node, timeout)
	} else {
		timer.Reset(TIMEOUT)
	}

	return !exists
}

// RegisterAll registers all of the provided nodes and keys.
func (store *LocationMap) RegisterAll(nodeMap map[string][]ID, timeout time.Duration) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	for key, nodes := range nodeMap {
		_, exists := store.Data[key]
		if !exists {
			store.Data[key] = make(map[ID]*time.Timer)
		}
		for _, node := range nodes {
			store.Data[key][node] = store.newTimeout(key, node, timeout)
		}
	}
}

// Unregister unregisters the specified node for the specified key.
// Returns false if the node was not registered for the key.
func (store *LocationMap) Unregister(key string, node ID) bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	_, existed := store.Data[key][node]
	delete(store.Data[key], node)

	return existed
}

// UnregisterAll unregisters all nodes that are registered for the provided key.
// Returns all replicas that were advertising the key.
func (store *LocationMap) UnregisterAll(key string) (nodes []ID) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	nodes = slice(store.Data[key])
	delete(store.Data, key)

	return
}

// Get the nodes that are advertising a given key.
func (store *LocationMap) Get(key string) (nodes []ID) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	nodes = slice(store.Data[key])
	return
}

// GetTransferRegistrations removes and returns all objects that should be transferred to the remote node.
func (store *LocationMap) GetTransferRegistrations(
	localId ID,
	remoteNodeId ID,
) map[string][]ID {
	transfer := make(map[string][]ID)

	store.mutex.Lock()
	defer store.mutex.Unlock()

	for key, values := range store.Data {
		// Compare the first digit after the prefix
		if Hash(key).IsNewRoute(remoteNodeId, localId) {
			transfer[key] = slice(values)
		}
	}

	for key := range transfer {
		delete(store.Data, key)
	}

	return transfer
}

// Utility method. Creates an expiry timer for the (key, value) pair.
func (store *LocationMap) newTimeout(
	key string,
	node ID,
	timeout time.Duration,
) *time.Timer {
	expire := func() {
		fmt.Printf("Expiring %v for node %v\n", key, node)

		store.mutex.Lock()

		timer, exists := store.Data[key][node]
		if exists {
			timer.Stop()
			delete(store.Data[key], node)
		}

		store.mutex.Unlock()
	}

	return time.AfterFunc(timeout, expire)
}

// Utility function to get the keys of a map
func slice(valmap map[ID]*time.Timer) (values []ID) {
	for value := range valmap {
		values = append(values, value)
	}
	return
}
