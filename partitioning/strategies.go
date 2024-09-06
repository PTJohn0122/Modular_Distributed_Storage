package partitioning

// KeyRange represents a range of keys that is inclusive on both ends
type KeyRange struct {
	Start string
	End   string
}

// Reassignment defines a transfer of keys from one replica group to another.
type Reassignment struct {
	From uint64
	To   uint64

	Range KeyRange
}

// Assigner defines the API of a partitioning strategy.
type Assigner interface {
	// Lookup returns the ID of the replica group to which the specified key is assigned.
	Lookup(key string) (id uint64, rewrittenKey string, err error)

	// AddReplicaGroup adds a replica group to the assigner, returning a list of key ranges
	// that need to be reassigned to this new group.
	AddReplicaGroup(id uint64) []Reassignment

	// RemoveReplicaGroup removes a replica group from the assigner, returning a list of key
	// ranges that neeed to be reassigned to other replica groups.
	RemoveReplicaGroup(id uint64) []Reassignment
}
