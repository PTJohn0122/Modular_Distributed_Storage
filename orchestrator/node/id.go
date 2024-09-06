package node

import (
	"encoding/binary"

	"github.com/google/uuid"
)

// ID is a generalized id generator that can be used to create unique ids for nodes, replicator groups, and any other entities.
func ID() uint64 {
	const (
		// The number of bytes used to represent any ID in the system. While a small
		// value may have conflicts, it is easier to parse when debugging so is used.
		// Changing this value will require changing how the sliced UUID is parsed
		// (ie. changing Uin16 to Uint64 etc.)
		bytesInID = 2
	)

	var fullID uuid.UUID
	var id uint16

	// 0 is used as a placeholder ID for the leader in Raft
	for id == 0 {
		fullID = uuid.New()
		id = binary.BigEndian.Uint16(fullID[:bytesInID])
	}
	return uint64(id)
}
