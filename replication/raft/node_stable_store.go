package raft

import (
	"encoding/binary"
	pb "modist/proto"
)

// ////////////////////////////////////////////////////////////////////////////////
// // High level API for StableStore                                        	 //
// ////////////////////////////////////////////////////////////////////////////////

// initStore stores zero-index log
func (rn *RaftNode) InitStableStore() {
	rn.StoreLog(&pb.LogEntry{
		Index: 0,
		Term:  0,
		Type:  pb.EntryType_NORMAL,
		Data:  nil,
	})
}

// SetCurrentTerm sets the current node's term and writes log to disk
func (rn *RaftNode) SetCurrentTerm(newTerm uint64) {
	currentTerm := rn.GetCurrentTerm()
	if newTerm != currentTerm {
		rn.log.Printf("[node %d] setting current term from %v -> %v", rn.node.ID, currentTerm, newTerm)
	}
	err := rn.stableStore.SetUint64([]byte("current_term"), newTerm)
	if err != nil {
		rn.log.Printf("unable to flush new term to disk: %v", err)
		panic(err)
	}
}

// GetCurrentTerm returns the current node's term
func (rn *RaftNode) GetCurrentTerm() uint64 {
	return rn.stableStore.GetUint64([]byte("current_term"))
}

// setVotedFor sets the candidateID for which the current node voted for, and writes log to disk
func (rn *RaftNode) setVotedFor(candidateID uint64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, candidateID)

	err := rn.stableStore.SetBytes([]byte("voted_for"), b)
	if err != nil {
		rn.log.Printf("unable to flush new votedFor to disk: %v", err)
		panic(err)
	}
}

// GetVotedFor returns the ID of the candidate that the current node voted for
func (rn *RaftNode) GetVotedFor() uint64 {
	b := rn.stableStore.GetBytes([]byte("voted_for"))
	candidateID := binary.LittleEndian.Uint64(b)
	return candidateID
}

// LastLogIndex returns index of last log. If no log exists, it returns 0.
func (rn *RaftNode) LastLogIndex() uint64 {
	return rn.stableStore.LastLogIndex()
}

// StoreLog appends log to log entry. Should always succeed
func (rn *RaftNode) StoreLog(log *pb.LogEntry) {
	err := rn.stableStore.StoreLog(log)
	if err != nil {
		panic(err)
	}
}

// GetLog gets a log at a specific index. If log does not exist, GetLog returns nil
func (rn *RaftNode) GetLog(index uint64) *pb.LogEntry {
	return rn.stableStore.GetLog(index)
}

// TruncateLog deletes logs from index to end of logs. Should always succeed
func (rn *RaftNode) TruncateLog(index uint64) {
	err := rn.stableStore.TruncateLog(index)
	if err != nil {
		panic(err)
	}
}

// RemoveLogs removes data from stableStore
func (rn *RaftNode) RemoveLogs() {
	rn.stableStore.Remove()
}
