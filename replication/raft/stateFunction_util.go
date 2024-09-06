package raft

import (
	"math/rand"
	pb "modist/proto"
	"time"
)

// Returns a time between [timeout, 2 * timeout]
func randomizedTimeout(timeout time.Duration) time.Duration {
	rand.Seed(time.Now().UnixNano())
	return timeout + time.Duration(rand.Int63n(int64(timeout)))
}

// A wrapper on updating commit index and applying log with locking
func (rn *RaftNode) updateCIApplyLog() {
	rn.leaderMu.Lock()
	update := rn.updateCommitIndex()
	if update {
		rn.applyLog()
	}
	rn.leaderMu.Unlock()
}

// Checks for the largest N that is smaller or equal to the majority of matchIndex
//
//	if it is greater than current commit index, update commit index and returns true
//	locking should be handled by the caller
func (rn *RaftNode) updateCommitIndex() bool {
	if rn.LastLogIndex() <= rn.commitIndex {
		return false
	}

	maj := len(rn.node.PeerConns)/2 + 1
	freq := make(map[uint64]int)
	for _, val := range rn.matchIndex {
		freq[val] = freq[val] + 1
	}

	for newComInd := rn.LastLogIndex(); newComInd > rn.commitIndex; newComInd-- {
		largerCount := 0
		for val := newComInd; val <= rn.LastLogIndex(); val++ {
			largerCount += freq[val]
		}
		if largerCount >= maj {
			rn.commitIndex = newComInd
			rn.log.Printf("commit index updated to: %d", rn.commitIndex)
			return true
		}
	}

	rn.log.Printf("commit index: %d was not updated", rn.commitIndex)
	return false
}

// Apply log to state machine until lastApplied = commitIndex
//
//	locking should be handled by the caller
func (rn *RaftNode) applyLog() {
	for rn.commitIndex > rn.lastApplied {
		rn.log.Printf("[node %d] applying log. lastIndex: %d, commitIndex: %d, lastApplied: %d\n", rn.node.ID, rn.LastLogIndex(), rn.commitIndex, rn.lastApplied)
		data := commit(rn.GetLog(rn.lastApplied + 1).GetData())
		if data != nil {
			rn.commitC <- &data
			rn.log.Printf("[%s %d] sent into commitC: %v", rn.state.String(), rn.node.ID, data)
		}
		rn.lastApplied++
	}
}

// Try appending entries in the request to rn's log
// Returns whether appending was successful
func (rn *RaftNode) tryAppendToLocalLog(req *pb.AppendEntriesRequest) bool {
	prevLogIndex := req.GetPrevLogIndex()
	logEntry := rn.GetLog(prevLogIndex)
	if logEntry != nil && logEntry.GetTerm() == req.GetPrevLogTerm() {
		lastNewIndex := prevLogIndex

		for _, incomingEntry := range req.GetEntries() {
			rn.log.Printf("[%s %d] appending entry: %v", rn.state.String(), rn.node.ID, incomingEntry)
			lastNewIndex = incomingEntry.GetIndex()
			logEntry = rn.GetLog(lastNewIndex)
			if logEntry == nil {
				rn.StoreLog(incomingEntry)
			} else if logEntry.GetTerm() != incomingEntry.GetTerm() {
				rn.TruncateLog(uint64(lastNewIndex))
				rn.StoreLog(incomingEntry)
			}
		}

		rn.log.Printf("[%s %d] last new index: %d", rn.state.String(), rn.node.ID, lastNewIndex)

		if req.GetLeaderCommit() > rn.commitIndex {
			if req.GetLeaderCommit() > lastNewIndex {
				rn.commitIndex = lastNewIndex
			} else {
				rn.commitIndex = req.GetLeaderCommit()
			}
			rn.log.Printf("[%s %d] commitIndex updated to: %d", rn.state.String(), rn.node.ID, rn.commitIndex)
		}
		return true
	}
	return false
}

// Handle and Reply to vote request
// Returns whether rn should convert to follower and whether vote is granted
func (rn *RaftNode) handleReqVoteMsg(req *pb.RequestVoteRequest) (grantVote bool, toFollower bool) {
	toFollower = false
	grantVote = false
	originalTerm := rn.GetCurrentTerm()

	if req.GetTerm() < originalTerm {
		return
	}
	if req.GetFrom() == rn.node.ID { // Q: ignore request from self
		return
	}

	if req.GetTerm() > originalTerm {
		rn.SetCurrentTerm(req.GetTerm())
		rn.setVotedFor(None)
		toFollower = true
	}

	if rn.state != LeaderState || req.GetTerm() > originalTerm { // for leader to grant vote, req.term must be strictly larger
		if rn.GetVotedFor() == None || rn.GetVotedFor() == req.GetFrom() {
			// check up-to-date-ness
			localLastIndex := rn.LastLogIndex()
			localLastTerm := rn.GetLog(localLastIndex).GetTerm()
			if req.GetLastLogTerm() > localLastTerm ||
				(req.GetLastLogTerm() == localLastTerm &&
					req.GetLastLogIndex() >= localLastIndex) {
				grantVote = true
				toFollower = true
				rn.setVotedFor(req.GetFrom())
				rn.log.Printf("[%s %d] has set votedfor to %d", rn.state.String(), rn.node.ID, rn.GetVotedFor())
			}
		}
	}
	return
}
