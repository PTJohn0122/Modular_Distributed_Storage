package raft

import (
	"context"
	"fmt"
	pb "modist/proto"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (rn *RaftNode) doLeader() stateFunction {
	rn.log.Printf("node %d transitioning to leader state at term %d", rn.node.ID, rn.GetCurrentTerm())
	rn.state = LeaderState

	// initialization
	rn.leader = rn.node.ID
	rn.setVotedFor(rn.node.ID) // in some test cases a newly initialized node directly becomes a leader. So even though votedFor should be set in candidate state, set it here again.
	toFollowerC := make(chan bool)

	rn.StoreLog(&pb.LogEntry{
		Index: rn.LastLogIndex() + 1,
		Term:  rn.GetCurrentTerm(),
		Type:  pb.EntryType_NORMAL,
		Data:  nil, // the no-op entry
	})

	for id := range rn.node.PeerNodes {
		if id != rn.node.ID {
			rn.nextIndex[id] = rn.LastLogIndex()
			rn.matchIndex[id] = 0
		} else {
			rn.nextIndex[id] = rn.LastLogIndex() + 1
			rn.matchIndex[id] = rn.LastLogIndex()
		}
	}
	rn.log.Printf("[leadernode %d] Initailized, LastLogIndex = %d, NextIndex = %v, MatchIndex = %v", rn.node.ID, rn.LastLogIndex(), rn.nextIndex, rn.matchIndex)
	// appendEntriesFunc returns whether the leader should step down
	appendEntriesFunc := func(ctx context.Context, rn *RaftNode) bool {
		pendingResults := len(rn.node.PeerConns) - 1
		maj := len(rn.node.PeerConns)/2 + 1
		successCount := 0
		successCountC := make(chan int)
		lastLogIndex := rn.LastLogIndex()

		if maj == 1 { // handle edge case: when there's only 1 node
			rn.leaderMu.Lock()
			rn.commitIndex = lastLogIndex
			rn.applyLog()
			rn.leaderMu.Unlock()
			return false
		}

		for id := range rn.node.PeerConns {
			if id == rn.node.ID {
				continue // do not send rpc to self
			}

			go func(ctx context.Context, rn *RaftNode, id uint64, successCountC chan int, toFollowerC chan bool) {
				rn.leaderMu.Lock()
				entries := make([]*pb.LogEntry, 0)
				prevLogIndex := rn.nextIndex[id] - 1
				for logIdx := prevLogIndex + 1; logIdx <= lastLogIndex; logIdx++ {
					entries = append(entries, rn.GetLog(logIdx))
				}
				req := pb.AppendEntriesRequest{
					From:         rn.node.ID,
					To:           id,
					Term:         rn.GetCurrentTerm(),
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rn.GetLog(prevLogIndex).GetTerm(),
					Entries:      entries,
					LeaderCommit: rn.commitIndex,
				}
				rn.leaderMu.Unlock()

				rn.log.Printf("[leader %d] sending AE RPC {%v}\n", rn.node.ID, req)
				followerNode := pb.NewRaftRPCClient(rn.node.PeerConns[id])
				rep, err := followerNode.AppendEntries(ctx, &req)
				if err != nil {
					rn.log.Printf("%s", err)
					successCountC <- 0
					return
				}

				rn.log.Printf("[leader %d] received reply(not yet processed!): %v\n", rn.node.ID, rep)
				if rep.Success {
					// might need linear operation in later
					rn.leaderMu.Lock()
					rn.nextIndex[id] = lastLogIndex + 1
					rn.matchIndex[id] = lastLogIndex
					rn.leaderMu.Unlock()
					rn.log.Printf("matchIndex[%d] updated to: %d", id, lastLogIndex)
					successCountC <- 1
				} else {
					if rep.GetTerm() > rn.GetCurrentTerm() {
						rn.SetCurrentTerm(rep.GetTerm())
						toFollowerC <- true
						return
					}

					rn.leaderMu.Lock()
					if rn.nextIndex[id] > 1 {
						rn.nextIndex[id]--
					}
					rn.leaderMu.Unlock()
					successCountC <- 0
				}
			}(ctx, rn, id, successCountC, toFollowerC)
		}

		heartbeatTimer := time.NewTimer(rn.heartbeatTimeout)
		defer heartbeatTimer.Stop()

		for {
			select {
			case <-toFollowerC:
				return true // don't try to gather all results, immediate step down
			case result := <-successCountC:
				rn.log.Printf("[leader %d] processing reply (successcount): %d\n", rn.node.ID, result)
				select { // drain toFollowerC
				case <-toFollowerC:
					rn.log.Printf("[leader %d] detect toFollower when processing reply\n", rn.node.ID)
					return true
				default:
				}
				successCount += result
				pendingResults--
				if successCount+1 == maj {
					rn.log.Printf("[leader %d] has collected success from majority. returning...\n", rn.node.ID)
					rn.leaderMu.Lock()
					if lastLogIndex > rn.commitIndex {
						rn.commitIndex = lastLogIndex
						rn.applyLog()
					}
					rn.leaderMu.Unlock()
					return false
				}
				if pendingResults == 0 {
					rn.log.Printf("[leader %d] has collected all results. returning...\n", rn.node.ID)
					return false
				}
				continue
			default:
			}
			select {
			case <-heartbeatTimer.C: // leader has waited long enough
				select { // drain toFollowerC
				case <-toFollowerC:
					rn.log.Printf("[leader %d] detect toFollower when processing reply\n", rn.node.ID)
					return true
				default:
				}
				rn.log.Printf("[leader %d] timed out waiting for AE results\n", rn.node.ID)
				return false
			default:
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if appendEntriesFunc(ctx, rn) { // try broadcasting the no-op entry
		return rn.doFollower
	}
	rn.log.Printf("returning from 1st appendEntriesFunc. toFollower = false\n")

	heartbeatTimer := time.NewTimer(rn.heartbeatTimeout)
	defer heartbeatTimer.Stop()

	for {
		select {
		case <-toFollowerC:
			return rn.doFollower
		case requestVoteMsg := <-rn.requestVoteC:
			rn.log.Printf("[leader %d]: got vote request: %v. Current votedFor=%d\n", rn.node.ID, requestVoteMsg.request, rn.GetVotedFor())

			grantVote, toFollower := rn.handleReqVoteMsg(requestVoteMsg.request)

			requestVoteMsg.reply <- pb.RequestVoteReply{
				From:        rn.node.ID,
				To:          requestVoteMsg.request.From,
				Term:        rn.GetCurrentTerm(),
				VoteGranted: grantVote,
			}
			if toFollower {
				rn.leader = 0
				return rn.doFollower
			}
			continue

		case appendEntriesMsg := <-rn.appendEntriesC:
			rn.log.Printf("[leader %d]: got AppendEntries request: %v\n", rn.node.ID, appendEntriesMsg.request)
			success := false
			toFollower := false

			if appendEntriesMsg.request.GetTerm() > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(appendEntriesMsg.request.GetTerm())
				rn.setVotedFor(None)
				rn.leader = appendEntriesMsg.request.GetFrom()
				toFollower = true
				success = rn.tryAppendToLocalLog(appendEntriesMsg.request)
				rn.applyLog()
			}

			rep := pb.AppendEntriesReply{
				From:    rn.node.ID,
				To:      appendEntriesMsg.request.From,
				Term:    rn.GetCurrentTerm(),
				Success: success,
			}
			rn.log.Printf("[leader %d]: sending AppendEntries reply: %v\n", rn.node.ID, rep)
			appendEntriesMsg.reply <- rep

			if toFollower {
				return rn.doFollower
			}
			continue

		case data, ok := <-rn.proposeC:
			if !ok {
				// rn.log.Printf("[leader %d]: reading from a closed proposeC. initializing shut down...\n", rn.node.ID)
				fmt.Printf("[leader %d]: reading from a closed proposeC. initializing shut down...\n", rn.node.ID)
				rn.proposeC = nil
				rn.state = ExitState
				rn.Stop()
				close(rn.stopC)
				return nil
			}

			rn.log.Printf("[leader %d]: got proposal: %v\n", rn.node.ID, data)

			heartbeatTimer.Stop()
			heartbeatTimer.Reset(rn.heartbeatTimeout)

			// append to own log:
			rn.StoreLog(&pb.LogEntry{
				Index: rn.LastLogIndex() + 1,
				Term:  rn.GetCurrentTerm(),
				Type:  pb.EntryType_NORMAL,
				Data:  data,
			})

			rn.updateCIApplyLog()

			toFollower := appendEntriesFunc(ctx, rn)
			rn.log.Printf("returning from appendEntriesFunc. toFollower = %t\n", toFollower)
			if toFollower {
				return rn.doFollower
			}
			continue

		default:
		}
		select {
		case <-heartbeatTimer.C:
			select { // drain toFollowerC
			case <-toFollowerC:
				return rn.doFollower
			default:
			}
			rn.log.Printf("[leader %d] timed out. sending heartbeat\n", rn.node.ID)
			heartbeatTimer.Stop()
			heartbeatTimer.Reset(rn.heartbeatTimeout)

			rn.updateCIApplyLog()

			toFollower := appendEntriesFunc(ctx, rn)
			rn.log.Printf("returning from appendEntriesFunc. toFollower = %t\n", toFollower)
			if toFollower {
				return rn.doFollower
			}
		default:
		}
	}
}
