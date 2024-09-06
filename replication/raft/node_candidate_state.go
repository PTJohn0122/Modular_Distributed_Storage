package raft

import (
	"context"
	pb "modist/proto"
	"time"
)

// doCandidate implements the logic for a Raft node in the candidate state.
func (rn *RaftNode) doCandidate() stateFunction {
	rn.state = CandidateState
	rn.log.Printf("node %d transitioning to %s state at term %d", rn.node.ID, rn.state, rn.GetCurrentTerm())

	// initialization
	rn.SetCurrentTerm(rn.GetCurrentTerm() + 1)
	rn.setVotedFor(rn.node.ID)

	maj := len(rn.node.PeerConns)/2 + 1
	if maj == 1 {
		return rn.doLeader
	}
	numVotesFromOthers := 0
	numRejsFromOthers := 0
	repC := make(chan *pb.RequestVoteReply, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sendout RV RPC
	for peerID := range rn.node.PeerConns {
		if peerID == rn.node.ID {
			continue
		}
		go func(ctx context.Context, rn *RaftNode, peerID uint64, repC chan *pb.RequestVoteReply) {
			conn := rn.node.PeerConns[peerID]
			peerNode := pb.NewRaftRPCClient(conn)
			rep, err := peerNode.RequestVote(ctx, &pb.RequestVoteRequest{
				From:         rn.node.ID,
				To:           peerID,
				Term:         rn.GetCurrentTerm(),
				LastLogIndex: rn.LastLogIndex(),
				LastLogTerm:  rn.GetLog(rn.LastLogIndex()).GetTerm(),
			})
			if err != nil {
				rn.log.Printf("[candidate %d]: error getting requestVote reply from node %d: %v\n", rn.node.ID, peerID, err)
				return
			}
			rn.log.Printf("[candidate %d]: got vote reply from node %d: %v\n", rn.node.ID, peerID, rep)
			repC <- rep
		}(ctx, rn, peerID, repC)
	}

	// reset timer
	electionTimer := time.NewTimer(randomizedTimeout(rn.electionTimeout))
	defer electionTimer.Stop()

	for {
		select {
		case rep := <-repC:
			if rep.GetTerm() > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(rep.GetTerm())
				rn.log.Printf("[candidate %d]: convert to follower\n", rn.node.ID)
				return rn.doFollower
			}
			if rep.GetVoteGranted() {
				numVotesFromOthers++
			} else {
				numRejsFromOthers++
			}
			if numVotesFromOthers+1 >= maj {
				return rn.doLeader
			}
			if numRejsFromOthers >= maj {
				return rn.doFollower
			}
			continue

		case _, ok := <-rn.proposeC:
			// Do nothing per ED post #466
			if !ok {
				rn.log.Printf("[candidate %d]: reading from a closed proposeC. initializing shut down...\n", rn.node.ID)
				rn.proposeC = nil
				rn.state = ExitState
				rn.Stop()
				close(rn.stopC)
				return nil
			}
			continue

		case appendEntriesMsg := <-rn.appendEntriesC:
			rn.log.Printf("[candidate %d]: got AppendEntries request: %v\n", rn.node.ID, appendEntriesMsg.request)
			success := false
			toFollower := false

			if appendEntriesMsg.request.Term > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(appendEntriesMsg.request.GetTerm())
				rn.setVotedFor(None)
			}

			if appendEntriesMsg.request.GetTerm() == rn.GetCurrentTerm() {
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
			rn.log.Printf("[candidate %d]: sending AppendEntries reply: %v\n", rn.node.ID, rep)
			appendEntriesMsg.reply <- rep

			if toFollower {
				return rn.doFollower
			}
			continue

		case requestVoteMsg := <-rn.requestVoteC:
			rn.log.Printf("[candidate %d]: got vote request: %v\n", rn.node.ID, requestVoteMsg.request)

			grantVote, toFollower := rn.handleReqVoteMsg(requestVoteMsg.request)

			requestVoteMsg.reply <- pb.RequestVoteReply{
				From:        rn.node.ID,
				To:          requestVoteMsg.request.From,
				Term:        rn.GetCurrentTerm(),
				VoteGranted: grantVote,
			}
			if toFollower {
				return rn.doFollower
			}
			continue

		default:
		}
		select {
		case <-electionTimer.C:
			rn.log.Printf("[candidate %d]: election timeout, re-launch the election\n", rn.node.ID)
			return rn.doCandidate
		default:
		}
	}
}
