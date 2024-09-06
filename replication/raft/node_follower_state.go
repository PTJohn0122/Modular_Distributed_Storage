package raft

import (
	"context"
	"fmt"
	pb "modist/proto"
	"time"
)

// doFollower implements the logic for a Raft node in the follower state.
func (rn *RaftNode) doFollower() stateFunction {
	rn.state = FollowerState
	rn.log.Printf("node %d transitioning to %s state at term %d", rn.node.ID, rn.state, rn.GetCurrentTerm())

	// initialization
	rn.setVotedFor(None)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	electionTimer := time.NewTimer(randomizedTimeout(rn.electionTimeout))
	defer electionTimer.Stop()

	for {
		select {
		case data, ok := <-rn.proposeC:
			if !ok {
				rn.log.Printf("[follower %d]: reading from a closed proposeC. initializing shut down...\n", rn.node.ID)
				// ref: https://stackoverflow.com/questions/43616434/closed-channel-vs-nil-channel
				// setting to nil basically disables this case statement. In a select statement, a nil channel will never be selected
				rn.proposeC = nil
				rn.state = ExitState
				rn.Stop()
				close(rn.stopC)
				return nil
			}

			if rn.leader != 0 {
				// rn.log.Printf("[follower %d]: forwarding proposal to leader(%d)...\n", rn.node.ID, rn.leader)
				fmt.Printf("[follower %d]: forwarding proposal to leader(%d)...\n", rn.node.ID, rn.leader)
				go func(ctx context.Context, rn *RaftNode, data []byte) {
					conn := rn.node.PeerConns[rn.leader]
					leaderNode := pb.NewRaftRPCClient(conn)
					_, err := leaderNode.Propose(ctx, &pb.ProposalRequest{
						From: rn.node.ID,
						To:   rn.leader,
						Data: data,
					})
					if err != nil {
						rn.log.Printf("RPC error from leader(%d).Propose: %s", rn.leader, err)
					}
				}(ctx, rn, data)
			}
			continue

		case requestVoteMsg := <-rn.requestVoteC:
			rn.log.Printf("[follower %d]: got vote request: %v\n", rn.node.ID, requestVoteMsg.request)

			grantVote, _ := rn.handleReqVoteMsg(requestVoteMsg.request)

			requestVoteMsg.reply <- pb.RequestVoteReply{
				From:        rn.node.ID,
				To:          requestVoteMsg.request.From,
				Term:        rn.GetCurrentTerm(),
				VoteGranted: grantVote,
			}
			continue

		case appendEntriesMsg := <-rn.appendEntriesC:
			rn.log.Printf("[follower %d]: got AppendEntries request: %v\n", rn.node.ID, appendEntriesMsg.request)
			if appendEntriesMsg.request.GetTerm() > rn.GetCurrentTerm() {
				rn.SetCurrentTerm(appendEntriesMsg.request.GetTerm())
				rn.setVotedFor(None)
			}

			success := false

			electionTimer.Stop()
			electionTimer.Reset(randomizedTimeout(rn.electionTimeout)) // from what i've read, the usage of Reset() is tricky

			if appendEntriesMsg.request.GetTerm() == rn.GetCurrentTerm() {
				rn.leader = appendEntriesMsg.request.GetFrom() // update leader when necessary
				success = rn.tryAppendToLocalLog(appendEntriesMsg.request)
				rn.applyLog()
			}

			rep := pb.AppendEntriesReply{
				From:    rn.node.ID,
				To:      appendEntriesMsg.request.From,
				Term:    rn.GetCurrentTerm(),
				Success: success,
			}
			rn.log.Printf("[follower %d]: sending AppendEntries reply: %v\n", rn.node.ID, rep)
			appendEntriesMsg.reply <- rep
			continue

		default:
		}
		select {
		case <-electionTimer.C:
			rn.log.Printf("[follower %d]: leader(%d) timeout. switching into candidate...\n", rn.node.ID, rn.leader)
			return rn.doCandidate
		default:
		}
	}
}
