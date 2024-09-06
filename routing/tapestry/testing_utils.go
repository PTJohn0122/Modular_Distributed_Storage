package tapestry

import (
	"errors"
	"fmt"
	"modist/orchestrator/node"
	pb "modist/proto"
	"strconv"
	"time"
)

var port = 10000

func generateTapestryAddrs(k int) []string {
	var peers []string
	for i := 0; i < k; i++ {
		peers = append(peers, fmt.Sprintf("localhost:%d", port))
		port++
	}
	return peers
}

func convertHexToUintIDs(ids ...string) (uintIds []uint64, err error) {
	for _, id := range ids {
		uintId, err := strconv.ParseUint(id, 16, 64)
		if err != nil {
			return nil, err
		}
		uintIds = append(uintIds, uintId)
	}
	return
}

// Creates a Tapestry mesh given ids as hex strings
func MakeTapestries(connectThem bool, ids ...string) ([]*TapestryNode, error) {
	uintIds, err := convertHexToUintIDs(ids...)
	if err != nil {
		return nil, err
	}

	nodes := node.CreateWithIds(generateTapestryAddrs(len(ids)), uintIds)
	tapestries := make([]*TapestryNode, 0, len(ids))
	for i := 0; i < len(ids); i++ {
		args := Args{
			Node: nodes[i],
		}

		if i > 0 && connectThem {
			args.Join = true
			args.ConnectTo = tapestries[0].Node.ID
		} else {
			args.Join = false
		}

		t := Configure(args)
		if t == nil {
			return tapestries, errors.New("Unable to start tapestry mesh")
		}
		tapestries = append(tapestries, t.tapestryNode)
		time.Sleep(10 * time.Millisecond)
	}
	return tapestries, nil
}

// Creates a Tapestry mesh given connectIds and delayIds as hex strings.
// delayIds is used if you want to set up RPC connections for certain node IDs but
// you want to handle their configuration manually later (see TestSampleTapestryAddNodes
// for an example).
func MakeTapestriesDelayConnecting(
	connectThem bool,
	connectIds []string,
	delayIds []string,
) ([]*TapestryNode, []*node.Node, error) {
	uintConnectIds, err := convertHexToUintIDs(connectIds...)
	if err != nil {
		return nil, nil, err
	}
	uintDelayIds, err := convertHexToUintIDs(delayIds...)
	if err != nil {
		return nil, nil, err
	}

	uintIds := append(uintConnectIds, uintDelayIds...)
	nodes := node.CreateWithIds(generateTapestryAddrs(len(connectIds)+len(delayIds)), uintIds)
	delayNodes := nodes[len(connectIds):]

	tapestries := make([]*TapestryNode, 0, len(connectIds))
	for i := 0; i < len(connectIds); i++ {
		args := Args{
			Node: nodes[i],
		}

		if i > 0 && connectThem {
			args.Join = true
			args.ConnectTo = tapestries[0].Node.ID
		} else {
			args.Join = false
		}

		t := Configure(args)
		if t == nil {
			return tapestries, delayNodes, errors.New("Unable to start tapestry mesh")
		}
		tapestries = append(tapestries, t.tapestryNode)
		time.Sleep(10 * time.Millisecond)
	}
	return tapestries, delayNodes, nil
}

func KillTapestries(ts ...*TapestryNode) {
	for _, t := range ts {
		t.log.Println("killing")
		t.Kill()
		t.log.Println("finished killing")
	}
}

// Creates a pb.IdMsg given an id as a hex string and a level
func CreateIDMsg(id string, level int32) *pb.IdMsg {
	return &pb.IdMsg{
		Id:    MakeIDFromHexString(id).String(),
		Level: level,
	}
}
