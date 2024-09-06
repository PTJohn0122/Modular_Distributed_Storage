package tapestry

import (
	"bytes"
	"context"
	"fmt"
	pb "modist/proto"
	"testing"
	"time"
)

func TestSampleTapestrySetup(t *testing.T) {
	tap, _ := MakeTapestries(true, "1", "3", "5", "7") //Make a tapestry with these ids
	fmt.Printf("length of tap %d\n", len(tap))
	KillTapestries(tap[1], tap[2]) //Kill off two of them.
	resp, _ := tap[0].FindRoot(
		context.Background(),
		CreateIDMsg("2", 0),
	) //After killing 3 and 5, this should route to 7
	if resp.Next != tap[3].Id.String() {
		t.Errorf("Failed to kill successfully")
	}

}

func TestSampleTapestrySearch(t *testing.T) {
	tap, _ := MakeTapestries(true, "100", "456", "1234") //make a sample tap
	tap[1].Store("look at this lad", []byte("an absolute unit"))
	result, err := tap[0].Get("look at this lad") //Store a KV pair and try to fetch it
	fmt.Println(err)
	if !bytes.Equal(result, []byte("an absolute unit")) { //Ensure we correctly get our KV
		t.Errorf("Get failed")
	}
}

func TestSampleTapestryAddNodes(t *testing.T) {
	// Need to use this so that gRPC connections are set up correctly
	tap, delayNodes, _ := MakeTapestriesDelayConnecting(
		true,
		[]string{"1", "5", "9"},
		[]string{"8", "12"},
	)

	// Add some tap nodes after the initial construction
	for _, delayNode := range delayNodes {
		args := Args{
			Node:      delayNode,
			Join:      true,
			ConnectTo: tap[0].RetrieveID(tap[0].Id),
		}
		tn := Configure(args).tapestryNode
		tap = append(tap, tn)
		time.Sleep(1000 * time.Millisecond) //Wait for availability
	}

	resp, _ := tap[1].FindRoot(context.Background(), CreateIDMsg("7", 0))
	if resp.Next != tap[3].Id.String() {
		t.Errorf("Addition of node failed")
	}
}

func TestStoreRemove(t *testing.T) {
	tap, _ := MakeTapestries(true, "2", "4", "6") //Make a tapestry with these ids
	tap[1].Store("44", []byte("4444"))
	result, err := tap[0].Get("44")
	fmt.Println(err)
	if !bytes.Equal(result, []byte("4444")) {
		t.Errorf("Get failed")
	}
	tap[1].Remove("44")
	_, err = tap[0].Get("4")
	if err == nil {
		t.Errorf("Get should return err")
	}
}

func TestTransfer(t *testing.T) {
	tap, _ := MakeTapestries(true, "2222")
	needTransfer := make(map[string]*pb.Neighbors)
	needTransfer["hello"] = &pb.Neighbors{Neighbors: []string{MakeID(1111).String()}}
	ok, _ := tap[0].Transfer(nil, &pb.TransferData{
		From: "aserag",
		Data: needTransfer,
	})
	if ok.Ok != false {
		t.Errorf("from arg is wrong. pb.Ok should be false")
	}
	ok, _ = tap[0].Transfer(nil, &pb.TransferData{
		From: MakeID(1111).String(),
		Data: needTransfer,
	})
	if ok.Ok != true {
		t.Errorf("pb.Ok should be true")
	}
}

func TestSampleTapstryLargScale(t *testing.T) {
	tapIds := make([]string, 0)
	for i := 0; i < 20; i++ {
		tapIds = append(tapIds, fmt.Sprint(2*i))
	}
	tap, _ := MakeTapestries(true, tapIds...)
	tap[5].RemoveBackpointer(context.Background(), &pb.NodeMsg{Id: "12"})
	KillTapestries(tap[8:]...)
	resp, _ := tap[5].FindRoot(context.Background(), CreateIDMsg("12", 0))
	fmt.Println(tap[5].Id, resp)
}

func TestSampleTapstryExit(t *testing.T) {
	tap, _ := MakeTapestries(true, "32", "64", "128", "256", "512", "1024")
	tap[1].Store("Some Key", []byte("Some Value"))

	resp1, _ := tap[0].FindRoot(context.Background(), CreateIDMsg("64", 0))
	fmt.Println("Find root on tap 0 for Id = 64, result is", resp1)

	tap[1].Leave()
	tap[2].Leave()

	resp2, _ := tap[0].FindRoot(context.Background(), CreateIDMsg("64", 0))
	fmt.Println("Find root on tap 0 for Id = 64 after remove node 64 and node 128, result is", resp2)
	if resp1.Next == resp2.Next {
		t.Error("Nodes were not removed from system")
	}

	getRsp, err := tap[1].Get("Some Key")
	if bytes.Equal(getRsp, []byte("Some Value")) {
		t.Errorf("Still can get the value after remove node")
	}
	if err == nil {
		t.Errorf("Node 64 not removed")
	}

}

func TestRepublish(t *testing.T) {
	tap, _ := MakeTapestries(true, "66666")
	done, err := tap[0].Publish("republishkey")
	if err != nil {
		t.Errorf("got error from Publish")
	}
	time.Sleep(REPUBLISH)
	time.Sleep(2000 * time.Millisecond)
	done <- true
}
