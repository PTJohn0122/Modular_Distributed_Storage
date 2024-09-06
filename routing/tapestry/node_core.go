/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines functions to publish and lookup objects in a Tapestry mesh
 */

package tapestry

import (
	"context"
	"errors"
	"fmt"
	pb "modist/proto"
	"time"
)

// Store a blob on the local node and publish the key to the tapestry.
func (local *TapestryNode) Store(key string, value []byte) (err error) {
	// local.log.Println("[Store] Node ", local.Id, "is storing key", key, ": value", value)
	done, err := local.Publish(key)
	if err != nil {
		return err
	}
	local.blobstore.Put(key, value, done)
	// local.log.Println("[Store] finished store")
	return nil
}

// Get looks up a key in the tapestry then fetch the corresponding blob from the
// remote blob store.
func (local *TapestryNode) Get(key string) ([]byte, error) {
	// local.log.Println("[Get] Node ", local.Id, "is getting the key", key)
	// Lookup the key
	routerIds, err := local.Lookup(key)
	if err != nil {
		return nil, err
	}
	if len(routerIds) == 0 {
		return nil, fmt.Errorf("No routers returned for key %v", key)
	}

	// Contact router
	keyMsg := &pb.TapestryKey{
		Key: key,
	}

	var errs []error
	for _, routerId := range routerIds {
		conn := local.Node.PeerConns[local.RetrieveID(routerId)]
		router := pb.NewTapestryRPCClient(conn)
		resp, err := router.BlobStoreFetch(context.Background(), keyMsg)
		if err != nil {
			errs = append(errs, err)
		} else if resp.Data != nil {
			return resp.Data, nil
		}
	}

	return nil, fmt.Errorf("Error contacting routers, %v: %v", routerIds, errs)
}

// Remove the blob from the local blob store and stop advertising
func (local *TapestryNode) Remove(key string) bool {
	// local.log.Println("[Store] Node ", local.Id, "is removing key", key)
	return local.blobstore.Delete(key)
}

// Publishes the key in tapestry.
//
// - Start periodically publishing the key. At each publishing:
//   - Find the root node for the key
//   - Register the local node on the root
//   - if anything failed, retry; until RETRIES has been reached.
//
// - Return a channel for cancelling the publish
//   - if receiving from the channel, stop republishing
//
// Some note about publishing behavior:
//   - The first publishing attempt should attempt to retry at most RETRIES times if there is a failure.
//     i.e. if RETRIES = 3 and FindRoot errored or returned false after all 3 times, consider this publishing
//     attempt as failed. The error returned for Publish should be the error message associated with the final
//     retry.
//   - If any of these attempts succeed, you do not need to retry.
//   - In addition to the initial publishing attempt, you should repeat this entire publishing workflow at the
//     appropriate interval. i.e. every 5 seconds we attempt to publish, and THIS publishing attempt can either
//     succeed, or fail after at most RETRIES times.
//   - Keep trying to republish regardless of how the last attempt went
func (local *TapestryNode) Publish(key string) (chan bool, error) {
	var err error
	var rootMsg *pb.RootMsg
	var rootId ID
	var ok *pb.Ok
	// local.log.Println("[Publish] node", local.Id, " is publishing key", key)
	for i := 0; i < RETRIES; i++ {
		rootMsg, err = local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
		if err != nil {
			continue
		}
		rootId, err = ParseID(rootMsg.Next)
		if err != nil {
			continue
		}

		conn := local.Node.PeerConns[local.RetrieveID(rootId)]
		root := pb.NewTapestryRPCClient(conn)
		ok, err = root.Register(context.Background(), &pb.Registration{
			FromNode: local.Id.String(),
			Key:      key,
		})
		if err != nil {
			local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{rootId.String()}})
			continue
		}
		if !ok.Ok {
			continue
		}
		break
	}

	if err != nil {
		return nil, err
	}

	stopChan := make(chan bool)
	go func(key string, stopChan chan bool) {
		repubTicker := time.NewTicker(REPUBLISH)
		defer repubTicker.Stop()
		for {
			select {
			case <-stopChan:
				return
			case <-repubTicker.C:
				var err error
				var rootMsg *pb.RootMsg
				var rootId ID
				var ok *pb.Ok
				for i := 0; i < RETRIES; i++ {
					rootMsg, err = local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
					if err != nil {
						continue
					}
					rootId, err = ParseID(rootMsg.Next)
					if err != nil {
						continue
					}

					conn := local.Node.PeerConns[local.RetrieveID(rootId)]
					root := pb.NewTapestryRPCClient(conn)
					ok, err = root.Register(context.Background(), &pb.Registration{
						FromNode: local.Id.String(),
						Key:      key,
					})
					if err != nil {
						local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{rootId.String()}})
						continue
					}
					if !ok.Ok {
						continue
					}
					break
				}
			}
		}
	}(key, stopChan)

	return stopChan, nil
}

// Lookup look up the Tapestry nodes that are storing the blob for the specified key.
//
// - Find the root node for the key
// - Fetch the routers (nodes storing the blob) from the root's location map
// - Attempt up to RETRIES times
func (local *TapestryNode) Lookup(key string) ([]ID, error) {
	var err error
	var rootMsg *pb.RootMsg
	var rootId ID
	var fetchedLocations *pb.FetchedLocations
	var result []ID
	// local.log.Println("[Lookup] Node ", local.Id, "is looking up key", key)
	for i := 0; i < RETRIES; i++ {
		rootMsg, err = local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
		if err != nil {
			continue
		}
		rootId, err = ParseID(rootMsg.Next)
		if err != nil {
			continue
		}

		conn := local.Node.PeerConns[local.RetrieveID(rootId)]
		root := pb.NewTapestryRPCClient(conn)
		fetchedLocations, err = root.Fetch(context.Background(), &pb.TapestryKey{Key: key})
		if err != nil {
			local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{rootId.String()}})
			continue
		}
		if !fetchedLocations.IsRoot {
			continue
		}

		result, err = stringSliceToIds(fetchedLocations.Values)
		if err != nil {
			continue
		}
		return result, nil
	}

	if err != nil {
		return nil, err
	}
	return nil, errors.New("something went wrong but no err msg collected")
}

// FindRoot returns the root for the id in idMsg by recursive RPC calls on the next hop found in our routing table
//   - find the next hop from our routing table
//   - call FindRoot on nextHop
//   - if failed, add nextHop to toRemove, remove them from local routing table, retry
func (local *TapestryNode) FindRoot(ctx context.Context, idMsg *pb.IdMsg) (*pb.RootMsg, error) {
	// local.log.Println("[FindRoot] Node ", local.Id, "is finding root for node", idMsg.Id)
	id, err := ParseID(idMsg.Id)
	if err != nil {
		return nil, err
	}

	level := idMsg.Level
	toRemove := make([]ID, 0)
	nextHopId := local.Table.FindNextHop(id, level)

	for nextHopId != local.Id {
		conn := local.Node.PeerConns[local.RetrieveID(nextHopId)]
		nextHop := pb.NewTapestryRPCClient(conn)
		rootMsg, err := nextHop.FindRoot(context.Background(), &pb.IdMsg{
			Id:    id.String(),
			Level: level + 1,
		})
		if err != nil {
			toRemove = append(toRemove, nextHopId)
			local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{nextHopId.String()}})
			nextHopId = local.Table.FindNextHop(id, level)
		} else {
			stringToRemove := UniqueResults(idsToStringSlice(toRemove), rootMsg.ToRemove)
			local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: stringToRemove})

			return &pb.RootMsg{
				Next:     rootMsg.Next,
				ToRemove: stringToRemove,
			}, nil
		}
	}

	// local node is the root node
	return &pb.RootMsg{
		Next:     local.Id.String(),
		ToRemove: idsToStringSlice(toRemove),
	}, nil
}

// The node that stores some data with key is registering themselves to us as an advertiser of the key.
// - Check that we are the root node for the key, return true in pb.Ok if we are
// - Add the node to the location map (local.locationsByKey.Register)
//   - local.locationsByKey.Register kicks off a timer to remove the node if it's not advertised again
//     after TIMEOUT
func (local *TapestryNode) Register(
	ctx context.Context,
	registration *pb.Registration,
) (*pb.Ok, error) {
	// local.log.Println("[Register] Node ", local.Id, "is registering key", registration.Key)
	from, err := ParseID(registration.FromNode)
	if err != nil {
		return nil, err
	}

	key := registration.Key

	rootMsg, err := local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
	if err != nil {
		return nil, err
	}
	root, err := ParseID(rootMsg.Next)
	if err != nil {
		return nil, err
	}
	if root != local.Id {
		return &pb.Ok{Ok: false}, nil
	}

	local.LocationsByKey.Register(key, from, TIMEOUT)
	return &pb.Ok{Ok: true}, nil
}

// Fetch checks that we are the root node for the requested key and
// return all nodes that are registered in the local location map for this key
func (local *TapestryNode) Fetch(
	ctx context.Context,
	key *pb.TapestryKey,
) (*pb.FetchedLocations, error) {
	// local.log.Println("[Fetch] Node ", local.Id, "is fetching key", key.Key)
	rootMsg, err := local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key.Key).String(), Level: 0})
	if err != nil {
		return nil, err
	}
	root, err := ParseID(rootMsg.Next)
	if err != nil {
		return nil, nil
	}

	if root != local.Id {
		return &pb.FetchedLocations{
			IsRoot: false,
			Values: make([]string, 0),
		}, nil
	}
	return &pb.FetchedLocations{
		IsRoot: true,
		Values: idsToStringSlice(local.LocationsByKey.Get(key.Key)),
	}, nil
}

// Retrieves the blob corresponding to a key
func (local *TapestryNode) BlobStoreFetch(
	ctx context.Context,
	key *pb.TapestryKey,
) (*pb.DataBlob, error) {
	// local.log.Println("[BlobStoreFetch] node", local.Id, " is blob fetching key", key.Key)
	data, isOk := local.blobstore.Get(key.Key)

	var err error
	if !isOk {
		err = errors.New("Key not found")
	}

	return &pb.DataBlob{
		Key:  key.Key,
		Data: data,
	}, err
}

// Transfer registers all of the provided objects in the local location map. (local.locationsByKey.RegisterAll)
// If appropriate, add the from node to our local routing table
func (local *TapestryNode) Transfer(
	ctx context.Context,
	transferData *pb.TransferData,
) (*pb.Ok, error) {
	from, err := ParseID(transferData.From)
	if err != nil {
		return &pb.Ok{Ok: false}, nil
	}

	nodeMap := make(map[string][]ID)
	for key, set := range transferData.Data {
		nodeMap[key], err = stringSliceToIds(set.Neighbors)
		if err != nil {
			return &pb.Ok{Ok: false}, nil
		}
	}

	local.LocationsByKey.RegisterAll(nodeMap, TIMEOUT)
	local.Table.Add(from) // TODO: Q: need to check if there's an evicted node?

	return &pb.Ok{Ok: true}, nil
}

// calls FindRoot on a remote node to find the root of the given id
func (local *TapestryNode) FindRootOnRemoteNode(remoteNodeId ID, id ID) (*ID, error) {
	conn := local.Node.PeerConns[local.RetrieveID(remoteNodeId)]
	remoteNode := pb.NewTapestryRPCClient(conn)
	rootMsg, err := remoteNode.FindRoot(context.Background(), &pb.IdMsg{
		Id:    id.String(),
		Level: 0,
	})
	if err != nil {
		local.RemoveBadNodes(context.Background(), &pb.Neighbors{Neighbors: []string{remoteNodeId.String()}})
		return nil, err
	}

	root, err := ParseID(rootMsg.Next)
	if err != nil {
		return nil, err
	}
	return &root, nil
}
