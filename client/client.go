package client

import (
	"context"
	"fmt"
	pb "modist/proto"

	"google.golang.org/grpc"
)

// Client allows users to interact with a Modist cluster.
//
// Internally, clients maintain connections to all necessary nodes in the cluster, and may
// make use of ample caching for performance.
//
// Clients should be initialized using New() and are not safe for concurrent use.
type Client struct {
	routers     []pb.RouterClient
	partitioner pb.PartitionerClient

	connections []*grpc.ClientConn

	// A map of keys to the latest clock value seen for that key. It is updated
	// with every message received from the client. It is not thread safe, since the client
	// is single-threaded.
	clocks map[string]*pb.Clock
}

// Consistency defines the consistency level of a read.
type Consistency string

const (
	ConsistencyLinearizable Consistency = "linearizable"
	ConsistencyCausal       Consistency = "causal"
	ConsistencyNone         Consistency = "none"
)

// GetOption represents a modification of a get request.
type GetOption func(*pb.GetRequest)

// New creates a new Modist client that can be used to issue commands. Addresses should be in the form
// of URLs.
func New(routerAddrs []string, partitionerAddr string) (*Client, error) {
	c := &Client{clocks: map[string]*pb.Clock{}}
	for _, addr := range routerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("unable to intialize a grpc connection to a router: %w", err)
		}
		c.connections = append(c.connections, conn)
		c.routers = append(c.routers, pb.NewRouterClient(conn))
	}

	conn, err := grpc.Dial(partitionerAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("unable to intialize a grpc connection to a partitioner: %w", err)
	}
	c.connections = append(c.connections, conn)
	c.partitioner = pb.NewPartitionerClient(conn)

	return c, nil
}

func (c *Client) getClock(key string) *pb.Clock {
	clock, ok := c.clocks[key]
	if !ok {
		return nil
	}
	return clock
}

// Get retrieves the value associated with a key. If the key does not exist, an error is returned.
func (c *Client) Get(ctx context.Context, key string, opts ...GetOption) (string, error) {

	replica, conn, rewrittenKey, err := c.getReplicaGroupForKey(ctx, key)
	if err != nil {
		return "", fmt.Errorf("unable to fetch the correct replica group: %w", err)
	}
	defer conn.Close()

	req := &pb.GetRequest{
		Key:      rewrittenKey,
		Metadata: &pb.GetMetadata{},
	}
	req.Metadata.Clock = c.getClock(key)
	for _, o := range opts {
		o(req)
	}

	reply, err := replica.GetReplicatedKey(ctx, req)

	if err != nil {
		return "", err
	}

	c.clocks[key] = reply.GetClock()
	return reply.GetValue(), nil
}

// Put sets a value for a given key. It will overwrite any existing value.
func (c *Client) Put(ctx context.Context, key string, value string) error {
	replica, conn, rewrittenKey, err := c.getReplicaGroupForKey(ctx, key)
	if err != nil {
		return fmt.Errorf("unable to fetch the correct replica group: %w", err)
	}
	defer conn.Close()

	reply, err := replica.ReplicateKey(ctx, &pb.PutRequest{Key: rewrittenKey, Value: value, Clock: c.getClock(key)})
	if err != nil {
		return err
	}

	c.clocks[key] = reply.GetClock()
	return nil
}

// getReplicaGroupForKey returns a grpc client for the replica group that is responsible for the
// specified key. This is done by first talking to the partitioner and then looking up the address(es)
// of the corresponding replica group using the routing layer. In addition, the raw grpc
// connection and the rewritten key from the partitioner are returned.
//
// The caller is responsible for closing the returned connection.
func (c *Client) getReplicaGroupForKey(
	ctx context.Context,
	key string,
) (pb.ReplicatorClient, *grpc.ClientConn, string, error) {
	partitionLookupReply, err := c.partitioner.Lookup(ctx, &pb.PartitionLookupRequest{Key: key})
	if err != nil {
		return nil, nil, "", fmt.Errorf("error looking up partition for key %q: %v", key, err)
	}

	replicaGroupId := partitionLookupReply.GetReplicaGroupId()
	replicaAddrs, err := c.routers[0].Lookup(ctx, &pb.RouteLookupRequest{Id: replicaGroupId})
	if err != nil {
		return nil, nil, "", fmt.Errorf(
			"error looking up replica address for replica group %d: %v",
			replicaGroupId,
			err,
		)
	}
	addr := replicaAddrs.Addrs[0]

	// Connect to the address
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, "", fmt.Errorf("error dialing addr %q: %v", addr, err)
	}

	return pb.NewReplicatorClient(conn), conn, partitionLookupReply.RewrittenKey, nil
}

// Close cleans up any resources used by the client.
func (c *Client) Close(ctx context.Context) error {
	var firstErr error
	for _, conn := range c.connections {
		err := conn.Close()

		// Continue closing connections even if one fails
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// WithConsistency specifies the consistency level of a read.
func WithConsistency(c Consistency) func(*pb.GetRequest) {
	return func(r *pb.GetRequest) {
		switch c {
		case ConsistencyLinearizable:
			r.Metadata.Consistency = pb.Consistency_LINEARIZABLE
		case ConsistencyCausal:
			r.Metadata.Consistency = pb.Consistency_CAUSAL
		case ConsistencyNone:
			r.Metadata.Consistency = pb.Consistency_NONE
		}
	}
}
