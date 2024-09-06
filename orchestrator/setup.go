package orchestrator

import (
	"context"
	"fmt"
	"math/rand"
	"modist/client"
	"modist/orchestrator/node"
	"modist/partitioning"
	"modist/partitioning/localpartitioner"
	pb "modist/proto"
	"modist/replication/conflict"
	"modist/replication/leaderless"
	"modist/replication/raft"
	"modist/routing/localrouter"
	"modist/routing/tapestry"
	"modist/store"
	"time"
)

type Config struct {
	Routing struct {
		Nodes int
		Mode  string // local
	}
	Partitioning []struct {
		Name                       string
		AssignmentMode             string `yaml:"assignment_mode"` // consistent_hash, single
		ConsistentHashVirtualNodes int    `yaml:"consistent_hash_virtual_nodes"`

		// The names of replica groups to which this partitioner should be connected
		ReplicaGroups []string `yaml:"replica_groups"`
	}
	Replication []struct {
		Name  string
		Nodes int
		Mode  string // leaderless, raft
	}
}

type ReplicaGroup struct {
	ID    uint64
	Nodes []*node.Node
}

type Handler struct {
	routerNodes []*node.Node

	// Map from the partitioner friendly name to the corresponding node
	partitionerNodes map[string]*node.Node

	// Map from replica group friendly name to the corresponding nodes
	replicatorNodes map[string]ReplicaGroup
}

func generateAddrs(k, startingPort int) []string {
	var peers []string
	for i := 0; i < k; i++ {
		port := startingPort + i
		peers = append(peers, fmt.Sprintf("127.0.0.1:%d", port))
	}
	return peers
}

func New(ctx context.Context, cfg Config) (*Handler, error) {
	h := &Handler{
		partitionerNodes: map[string]*node.Node{},
		replicatorNodes:  map[string]ReplicaGroup{},
	}

	h.createNodes(cfg)

	routerServers := h.configureRouters(cfg)

	h.configureReplicators(ctx, cfg, routerServers)

	h.configurePartitioners(ctx, cfg, routerServers)

	return h, nil
}

// configureRouters initializes the routing system.
func (h *Handler) configureRouters(cfg Config) []pb.RouterServer {
	var routerServers []pb.RouterServer
	for i := 0; i < len(h.routerNodes); i++ {
		var server pb.RouterServer

		switch cfg.Routing.Mode {
		case "local":
			server = localrouter.Configure(localrouter.Args{Node: h.routerNodes[i]})
		case "tapestry":
			if i == 0 {
				server = tapestry.Configure(tapestry.Args{Node: h.routerNodes[i], Join: false})
			} else {
				server = tapestry.Configure(tapestry.Args{
					Node:      h.routerNodes[i],
					Join:      true,
					ConnectTo: h.routerNodes[0].ID,
				})
			}
		}

		routerServers = append(routerServers, server)
	}

	return routerServers
}

// configurePartitioners initializes the partitioning system.
//
// If first configures the assignment mode for each partitioner and starts up the
// grpc server. It then registers the relevant replica groups. Finally, it registers
// the partitioners with the routing layer.
func (h *Handler) configurePartitioners(
	ctx context.Context,
	cfg Config,
	routerServers []pb.RouterServer,
) {
	var partitionerServer pb.PartitionerServer
	for _, partitioner := range cfg.Partitioning {
		partitionerNode := h.partitionerNodes[partitioner.Name]

		var assigner partitioning.Assigner
		switch partitioner.AssignmentMode {
		case "single":
			assigner = partitioning.NewSingle()
		case "consistent_hash":
			virtualNodes := partitioner.ConsistentHashVirtualNodes
			if virtualNodes <= 0 {
				panic("partitioning.consistent_hash_virtual_nodes must be greater than 0")
			}
			assigner = partitioning.NewConsistentHash(virtualNodes)
		}

		partitionerServer = localpartitioner.Configure(localpartitioner.Args{
			Node:     partitionerNode,
			Assigner: assigner,
		})

		for _, clusterName := range partitioner.ReplicaGroups {
			partitionerServer.RegisterReplicaGroup(
				ctx,
				&pb.RegisterReplicaGroupRequest{Id: h.replicatorNodes[clusterName].ID},
			)
		}

		serverNum := rand.Intn(len(routerServers))
		routerServers[serverNum].Publish(ctx, &pb.RoutePublishRequest{
			Id:   partitionerNode.ID,
			Addr: partitionerNode.Addr.Host,
		})
	}
}

// configureReplicators initializes the replication system.
//
// It configures the replication mode and starts the grpc server for each node
// in the cluster. It also registers each node with the routing layer (with the id
// of the overall group).
func (h *Handler) configureReplicators(
	ctx context.Context,
	cfg Config,
	routerServers []pb.RouterServer,
) {
	for _, replicator := range cfg.Replication {
		cluster := h.replicatorNodes[replicator.Name]

		switch replicator.Mode {
		case "leaderless":
			quorum := (len(cluster.Nodes) / 2) + 1

			for _, node := range cluster.Nodes {
				leaderless.Configure[conflict.VersionVectorClock](
					leaderless.Args[conflict.VersionVectorClock]{
						Node:             node,
						W:                quorum,
						R:                quorum,
						ConflictResolver: conflict.NewVersionVectorConflictResolver(),
						LocalStore:       &store.Memory[*conflict.KV[conflict.VersionVectorClock]]{},
					},
				)
			}
		case "raft":
			// err := os.Mkdir("raft-log", 0777)
			// if err != nil && !os.IsExist(err) {
			// log.Fatal(err)
			// }

			for _, node := range cluster.Nodes {
				config := &raft.Config{
					ElectionTimeout:  time.Millisecond * 150,
					HeartbeatTimeout: time.Millisecond * 50,
					// Storage: raft.NewBoltStore(
					// filepath.Join("raft-log", fmt.Sprintf("raft%s", node.Addr.Host)),
					// ),
					Storage: raft.NewMemoryStore(),
				}

				raft.Configure(raft.Args{
					Node:   node,
					Config: config,
				})
			}
		default:
			panic(fmt.Errorf("unsupported replication mode %v", replicator.Mode))
		}

		// Register each node with the routing layer
		for _, node := range cluster.Nodes {
			serverNum := rand.Intn(len(routerServers))
			routerServers[serverNum].Publish(ctx, &pb.RoutePublishRequest{
				Id:   cluster.ID,
				Addr: node.Addr.Host,
			})
		}
	}
}

// createNodes instantiates nodes for all specified portions of the system.
//
// It first generates a list of addresses to use, and then creates nodes for each of them.
func (h *Handler) createNodes(cfg Config) {
	totalNodes := cfg.Routing.Nodes + len(cfg.Partitioning)

	for _, r := range cfg.Replication {
		totalNodes += r.Nodes
	}
	peers := generateAddrs(totalNodes, 5000)

	// Create nodes for all services
	i := 0
	routerAddrs := peers[i : i+cfg.Routing.Nodes]
	i += cfg.Routing.Nodes
	partitionerAddrs := peers[i : i+len(cfg.Partitioning)]
	i += len(cfg.Partitioning)

	var replicatorAddrs [][]string
	for _, r := range cfg.Replication {
		replicatorAddrs = append(replicatorAddrs, peers[i:i+r.Nodes])
		i += r.Nodes
	}

	h.routerNodes = node.Create(routerAddrs)
	partitionerNodes := node.Create(partitionerAddrs)
	for j, p := range partitionerNodes {
		h.partitionerNodes[cfg.Partitioning[j].Name] = p
	}

	for j := 0; j < len(cfg.Replication); j++ {
		h.replicatorNodes[cfg.Replication[j].Name] = ReplicaGroup{
			Nodes: node.Create(replicatorAddrs[j]),
			ID:    node.ID(),
		}
	}
}

// Client returns a client that uses the specified partitioner.
func (h *Handler) Client(partitionerName string) (*client.Client, error) {
	p, ok := h.partitionerNodes[partitionerName]
	if !ok {
		return nil, fmt.Errorf("partitioner with name %q does not exist", partitionerName)
	}

	var routerAddrs []string
	for _, r := range h.routerNodes {
		routerAddrs = append(routerAddrs, r.Addr.Host)
	}

	return client.New(routerAddrs, p.Addr.Host)
}

func (h *Handler) Close() error {
	return nil
}
