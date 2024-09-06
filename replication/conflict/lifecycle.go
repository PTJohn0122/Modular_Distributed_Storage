package conflict

import (
	"modist/orchestrator/node"
)

type DefaultLifecycle[T Clock] struct{}

func (d *DefaultLifecycle[T]) ReplicatorDidStart(node *node.Node) {}
func (d *DefaultLifecycle[T]) OnMessageReceive(clock T)           {}
func (d *DefaultLifecycle[T]) OnMessageSend()                     {}
func (d *DefaultLifecycle[T]) OnEvent()                           {}

type ReplicatorLifecycle[T Clock] interface {
	// Called synchronously when the replicator starts, before any messages have been sent or
	// received. The passed node is the node on which the replicator is running.
	ReplicatorDidStart(node *node.Node)

	// Called when any clock-carrying RPC is received. The passed clock will be the clock from the
	// received RPC.
	OnMessageReceive(clock T)

	// Called when any clock-carrying RPC is sent.
	OnMessageSend()

	// Called when a "notable" event happens on the server.
	OnEvent()
}
