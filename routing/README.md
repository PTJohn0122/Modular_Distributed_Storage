# Routing - Tapestry

In Modist, we use Distributed HashTables(DHT) to route between storage nodes. Each DHT entry represent the prefix and the address for node that stores the data with that prefix. Seraching cursor can jumping between nodes via DHT to find data. Thus each node can route to the target node without having a global information and saves costs in storing location information.

## Implementation Rules
By all the reading and understanding of materials, we defined serval rules in our implementations:
- All gRPC instances within for loops should be wrapped in a go routine function 
- If a function returns ```(&pb.Ok, err)```, we only return ```err``` when there is a RPC fail. This means a lost connection to the node with target ID.
- After a RPC instance failed and returned an ```err```, we should call removeBadNode(remoteID) to remove this broken ID from our local routing and backpointer tables. (Now only in Join() and AddMulticast())
- For AddNodeMultiCast, we added a base case when level > 40, so the transfer() of neighbors would be happen only on the last level. This can save transaction workflow. 

## Test Notes

Unit tests are implemented in `tapestry/sample_test.go`

- id_test.go: test the basic helper functions for id realted operations
- sample_test.go: testing the outmost functionalities of tapestry nodes