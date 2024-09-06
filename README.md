# modist
Created by Yutong Li(yli195) and Xingjian Hao(xhao9).

## Known Bugs
No know bugs detected thus far.

## Extra Features
No extra feature implemented thus far.

## Tests Overview

### Clocks & Conflicts 
- TestPhysicalConcurrentEventsHappenBefore: Test the happensBefore relationship between zero clock, 3 clocks with random timestamp, and a new clock

- TestPhysicalConcurrentEventsHappenConcurrently: Test the happensBefore relationship between 2 physical clocks with the same timestamp

- TestPhysicalClockConflictResolvingNoConflict: Test the conflict resolver when no args are passed in

- TestPhysicalClockConflictResolving: Test basic conflict resolving with 3 KVs with the same timestamp and different values

- TestMessageReceive: Test the happensBefore relation of a vector clock when receive a message. Two clocks are received and the vector should retain the one with highest value.

- TestMessageSend: Test the incrementation of vectors when send a message.

- TestNewClock: Test the creation of a new clock based on the current clock. The new clock should be a deep-copy of the original clock.

- TestResolveConflict: Test the clock merging when there is a confliction of vectors for multiple incoming clocks.

- TestHappenBeforeHeteroNodeIDCase: Test the happenBefore relationship when two conflict vectors have values on different nodeID.

- func TestHappenBeforeEmptyCase: Test the happenBefore relationship when an empty vector meet a all-zero vector.

### Leaderless
- TestHandlePeerWrite: Test 4 cases: 1) a new KV with a new key, 2) a concurrent KV with larger lexically value, 3) an outdated KV, 4) a newer KV

### Partitioning
- Test_Lookup_SimpleIdentity： test the basic operation of lookup function, including specific number and new number falls into intervals.

- Test_AddReplicaGroup: test the adding of groups with lookup checks and nodes number check

- Test_RemoveReplicaGroup: test the removal of groups with lookup checks and nodes number check 

- Test_RemoveReplicaGroup_Empt: test the removal of groups when the hash cycle is empty

- Test_AddRemoveReplicaGroup: test the basic operations of adding and removing replica groups, including adding, checking cycle length, removing exist group and unexist group. 

### Tapestry 

#### Implementation Rules
By all the reading and understanding of materials, we defined serval rules in our implementations:
- All gRPC instances within for loops should be wrapped in a go routine function 
- If a function returns ```(&pb.Ok, err)```, we only return ```err``` when there is a RPC fail. This means a lost connection to the node with target ID.
- After a RPC instance failed and returned an ```err```, we should call removeBadNode(remoteID) to remove this broken ID from our local routing and backpointer tables. (Now only in Join() and AddMulticast())
- For AddNodeMultiCast, we added a base case when level > 40, so the transfer() of neighbors would be happen only on the last level. This can save transaction workflow. 

#### Testing Specifics
- id_test.go: test the basic helper functions for id realted operations
- sample_test.go: testing the outmost functionalities of tapestry nodes