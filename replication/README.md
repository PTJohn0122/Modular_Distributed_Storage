# Replication

Modist provides two replication methods, `Leaderless` and `Raft`.

## Leaderless
Leaderless replication is a decentralized approach that uses consistent hashing for data distribution across nodes. Each node is assigned a range of hashes, creating a ring of nodes. Data items are assigned a hash, and each item is stored in the node corresponding to its hash range.

## Raft
Raft is a consensus algorithm that provides a strongly consistent replication method with a leader-based approach.

In Raft, one node is elected as the leader, which is responsible for managing all write requests to maintain strict consistency. Other nodes act as followers.
During regular intervals, followers send heartbeats to indicate their status. If a follower fails to receive a heartbeat, it assumes the leader is down and starts an election to select a new leader.

### Leaderless
- TestHandlePeerWrite: Test 4 cases: 1) a new KV with a new key, 2) a concurrent KV with larger lexically value, 3) an outdated KV, 4) a newer KV

### Raft
- Test_Lookup_SimpleIdentity： test the basic operation of lookup function, including specific number and new number falls into intervals.

- Test_AddReplicaGroup: test the adding of groups with lookup checks and nodes number check

- Test_RemoveReplicaGroup: test the removal of groups with lookup checks and nodes number check 

- Test_RemoveReplicaGroup_Empt: test the removal of groups when the hash cycle is empty

- Test_AddRemoveReplicaGroup: test the basic operations of adding and removing replica groups, including adding, checking cycle length, removing exist group and unexist group. 