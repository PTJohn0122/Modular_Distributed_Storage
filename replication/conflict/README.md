# Conflict

Two types of clocks are implemented to adjust different needs. For many distributed systems, conflicts are the most common issue to resolve, especially when network segmentation or node offline happens. Utilizing different types of clocks provides a double guarantee for the linearisation of all updates.

## Physical Clock
A physical clock is a timestamp, usually a UNIX timestamp. It uses clock timestemp on each node for sychronization. 

## Vector Clock
A logical clock is some other data structure that is comparable with itself so that we can tell if one logical clock is "less than" another, "greater than" another, or the same as another.

## Test Note

Here are explainations for the unit tests in `physical_test.go` and `vector_test.go`.

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