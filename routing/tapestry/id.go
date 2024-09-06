/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines IDs for tapestry and provides various utility functions
 *  for manipulating and creating them. Provides functions to compare IDs
 *  for insertion into routing tables, and for implementing the routing
 *  algorithm.
 */

package tapestry

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/big"
	"strconv"
)

// An ID is a digit array
type ID [DIGITS]Digit

// Digit is just a typedef'ed uint8
type Digit uint8

// MakeID creates an ID given a uint64
// Used to convert node.ID to TapestryNode.Id
func MakeID(id uint64) (convertedId ID) {
	stringId := strconv.FormatUint(id, 16)
	for i := 0; i < DIGITS && i < len(stringId); i++ {
		d, err := strconv.ParseInt(stringId[i:i+1], 16, 0)
		if err != nil {
			return convertedId
		}
		convertedId[i] = Digit(d)
	}
	for i := len(stringId); i < DIGITS; i++ {
		convertedId[i] = Digit(0)
	}

	return convertedId
}

// Uses MakeID to create ID from hex string
// Used for testing purposes
func MakeIDFromHexString(id string) (convertedID ID) {
	parsedId, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return
	}

	return MakeID(parsedId)
}

// RetrieveID converts an ID to a uint64.
// Used to convert TapestryNode.Id to node.ID. Note that an ID of 100... may correspond to a
// uint64 of 1, 10, 100, etc. but we only want to select ones that are valid IDs for peer nodes.
func (local *TapestryNode) RetrieveID(id ID) (convertedID uint64) {
	var stringId string

	// Find position of last digit that is not 0
	j := DIGITS - 1
	for j >= 0 && id[j] == 0 {
		j--
	}

	for k := 0; k < DIGITS; k++ {
		d := strconv.FormatUint(uint64(id[k]), 16)
		stringId += d

		if k >= j {
			// For each possible ID, check if the converted ID is in PeerNodes
			convertedId, _ := strconv.ParseUint(stringId, 16, 64)
			if _, ok := local.Node.PeerNodes[convertedId]; ok {
				return convertedId
			}
		}
	}

	return 0
}

// Hash hashes the string to an ID
func Hash(key string) (id ID) {
	// Sha-hash the key
	sha := sha1.New()
	sha.Write([]byte(key))
	hash := sha.Sum([]byte{})

	// Store in an ID
	for i := range id {
		id[i] = Digit(hash[(i/2)%len(hash)])
		if i%2 == 0 {
			id[i] >>= 4
		}
		id[i] %= BASE
	}

	return id
}

// SharedPrefixLength returns the length of the prefix that is shared by the two IDs.
func SharedPrefixLength(a ID, b ID) (i int) {
	// TODO(students): [Tapestry] Implement me!
	prefixLen := 0
	for idx := 0; idx < DIGITS; idx++ {
		if a[idx] == b[idx] {
			prefixLen++
		} else {
			break
		}
	}
	return prefixLen
}

// Used by Tapestry's surrogate routing.  Given IDs newId and currentId, will id now route
// to newId?
//
// In our surrogate routing, we move right from the missing cell until we find a non-missing cell
// with a node.
//
// The better choice in routing between newId and currentId is the id that:
//   - has the longest shared prefix with id
//   - if both have prefix of length n, which input has a better (n+1)th digit?
//   - if both have the same (n+1)th digit, consider (n+2)th digit, etc.
//
// We define a "better" digit as the closer digit when moving right from id.
// HINT: We can use MOD for this comparison.
//
// IsNewRoute returns true if newId is the better choice.
// Returns false if currentId is the better choice or if newId == currentId.
func (id ID) IsNewRoute(newId ID, currentId ID) bool {
	// TODO(students): [Tapestry] Implement me!
	newPrefixLen := SharedPrefixLength(id, newId)
	currPrefixLen := SharedPrefixLength(id, currentId)
	if newPrefixLen > currPrefixLen {
		return true
	} else if newPrefixLen == currPrefixLen {
		for i := newPrefixLen; i < DIGITS; i++ {
			if newId[i] != currentId[i] {
				return (newId[i]-id[i])%BASE < (currentId[i]-id[i])%BASE
			}
		}
		// if id.Closer(newId, currentId) {
		// 	fmt.Println("[IsNewRoute], newId is closer", "newId =", newId, "currId =", currentId)
		// 	return true
		// }
		// fmt.Println("[IsNewRoute], currentId is closer", "newId =", newId, "currId =", currentId)
	}
	return false
}

// Closer is used when inserting nodes into Tapestry's routing table.  If the routing
// table has multiple candidate nodes for a slot, then it chooses the node that
// is closer to the local node.
//
// In a production Tapestry implementation, closeness is determined by looking
// at the round-trip-times (RTTs) between (a, id) and (b, id); the node with the
// shorter RTT is closer.
//
// In this implementation, we have decided to define closeness as the absolute
// value of the difference between a and b. This is NOT the same as your
// implementation of BetterChoice.
//
// Return true if a is closer than b.
// Return false if b is closer than a, or if a == b.
func (id ID) Closer(a ID, b ID) bool {
	// TODO(students): [Tapestry] Implement me!
	absId := id.Big()
	absA := a.Big()
	absB := b.Big()
	return big.NewInt(0).Sub(absId, absA).CmpAbs(big.NewInt(0).Sub(absId, absB)) < 0
}

// Helper function: convert an ID to a big int.
func (id ID) Big() (b *big.Int) {
	b = big.NewInt(0)
	base := big.NewInt(BASE)
	for _, digit := range id {
		b.Mul(b, base)
		b.Add(b, big.NewInt(int64(digit)))
	}
	return b
}

// String representation of an ID is hexstring of each digit.
func (id ID) String() string {
	var buf bytes.Buffer
	for _, d := range id {
		buf.WriteString(d.String())
	}
	return buf.String()
}

// ParseID parses an ID from String
func ParseID(stringID string) (ID, error) {
	var id ID

	if len(stringID) != DIGITS {
		return id, fmt.Errorf(
			"Cannot parse %v as ID, requires length %v, actual length %v",
			stringID,
			DIGITS,
			len(stringID),
		)
	}

	for i := 0; i < DIGITS; i++ {
		d, err := strconv.ParseInt(stringID[i:i+1], 16, 0)
		if err != nil {
			return id, err
		}
		id[i] = Digit(d)
	}

	return id, nil
}

// String representation of a digit is its hex value
func (digit Digit) String() string {
	return fmt.Sprintf("%X", byte(digit))
}

func (id ID) bytes() []byte {
	b := make([]byte, len(id))
	for idx, d := range id {
		b[idx] = byte(d)
	}
	return b
}

func idFromBytes(b []byte) (i ID) {
	if len(b) < DIGITS {
		return
	}
	for idx, d := range b[:DIGITS] {
		i[idx] = Digit(d)
	}
	return
}

// Parses a slice of IDs into a slice of strings
func idsToStringSlice(ids []ID) []string {
	stringIDs := make([]string, 0)
	for _, id := range ids {
		stringIDs = append(stringIDs, id.String())
	}
	return stringIDs
}

// Parses a slice of strings into a slice of IDs
func stringSliceToIds(stringIDs []string) ([]ID, error) {
	IDs := make([]ID, 0)
	for _, stringID := range stringIDs {
		id, err := ParseID(stringID)
		if err != nil {
			return nil, err
		}

		IDs = append(IDs, id)
	}
	return IDs, nil
}
