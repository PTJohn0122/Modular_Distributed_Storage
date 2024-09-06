package partitioning

import "errors"

type Single struct {
	id *uint64
}

func NewSingle() *Single {
	return &Single{}
}

func (s *Single) Lookup(key string) (uint64, string, error) {
	if s.id == nil {
		return 0, "", errors.New("there are currently no registered replica groups")
	}
	return *s.id, key, nil
}

func (s *Single) AddReplicaGroup(id uint64) []Reassignment {
	// Only use the first replica group
	if s.id == nil {
		s.id = &id
	}
	return nil
}

func (s *Single) RemoveReplicaGroup(id uint64) []Reassignment {
	// Do not allow removals
	return nil
}
