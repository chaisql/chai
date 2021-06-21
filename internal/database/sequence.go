package database

import "sync"

type Sequence struct {
	Info *SequenceInfo

	mu           sync.Mutex
	CurrentValue int64
}

func (s *Sequence) Next() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return 0, nil
}
