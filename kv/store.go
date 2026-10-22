package kv

import "sync"

// Store is a concurrency-safe local state machine. Raft integration will
// replace direct mutation with committed log entries in a later milestone.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store { return &Store{data: make(map[string]string)} }

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
