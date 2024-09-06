package store

import "sync"

type Memory[T any] struct {
	values map[string]T
	mu     sync.RWMutex
}

type memoryTx[T any] struct {
	m        *Memory[T]
	readonly bool
}

func (tx *memoryTx[T]) Rollback() error {
	panic("Unimplemented")
}

func (tx *memoryTx[T]) Commit() error {
	if tx.readonly {
		tx.m.mu.RUnlock()
	} else {
		tx.m.mu.Unlock()
	}
	return nil
}

func (tx *memoryTx[T]) Get(key string) (T, bool) {
	val, ok := tx.m.values[key]
	return val, ok
}

func (tx *memoryTx[T]) Put(key string, value T) {
	if tx.readonly {
		panic("Cannot write to a read-only transaction")
	}

	tx.m.values[key] = value
}

// All transactions must be created via BeginTx
func (m *Memory[T]) BeginTx(readonly bool) Tx[T] {
	m.init()
	if readonly {
		m.mu.RLock()
	} else {
		m.mu.Lock()
	}
	return &memoryTx[T]{m: m, readonly: readonly}
}

func (m *Memory[T]) init() {
	if m.values == nil {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.values == nil {
			m.values = make(map[string]T)
		}
	}
}

func (m *Memory[T]) Get(key string) (T, bool) {
	m.init()
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.values[key]
	return val, ok
}

func (m *Memory[T]) Put(key string, payload T) {
	m.init()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.values[key] = payload
}
