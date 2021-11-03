package mock

import (
	"fmt"
	"sort"

	"github.com/jmgilman/kv"
)

type MockLog struct {
	closed  bool
	entries map[uint64]kv.LogEntry
	indexes []uint64
}

func (m *MockLog) Close() error {
	if m.closed {
		return fmt.Errorf("Log already closed")
	} else {
		m.closed = true
	}

	return nil
}

func (m *MockLog) First() (uint64, error) {
	if len(m.indexes) == 0 {
		return 0, nil
	}
	return m.indexes[0], nil
}

func (m *MockLog) Last() (uint64, error) {
	if len(m.indexes) == 0 {
		return 0, nil
	}
	return m.indexes[len(m.indexes)-1], nil
}

func (m *MockLog) Read(index uint64) (kv.LogEntry, error) {
	entry, ok := m.entries[index]
	if !ok {
		return kv.LogEntry{}, fmt.Errorf("Index not found")
	}

	return entry, nil
}

func (m *MockLog) Write(index uint64, entry kv.LogEntry) error {
	m.entries[index] = entry
	m.indexes = append(m.indexes, index)
	sort.Slice(m.indexes, func(i, j int) bool {
		return m.indexes[i] < m.indexes[j]
	})

	return nil
}
