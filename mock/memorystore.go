package mock

import (
	"sort"

	"github.com/jmgilman/kv"
)

// MockMemoryStore represents a mock of kv.MemoryStore.
type MockMemoryStore struct {
	store []kv.KVPair
}

func (m *MockMemoryStore) Delete(key string) error {
	for i, pair := range m.store {
		if key == pair.Key {
			m.store = append(m.store[:i], m.store[i+1:]...)
		}
	}

	return nil
}

func (m *MockMemoryStore) Get(key string) (*kv.KVPair, error) {
	for _, pair := range m.store {
		if pair.Key == key {
			return &pair, nil
		}
	}

	return nil, kv.ErrorNoSuchKey
}

func (m *MockMemoryStore) Min() *kv.KVPair {
	if len(m.store) == 0 {
		return nil
	} else {
		return &m.store[0]
	}
}

func (m *MockMemoryStore) Max() *kv.KVPair {
	if len(m.store) == 0 {
		return nil
	} else {
		return &m.store[len(m.store)-1]
	}
}

func (m *MockMemoryStore) Pairs() []*kv.KVPair {
	var pairs []*kv.KVPair
	for i := 0; i < len(m.store); i++ {
		pairs = append(pairs, &m.store[i])
	}

	return pairs
}

func (m *MockMemoryStore) Put(pair kv.KVPair) error {
	m.store = append(m.store, pair)
	sort.Slice(m.store, func(i, j int) bool {
		return m.store[i].Key < m.store[j].Key
	})
	return nil
}

func (m *MockMemoryStore) Range(key string) (*kv.KVPair, *kv.KVPair, error) {
	if len(m.store) == 0 {
		return nil, nil, kv.ErrorOutOfRange
	}

	if key < m.Min().Key {
		return nil, nil, kv.ErrorOutOfRange
	} else if key > m.Max().Key {
		return nil, nil, kv.ErrorOutOfRange
	} else if key == m.Min().Key {
		rpair := m.store[1]
		return nil, &rpair, nil
	} else if key == m.Max().Key {
		lpair := m.store[len(m.store)-2]
		return &lpair, nil, nil
	}

	for i, pair := range m.store {
		if pair.Key > key {
			lpair := m.store[i-1]
			rpair := m.store[i]
			return &lpair, &rpair, nil
		}
	}

	return nil, nil, kv.ErrorOutOfRange
}

func (m *MockMemoryStore) Size() int {
	return len(m.store)
}

func NewMockMemoryStore(pairs []kv.KVPair) MockMemoryStore {
	return MockMemoryStore{pairs}
}
