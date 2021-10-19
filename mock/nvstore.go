package mock

import "github.com/jmgilman/kv"

// MockNVStore represents a mock of kv.NVStore
type MockNVStore struct {
	GetFn func(key string) (*kv.KVPair, error)
	PutFn func(store kv.MemoryStore) error
}

func (m *MockNVStore) Get(key string) (*kv.KVPair, error) {
	return m.GetFn(key)
}

func (m *MockNVStore) Put(store kv.MemoryStore) error {
	return m.PutFn(store)
}
