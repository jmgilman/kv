package mock

import "github.com/jmgilman/kv"

// MockNVStore represents a mock of kv.NVStore
type MockNVStore struct {
	GetFn func(key string) (*kv.KVPair, error)
	PutFn func(store kv.MemoryStore) (kv.SegmentID, error)
}

func (m *MockNVStore) Get(key string) (*kv.KVPair, error) {
	return m.GetFn(key)
}

func (m *MockNVStore) New(store kv.MemoryStore) (kv.SegmentID, error) {
	return m.PutFn(store)
}
