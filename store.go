package kv

// MemoryStoreFactory is a function that produces new MemoryStore's.
type MemoryStoreFactory func() MemoryStore

// NVStore represents a non-volatile append-only structure for storing
// MemoryStore's.
type NVStore interface {
	Get(key string) (*KVPair, error)
	New(store MemoryStore) (SegmentID, error)
}

// MemoryStore represents an ordered in-memory storage object for key/value
// pairs.
type MemoryStore interface {
	Delete(key string) error
	Get(key string) (*KVPair, error)
	Min() *KVPair
	Max() *KVPair
	Pairs() []*KVPair
	Put(pair KVPair) error
	Range(key string) (*KVPair, *KVPair, error)
	Size() int
}
