package mock

import (
	"fmt"

	"github.com/jmgilman/kv"
)

type MockSegment struct {
	id    kv.SegmentID
	store MockMemoryStore
}

func (m *MockSegment) Get(key string) (*kv.KVPair, error) {
	return m.store.Get(key)
}

func (m *MockSegment) ID() kv.SegmentID {
	return m.id
}

func NewMockSegment(pairs []kv.KVPair) MockSegment {
	id := kv.NewSegmentID()
	store := NewMockMemoryStore(pairs)
	return MockSegment{
		id:    id,
		store: store,
	}
}

type MockSegmentBackend struct {
	segments map[kv.SegmentID]MockSegment
}

func (m *MockSegmentBackend) Get(id kv.SegmentID) (kv.Segment, error) {
	segment, ok := m.segments[id]
	if !ok {
		return &MockSegment{}, fmt.Errorf("Segment not found")
	}

	return &segment, nil
}

func (m *MockSegmentBackend) New(store kv.MemoryStore) (kv.SegmentID, error) {
	var pairs []kv.KVPair
	for _, pair := range store.Pairs() {
		pairs = append(pairs, *pair)
	}

	id := kv.NewSegmentID()
	segment := MockSegment{
		id:    id,
		store: NewMockMemoryStore(pairs),
	}
	m.segments[id] = segment

	return id, nil
}

func (m *MockSegmentBackend) NewWriter(id kv.SegmentID) (kv.SegmentWriter, error) {
	writer := NewMockSegmentWriter(id, m)
	return &writer, nil
}

func NewMockSegmentBackend() MockSegmentBackend {
	return MockSegmentBackend{
		segments: map[kv.SegmentID]MockSegment{},
	}
}

type MockSegmentWriter struct {
	backend *MockSegmentBackend
	id      kv.SegmentID
	pairs   []kv.KVPair
}

func (m *MockSegmentWriter) Close() error {
	m.backend.segments[m.id] = NewMockSegment(m.pairs)
	return nil
}

func (m *MockSegmentWriter) Write(pair kv.KVPair) (int, error) {
	m.pairs = append(m.pairs, pair)
	return len(pair.Key) + len(pair.Value), nil
}

func (m *MockSegmentWriter) WriteAll(pairs []kv.KVPair) (int, error) {
	var size int
	for _, pair := range pairs {
		m.pairs = append(m.pairs, pair)
		size += len(pair.Key) + len(pair.Value)
	}

	return size, nil
}

func NewMockSegmentWriter(id kv.SegmentID, backend *MockSegmentBackend) MockSegmentWriter {
	return MockSegmentWriter{
		backend: backend,
		id:      id,
	}
}
