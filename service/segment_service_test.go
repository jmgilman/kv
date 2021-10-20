package service

import (
	"testing"

	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock"
	"github.com/jmgilman/kv/mock/helper"
	"github.com/matryer/is"
)

func NewMockSegmentService() SegmentService {
	factory := func() kv.MemoryStore {
		return &mock.MockMemoryStore{}
	}
	backend := mock.NewMockSegmentBackend()

	return NewSegmentService(&backend, factory)
}

func TestSegmentServiceNew(t *testing.T) {
	size := 10
	is := is.New(t)

	// Create test data
	service := NewMockSegmentService()
	store := helper.NewRandomMemoryStore(size)

	// Add new MemoryStore
	_, err := service.New(&store)
	is.NoErr(err)

	// Segment was added
	is.Equal(len(service.buffer), 1)

	// Segment contains correct data
	segment := service.buffer[0]
	for _, pair := range store.Pairs() {
		_, err := segment.Get(pair.Key)
		is.NoErr(err)
	}
}
