package sstable

import (
	"fmt"
	"testing"

	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock"
	"github.com/jmgilman/kv/mock/helper"
	"github.com/matryer/is"
	"github.com/spf13/afero"
)

func NewMockSegmentBackend(indexFactor int) SegmentBackend {
	factory := func() kv.MemoryStore {
		return &mock.MockMemoryStore{}
	}
	return SegmentBackend{
		encoder:      &mock.MockEncoder{},
		indexFactor:  indexFactor,
		fs:           afero.NewMemMapFs(),
		root:         "test",
		storeFactory: factory,
	}
}

func TestSegmentBackendGet(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)
	id := kv.NewSegmentID()
	filePath := fmt.Sprintf("test/segment-%s.dat", id.String())
	backend := NewMockSegmentBackend(factor)

	// Create test file
	file, err := backend.fs.Create(filePath)
	is.NoErr(err)

	// Write test data to file
	pairs := helper.NewRandomSortedPairs(size)
	_, encoder, err := NewMockSegmentFile(file, pairs, factor)
	is.NoErr(err)
	file.Close()

	// Get segment
	backend.encoder = &encoder
	_, err = backend.Get(id)
	is.NoErr(err)
}

func TestSegmentBackendgetFileName(t *testing.T) {
	is := is.New(t)
	id := kv.NewSegmentID()
	filePath := fmt.Sprintf("segment-%s.dat", id.String())
	backend := NewMockSegmentBackend(0)

	is.Equal(backend.getFileName(id), filePath)
}

func TestSegmentBackendNew(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)
	backend := NewMockSegmentBackend(factor)

	// Create random memory store
	store := helper.NewRandomMemoryStore(size)

	// Write store
	id, err := backend.New(&store)
	is.NoErr(err)

	// Verify correct file exists
	filePath := fmt.Sprintf("test/segment-%s.dat", id.String())
	s, err := backend.fs.Stat(filePath)
	is.NoErr(err)

	// Verify file size
	entrySize := 4
	dataSize := size * entrySize
	tableSize := ((size / factor) + 2) * entrySize
	is.Equal(s.Size(), int64(dataSize+tableSize+4))
}

func TestSegmentBackendNewWriter(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)
	id := kv.NewSegmentID()
	filePath := fmt.Sprintf("test/segment-%s.dat", id.String())
	backend := NewMockSegmentBackend(factor)

	// Create file and write test data
	pairs := helper.NewRandomSortedPairs(size)
	result, err := backend.NewWriter(id)
	is.NoErr(err)

	_, err = result.WriteAll(pairs)
	is.NoErr(err)
	err = result.Close()
	is.NoErr(err)

	// Verify correct file exists
	s, err := backend.fs.Stat(filePath)
	is.NoErr(err)

	// Verify file size
	entrySize := 4
	dataSize := size * entrySize
	tableSize := ((size / factor) + 2) * entrySize
	is.Equal(s.Size(), int64(dataSize+tableSize+4))
}
