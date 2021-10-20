package sstable

import (
	"fmt"
	"path"

	"github.com/jmgilman/kv"
	"github.com/spf13/afero"
)

type StoreFactory func() kv.MemoryStore

// SegmentBackend implements kv.SegmentBackend by providing persistent storage
// for Segment's using SSTable's stored on the local filesystem.
type SegmentBackend struct {
	encoder      kv.Encoder
	fs           afero.Fs
	indexFactor  int
	storeFactory StoreFactory
	root         string
}

// Get returns the segment with the given SegmentID.
func (s *SegmentBackend) Get(id kv.SegmentID) (Segment, error) {
	// Open segment file
	filePath := path.Join(s.root, s.getFileName(id))
	file, err := s.fs.Open(filePath)
	if err != nil {
		return Segment{}, err
	}

	// Create new segment
	stat, err := file.Stat()
	if err != nil {
		return Segment{}, err
	}
	segment := NewSegment(file, s.encoder, s.storeFactory(), int(stat.Size()))

	// Load index table
	err = segment.LoadIndex()
	if err != nil {
		return Segment{}, err
	}

	return segment, nil
}

// getFileName returns the format in which Segment's are stored by id on the
// local filesystem.
func (s *SegmentBackend) getFileName(id kv.SegmentID) string {
	return fmt.Sprintf("segment-%s.dat", id.String())
}

// New creates a new segment from a MemoryStore and returns its ID.
func (s *SegmentBackend) New(store kv.MemoryStore) (kv.SegmentID, error) {
	// Create file
	id := kv.NewSegmentID()
	writer, err := s.NewWriter(id)
	if err != nil {
		return id, err
	}

	// Write contents of MemoryStore
	for _, pair := range store.Pairs() {
		_, err := writer.Write(*pair)
		if err != nil {
			return id, err
		}
	}

	// Close file
	err = writer.Close()
	return id, err
}

// NewWriter creates a new segment and returns it wrapped in a SegmentWriter.
func (s *SegmentBackend) NewWriter(id kv.SegmentID) (kv.SegmentWriter, error) {
	// Create new file
	filePath := path.Join(s.root, s.getFileName(id))
	file, err := s.fs.Create(filePath)
	if err != nil {
		return &SegmentWriter{}, err
	}

	writer := NewSegmentWriter(id, file, s.encoder, s.storeFactory(), s.indexFactor)
	return &writer, nil
}

func NewSegmentBackend(root string, encoder kv.Encoder, indexFactor int, storeFactory StoreFactory) SegmentBackend {
	return SegmentBackend{
		encoder:      encoder,
		fs:           afero.NewOsFs(),
		indexFactor:  indexFactor,
		storeFactory: storeFactory,
		root:         root,
	}
}
