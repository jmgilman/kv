package kv

import (
	"errors"

	"github.com/google/uuid"
)

type SegmentID = uuid.UUID

// Segment is the base building block for a non-volatile KV store and provides
// a durable version of a MemoryStore.
type Segment interface {
	// ID returns the unique ID of this segment.
	ID() SegmentID
	// Get searches the segment for the given key and returns the KVPair if found.
	// Returns ErrorNoSuchKey if the key was not found.
	Get(key string) (*KVPair, error)
}

// SegmentBackend represents an interface which is capable of persistently
// storing and retrieving Segment's.
type SegmentBackend interface {
	// Get returns the Segment with the given ID.
	Get(id SegmentID) (Segment, error)

	// New creates a new Segment from a MemoryStore and returns its ID.
	New(store MemoryStore) (SegmentID, error)

	// NewWriter creates a new Segment and wraps it in a SegmentWriter for
	// further manipulation.
	NewWriter(id SegmentID) (SegmentWriter, error)
}

// SegmentWriter provides an interface for building segment's through writing
// KVPair's to an internal stream.
type SegmentWriter interface {
	// Closes the internal stream, comitting the new segment.
	Close() error

	// Writes an individual KVPair to the internal stream.
	Write(pair KVPair) (int, error)

	// Writes a slice of KVPair's to the internal stream.
	WriteAll(pairs []KVPair) (int, error)
}

// SegmentLevel is an ordered slice of Segment's where smaller indexed Segment's
// contain newer data and larger indexed Segment's contain older data. It
// provides a common interface for searching across mulitiple Segment's.
type SegmentLevel []Segment

// Get queries the internal list of Segment's for the given key and returns the
// first found result.
func (s *SegmentLevel) Get(key string) (*KVPair, error) {
	for _, segment := range *s {
		pair, err := segment.Get(key)
		if err != nil {
			if !errors.Is(err, ErrorNoSuchKey) {
				return nil, err
			}
		}

		return pair, nil
	}

	return nil, ErrorNoSuchKey
}

func NewSegmentID() SegmentID {
	return uuid.New()
}
