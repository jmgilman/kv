package kv

import (
	"errors"

	"github.com/google/uuid"
)

type SegmentID = uuid.UUID

// Segment is the base building block for a non-volatile KV store and provides
// a durable version of a MemoryStore.
type Segment interface {
	// Get searches the segment for the given key and returns the KVPair if found.
	// Returns ErrorNoSuchKey if the key was not found.
	Get(key string) (*KVPair, error)
}

// SegmentBackend represents a system which is capable of persistently storing
// and retrieving Segment's.
type SegmentBackend interface {
	Load(id SegmentID, encoder Encoder) (Segment, error)
	New(id SegmentID, encoder Encoder) (SegmentWriter, error)
}

// SegmentWriter provides an interface for building segment's through writing
// KVPair's to an internal stream.
type SegmentWriter interface {
	// Closes the internal stream, comitting the new segment.
	Close() error

	// Writes an individual KVPair to the internal stream.
	Write(pair KVPair) error

	// Writes a slice of KVPair's to the internal stream.
	WriteAll(pairs []KVPair) error
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
