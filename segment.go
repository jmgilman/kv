package kv

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/google/uuid"
)

var ErrorInvalidSegmentLevel = errors.New("invalid segment level")
var ErrorSegmentNotFound = errors.New("segment not found")

type SegmentID = uuid.UUID

// Segment is the base building block for a non-volatile KV store and provides
// a durable version of a MemoryStore.
type Segment interface {
	// ID returns the unique ID of this segment.
	ID() SegmentID

	// Min returns the lowest key stored in this segment.
	Min() *KVPair

	// Max returns the highest key stored in this segment.
	Max() *KVPair

	// Get searches the segment for the given key and returns the KVPair if found.
	// Returns ErrorNoSuchKey if the key was not found.
	Get(key string) (*KVPair, error)
}

// SegmentBackend represents an interface which is capable of persistently
// storing and retrieving Segment's.
type SegmentBackend interface {
	// Delete removes the segment with the given ID.
	Delete(id SegmentID) error

	// Get returns the Segment with the given ID.
	Get(id SegmentID) (Segment, error)

	// New creates a new Segment from a MemoryStore and returns its ID.
	New(id SegmentID, store MemoryStore) error

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

// // SegmentLevel is an ordered slice of Segment's where smaller indexed Segment's
// // contain newer data and larger indexed Segment's contain older data. It
// // provides a common interface for searching across mulitiple Segment's.
// type SegmentLevel []Segment

// // Get queries the internal list of Segment's for the given key and returns the
// // first found result.
// func (s *SegmentLevel) Get(key string) (*KVPair, error) {
// 	for _, segment := range *s {
// 		pair, err := segment.Get(key)
// 		if err != nil {
// 			if !errors.Is(err, ErrorNoSuchKey) {
// 				return nil, err
// 			}
// 		}

// 		return pair, nil
// 	}

// 	return nil, ErrorNoSuchKey
// }

type SegmentLevel struct {
	segments []Segment
}

func (s *SegmentLevel) DeleteSegment(id SegmentID) error {
	for i, segment := range s.segments {
		if segment.ID() == id {
			s.segments = append(s.segments[:i], s.segments[i+1:]...)
			return nil
		}
	}

	return ErrorSegmentNotFound
}

func (s *SegmentLevel) Get(key string) (*KVPair, error) {
	for _, segment := range s.segments {
		if key >= segment.Min().Key {
			if key <= segment.Max().Key {
				return segment.Get(key)
			}
		}
	}

	return nil, ErrorNoSuchKey
}

func (s *SegmentLevel) GetSegment(id SegmentID) (*Segment, error) {
	for _, segment := range s.segments {
		if segment.ID() == id {
			return &segment, nil
		}
	}

	return nil, ErrorSegmentNotFound
}

func (s *SegmentLevel) Put(segment Segment) {
	s.segments = append(s.segments, segment)
	sort.Slice(s.segments, func(i, j int) bool {
		return s.segments[i].Min().Key < s.segments[j].Min().Key
	})
}

func NewSegmentLevel(segments []Segment) SegmentLevel {
	return SegmentLevel{
		segments: segments,
	}
}

func NewSegmentID() SegmentID {
	return uuid.New()
}

type SegmentStore struct {
	backend SegmentBackend
	buffer  []Segment
	levels  []SegmentLevel
	log     Log
}

func (s *SegmentStore) Delete(id SegmentID) error {
	// Log delete
	meta := []KVPair{NewKVPair("ID", []byte(id.String()))}
	if err := s.newLogEntry(LogDelete, meta); err != nil {
		return err
	}

	// Search for segment in buffer
	for i, segment := range s.buffer {
		if segment.ID() == id {
			// Delete segment from backend
			if err := s.backend.Delete(id); err != nil {
				return err
			}

			// Delete segment from buffer
			s.buffer = append(s.buffer[:i], s.buffer[i+1:]...)
			return nil
		}
	}

	// Search for segment in levels
	for _, level := range s.levels {
		if _, err := level.GetSegment(id); err == nil {
			// Delete segment from backend
			if err := s.backend.Delete(id); err != nil {
				return err
			}

			// Delete segment from level
			if err := level.DeleteSegment(id); err != nil {
				return err
			}

			return nil
		}
	}

	return ErrorSegmentNotFound
}

func (s *SegmentStore) New(store MemoryStore) (SegmentID, error) {
	// Log new segment
	id := NewSegmentID()
	meta := []KVPair{NewKVPair("ID", []byte(id.String()))}
	if err := s.newLogEntry(LogNew, meta); err != nil {
		return id, err
	}

	// Create new segment
	if err := s.backend.New(id, store); err != nil {
		return id, err
	}

	// Load newly created segment
	segment, err := s.backend.Get(id)
	if err != nil {
		return id, err
	}

	s.buffer = append(s.buffer, segment)
	return id, nil
}

func (s *SegmentStore) Put(level int, segment Segment) error {
	// Log put
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(level))
	meta := []KVPair{NewKVPair("ID", []byte(segment.ID().String())), NewKVPair("Level", buf)}
	if err := s.newLogEntry(LogPut, meta); err != nil {
		return err
	}

	// Check if level is valid
	if level > len(s.levels) {
		return ErrorInvalidSegmentLevel
	}

	// Check if a new level needs to be added
	if level > len(s.levels)-1 {
		s.levels = append(s.levels, NewSegmentLevel([]Segment{}))
	}

	// Add segment to level
	s.levels[level].Put(segment)
	return nil
}

func (s *SegmentStore) newLogEntry(action LogAction, meta []KVPair) error {
	entry := NewLogEntry(action, meta)
	index, err := s.log.Last()
	if err != nil {
		return err
	}

	return s.log.Write(index+1, entry)
}
