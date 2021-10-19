package sstable

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/jmgilman/kv"
)

// Segment implements kv.Segment using an SSTable. It uses a contingous body
// of read-only, ordered, and encoded KVPair's in order to store the contents
// of a MemoryStore into a more durable long-term format.  Internally, it uses
// a MemoryStore in order to build a sparse index of its stored KVPair's to
// reduce the amount of IO required to find a key.
type Segment struct {
	data        io.ReadSeeker
	id          kv.SegmentID
	encoder     kv.Encoder
	index       kv.MemoryStore
	indexFactor int
	size        int
}

// Get searches the underlying SSTable for the given key by first checking
// the internal index table to locate the approximate position and then reading
// the contents of the SSTable at that position to find the key.
func (s *Segment) Get(key string) (*kv.KVPair, error) {
	// Get range to search for the given key
	start, end, err := s.searchIndex(key)
	if err != nil {
		return nil, err
	}

	// Create a limited reader set to the range
	s.data.Seek(int64(start), io.SeekStart)
	reader := LimitReadSeeker(s.data, int64(end-start))

	// Create a cursor to iterate over pairs in this range
	cursor := kv.NewCursor(s.encoder, reader)

	// Search the range for the given key
	for {
		pair, err := cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				return nil, err
			}
		}

		if key == pair.Key {
			return &pair, nil
		}
	}

	return nil, kv.ErrorNoSuchKey
}

// searchIndex searches the index table to find the range, in bytes, where the
// key is expected to be found. Returns ErrorNoSuchKey if the key is outside
// the range of the index table.
func (s *Segment) searchIndex(key string) (start int, end int, err error) {
	min := s.index.Min()
	max := s.index.Max()

	// Return out of range is the key is outside the bounds of the table
	if key < min.Key || key > max.Key {
		return 0, 0, kv.ErrorNoSuchKey
	}

	left, right, err := s.index.Range(key)
	if err != nil {
		return 0, 0, err
	}

	// Assume start is the beginning and end is the end of the data
	start = 0
	end = s.size

	// If left is not nil, start at its index position
	if left != nil {
		start = int(binary.BigEndian.Uint32(left.Value))
	}

	// If right is not nil, stop at its index position
	if right != nil {
		end = int(binary.BigEndian.Uint32(right.Value))
	}

	return start, end, nil
}

func NewSegment(data io.ReadSeeker, encoder kv.Encoder, index kv.MemoryStore, indexFactor int, size int) Segment {
	return Segment{
		data:        data,
		encoder:     encoder,
		index:       index,
		indexFactor: indexFactor,
		size:        size,
	}
}

// LimitedReadSeeker provides the same interface for io.LimitedReader for types
// implementing io.ReadSeeker.
type LimitedReadSeeker struct {
	R io.ReadSeeker
	N int64
}

func (l *LimitedReadSeeker) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}

	n, err = l.R.Read(p)
	l.N -= int64(n)

	return n, nil
}

func (l *LimitedReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return l.R.Seek(offset, whence)
}

func LimitReadSeeker(r io.ReadSeeker, n int64) io.ReadSeeker {
	return &LimitedReadSeeker{
		R: r,
		N: n,
	}
}
