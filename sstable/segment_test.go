package sstable

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock"
	"github.com/jmgilman/kv/mock/helper"
	"github.com/matryer/is"
)

func NewMockSegment(pairs []kv.KVPair, indexFactor int) (Segment, mock.MockEncoder) {
	encoder := mock.MockEncoder{}
	file, size := encoder.Set(pairs)
	store := mock.NewMockMemoryStore([]kv.KVPair{})

	for i, pair := range pairs {
		if i == 1 || i%3 == 0 || i == len(pairs)-1 {
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(i*4))
			store.Put(pair.Key, buf)
		}
	}

	return NewSegment(file, &encoder, &store, indexFactor, size), encoder
}

func TestSegmentFind(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)

	// Find every key in the test data
	segment, encoder := NewMockSegment(helper.NewRandomSortedPairs(size), factor)
	pairs := encoder.Pairs()
	for _, pair := range pairs {
		result, err := segment.Get(pair.Key)
		is.NoErr(err)
		is.Equal(result.Key, pair.Key)
		is.Equal(result.Value, pair.Value)
	}

	// Test non-existent key
	_, err := segment.Get("123")
	is.True(errors.Is(err, kv.ErrorNoSuchKey))
}

func TestSegmentIndex(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)

	segment, encoder := NewMockSegment(helper.NewRandomSortedPairs(size), factor)
	pairs := encoder.Pairs()

	// Each entry is the underlying buffer is 4 bytes (uint32) long
	entrySize := 4
	dataSize := len(pairs) * entrySize

	// First entry: beginning of file until 2nd key
	start, end, err := segment.searchIndex(pairs[0].Key)
	is.NoErr(err)
	is.Equal(start, 0)
	is.Equal(end, 1*entrySize)

	// Last entry: second to last index until end of file
	start, end, err = segment.searchIndex(pairs[len(pairs)-1].Key)
	is.NoErr(err)
	is.Equal(start, dataSize-4*entrySize)
	is.Equal(end, dataSize)

	// Key is out of range low
	key := (pairs[0].Key[0] - 1)
	_, _, err = segment.searchIndex(string(key))
	is.True(errors.Is(err, kv.ErrorNoSuchKey))

	// Key is out of range high
	key = (pairs[len(pairs)-1].Key[0] + 1)
	_, _, err = segment.searchIndex(string(key))
	is.True(errors.Is(err, kv.ErrorNoSuchKey))

}
