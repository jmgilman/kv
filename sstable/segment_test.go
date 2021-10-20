package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/dsnet/golib/memfile"
	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock"
	"github.com/jmgilman/kv/mock/helper"
	"github.com/matryer/is"
	"github.com/spf13/afero"
)

func NewMockSegment(pairs []kv.KVPair) (Segment, mock.MockEncoder) {
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

	return NewSegment(file, &encoder, &store, size), encoder
}

func NewMockSegmentFile(file afero.File, pairs []kv.KVPair, indexFactor int) (data mock.MockEncoder, index mock.MockEncoder, err error) {
	encoder := mock.MockEncoder{}
	indexEncoder := mock.MockEncoder{}
	indexBuf := bytes.NewBuffer([]byte{})

	var tableSize int
	for i, pair := range pairs {
		data, err := encoder.EncodePair(pair)
		if err != nil {
			return mock.MockEncoder{}, mock.MockEncoder{}, err
		}

		_, err = file.Write(data)
		if err != nil {
			return mock.MockEncoder{}, mock.MockEncoder{}, err
		}

		if i == 1 || i%indexFactor == 0 || i == len(pairs)-1 {
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, uint32(i*4))

			indexPair := kv.NewKVPair(pair.Key, buf)
			indexData, err := indexEncoder.EncodePair(indexPair)
			if err != nil {
				return mock.MockEncoder{}, mock.MockEncoder{}, nil
			}

			_, err = indexBuf.Write(indexData)
			if err != nil {
				return mock.MockEncoder{}, mock.MockEncoder{}, err
			}

			tableSize += 1
		}
	}

	_, err = file.Write(indexBuf.Bytes())
	if err != nil {
		return mock.MockEncoder{}, mock.MockEncoder{}, err
	}

	err = binary.Write(file, binary.BigEndian, uint32(tableSize*4))
	if err != nil {
		return mock.MockEncoder{}, mock.MockEncoder{}, err
	}

	return encoder, indexEncoder, nil
}

func TestSegmentFind(t *testing.T) {
	size := 10
	is := is.New(t)

	// Find every key in the test data
	segment, encoder := NewMockSegment(helper.NewRandomSortedPairs(size))
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
	is := is.New(t)

	segment, encoder := NewMockSegment(helper.NewRandomSortedPairs(size))
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

func TestSegmentLoadIndex(t *testing.T) {
	size := 10
	is := is.New(t)

	// Each entry is the underlying buffer is 4 bytes (uint32) long
	entrySize := 4

	// Setup a test file
	pairs := helper.NewRandomSortedPairs(size)
	encoder := mock.MockEncoder{}
	file := memfile.File{}

	for _, pair := range pairs {
		data, err := encoder.EncodePair(pair)
		is.NoErr(err)

		_, err = file.Write(data)
		is.NoErr(err)
	}

	tableSize := entrySize * size
	binary.Write(&file, binary.BigEndian, uint32(tableSize))

	// Create a new segment
	segment := NewSegment(&file, &encoder, &mock.MockMemoryStore{}, len(file.Bytes()))

	// Load index table
	err := segment.LoadIndex()
	is.NoErr(err)

	// Index table was loaded correctly
	indexPairs := segment.index.Pairs()
	for i := 0; i < len(pairs); i++ {
		is.Equal(pairs[i].Key, indexPairs[i].Key)
	}
}
