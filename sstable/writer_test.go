package sstable

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock"
	"github.com/jmgilman/kv/mock/helper"
	"github.com/matryer/is"
	"github.com/spf13/afero"
)

func NewMockSegmentWriter(indexFactor int) (SegmentWriter, afero.File, error) {
	id := kv.NewSegmentID()
	fs := afero.NewMemMapFs()
	file, err := fs.Create("test.dat")
	if err != nil {
		return SegmentWriter{}, nil, err
	}

	encoder := mock.MockEncoder{}
	table := mock.NewMockMemoryStore([]kv.KVPair{})
	return NewSegmentWriter(id, file, &encoder, &table, indexFactor), file, nil
}

func TestSegmentWriterClose(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)

	writer, file, err := NewMockSegmentWriter(factor)
	pairs := helper.NewRandomSortedPairs(size)
	is.NoErr(err)

	// Each entry is the underlying buffer is 4 bytes (uint32) long
	entrySize := 4

	// Write all pairs
	_, err = writer.WriteAll(pairs)
	is.NoErr(err)

	// Close the writer
	err = writer.Close()
	is.NoErr(err)

	// Last write was indexed
	is.Equal(writer.table.Pairs()[writer.table.Size()-1].Key, pairs[len(pairs)-1].Key)

	// File size is correct
	dataSize := entrySize * size
	indexSize := ((size / factor) + 2) * entrySize // Add two for first/last indexes
	fileSize := dataSize + indexSize + 4           // Last four bytes are index length

	s, err := file.Stat()
	is.NoErr(err)
	is.Equal(s.Size(), int64(fileSize))

	// Close was called
	buf := make([]byte, 4)
	_, err = file.Read(buf)
	is.Equal(err.Error(), "File is closed")
}

func TestSegmentWriterEncodeUint32(t *testing.T) {
	var num uint32 = 10
	var writer SegmentWriter
	is := is.New(t)

	result := writer.encodeUint32(num)
	numResult := binary.BigEndian.Uint32(result)
	is.Equal(numResult, uint32(num))
}

func TestSegmentWriterEncodeTable(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)

	writer, _, err := NewMockSegmentWriter(factor)
	pairs := helper.NewRandomSortedPairs(size)
	encoder := writer.encoder.(*mock.MockEncoder)
	is.NoErr(err)

	// Each entry is the underlying buffer is 4 bytes (uint32) long
	entrySize := 4

	// Create a random index table
	for _, pair := range pairs {
		err := writer.table.Put(pair.Key, pair.Value)
		is.NoErr(err)
	}

	result, err := writer.encodeTable()
	is.NoErr(err)

	// Encoder was called with all pairs in the table
	is.True(reflect.DeepEqual(encoder.Pairs(), pairs))

	// Output is the expected size
	is.Equal(len(result), size*entrySize)
}

func TestSegmentWriterWrite(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)

	writer, _, err := NewMockSegmentWriter(factor)
	pairs := helper.NewRandomSortedPairs(size)
	is.NoErr(err)

	// Each entry is the underlying buffer is 4 bytes (uint32) long
	entrySize := 4

	// Write all pairs
	for _, pair := range pairs {
		n, err := writer.Write(pair)
		is.NoErr(err)
		is.Equal(n, entrySize)
	}

	// First write was indexed
	_, err = writer.table.Get(pairs[0].Key)
	is.NoErr(err)

	// Next write determined by index factor
	_, err = writer.table.Get(pairs[factor-1].Key)
	is.NoErr(err)

	// Table size is correct
	is.Equal(writer.table.Size(), (size/factor)+1)
}

func TestSegmentWriterWriteAll(t *testing.T) {
	size := 10
	factor := 3
	is := is.New(t)

	writer, _, err := NewMockSegmentWriter(factor)
	pairs := helper.NewRandomSortedPairs(size)
	is.NoErr(err)

	// Each entry is the underlying buffer is 4 bytes (uint32) long
	entrySize := 4

	// Write all pairs
	n, err := writer.WriteAll(pairs)
	is.NoErr(err)

	// All pairs were written
	is.Equal(n, entrySize*(size))

	// Table size is correct
	is.Equal(writer.table.Size(), (size/factor)+1)
}
