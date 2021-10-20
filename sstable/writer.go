package sstable

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/jmgilman/kv"
)

// SegmentWriter implements kv.SegmentWriter for writing SSTable formatted
// segments to an underlying stream.
type SegmentWriter struct {
	byteIndex    int
	encoder      kv.Encoder
	id           kv.SegmentID
	index        int
	indexFactor  int
	lastKey      string
	lastKeyIndex int
	table        kv.MemoryStore
	writer       io.WriteCloser
}

// Close writes the last written KVPair to the index table and proceeds to
// encode the index table, writing it along with it's length to the end of the
// underlying stream before calling Close() on the underlying strema.
func (s *SegmentWriter) Close() error {
	// Always record the last key to the index table
	if s.index%s.indexFactor != 0 {
		s.table.Put(kv.NewKVPair(s.lastKey, s.encodeUint32(uint32(s.lastKeyIndex))))
	}

	// Write the encoded index table to the end of the stream
	encoded, err := s.encodeTable()
	if err != nil {
		return err
	}

	_, err = s.writer.Write(encoded)
	if err != nil {
		return err
	}

	// Last four bytes of a segment stream will always be size of index table
	indexSize := len(encoded)
	if err := binary.Write(s.writer, binary.BigEndian, uint32(indexSize)); err != nil {
		return err
	}

	return s.writer.Close()
}

// encodeUint32 takes an integer and encodes it as a binary Uint32.
func (s *SegmentWriter) encodeUint32(n uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, n)
	return buf
}

// encodeTable uses the internal encoder to encode all entries in the underlying
// index table and returns the result as a byte slice.
func (s *SegmentWriter) encodeTable() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	for _, pair := range s.table.Pairs() {
		encoded, err := s.encoder.EncodePair(*pair)
		if err != nil {
			return nil, err
		}

		buf.Write(encoded)
	}

	return buf.Bytes(), nil
}

// Write takes a KVPair and writes it to the underlying stream. An internal
// count is maintained for the number of writes made and is frequently checked
// in order to determine if a specific entry should be added to the index table
// based on the configured index factor. The first and last writes are always
// added to the index table.
func (s *SegmentWriter) Write(pair kv.KVPair) (int, error) {
	s.lastKey = pair.Key
	s.lastKeyIndex = s.byteIndex

	encoded, err := s.encoder.EncodePair(pair)
	if err != nil {
		return 0, err
	}

	n, err := s.writer.Write(encoded)
	if err != nil {
		return 0, nil
	}

	if (s.index+1)%s.indexFactor == 0 || (s.index+1) == 1 {
		s.table.Put(kv.NewKVPair(pair.Key, s.encodeUint32(uint32(s.byteIndex))))
	}
	s.byteIndex += n
	s.index += 1

	return n, nil
}

// WriteAll takes a slice of KVPair's and calls Write() on each of them.
func (s *SegmentWriter) WriteAll(pairs []kv.KVPair) (int, error) {
	var total int
	for _, pair := range pairs {
		n, err := s.Write(pair)
		if err != nil {
			return 0, nil
		}

		total += n
	}

	return total, nil
}

func NewSegmentWriter(id kv.SegmentID, writer io.WriteCloser, encoder kv.Encoder, table kv.MemoryStore, indexFactor int) SegmentWriter {
	return SegmentWriter{
		encoder:     encoder,
		id:          id,
		indexFactor: indexFactor,
		table:       table,
		writer:      writer,
	}
}
