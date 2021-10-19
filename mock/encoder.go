package mock

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/dsnet/golib/memfile"
	"github.com/jmgilman/kv"
)

// MockEncoder implements Encoder by appending encoded pairs to a private slice
// and returning the index of the pair in byte form. Subsequent calls to decode
// with the index will return the original pair.
type MockEncoder struct {
	pairs []kv.KVPair
}

// DecodePair reads four bytes into a uint32 and attempts to use the resulting
// value as the index into an internal slice containing all pairs passed to
// EncodePair(). Returns io.EOF when the underlying io.Reader returns io.EOF and
// io.ErrUnexpectedEOF if less than four bytes are read from the io.Reader.
func (t *MockEncoder) DecodePair(data io.Reader) (kv.KVPair, error) {
	buf := make([]byte, 4)
	n, err := data.Read(buf)

	if errors.Is(err, io.EOF) {
		return kv.KVPair{}, err
	} else if n < 4 {
		return kv.KVPair{}, io.ErrUnexpectedEOF
	} else if err != nil {
		return kv.KVPair{}, err
	}

	index := int(binary.BigEndian.Uint32(buf))
	return t.pairs[index], nil
}

// EncodePair appends the passed pair to an internal slice and returns its index
// encoded in bytes as an uint32.
func (t *MockEncoder) EncodePair(pair kv.KVPair) ([]byte, error) {
	t.pairs = append(t.pairs, pair)

	buf := make([]byte, 4)
	index := len(t.pairs) - 1
	binary.BigEndian.PutUint32(buf, uint32(index))
	return buf, nil
}

// Pairs returns the cumalative list of pairs that were passed into
// EncodePair().
func (m *MockEncoder) Pairs() []kv.KVPair {
	return m.pairs
}

// Set sets the interal slice for tracking pairs passed to EncodePair() and
// returns an io.ReadSeeker along with it's size which can be passed to
// DecodePair() in order to get the passed pairs back.
func (m *MockEncoder) Set(pairs []kv.KVPair) (io.ReadSeeker, int) {
	var file memfile.File
	for _, pair := range pairs {
		data, _ := m.EncodePair(pair)
		file.Write(data)
	}

	file.Seek(0, io.SeekStart)
	m.pairs = pairs
	return &file, len(file.Bytes())
}
