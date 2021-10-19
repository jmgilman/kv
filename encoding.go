package kv

import (
	"errors"
	"io"
)

type Encoder interface {
	DecodePair(data io.Reader) (KVPair, error)
	EncodePair(pair KVPair) ([]byte, error)
}

// Cursor provides an interface for iterating over a stream of encoded KVPair's.
type Cursor struct {
	data    io.ReadSeeker
	done    bool
	encoder Encoder
	offset  int
}

// Done returns true when all underlying entries have been read.
func (c *Cursor) Done() bool {
	return c.done
}

// Next returns the next decoded KVPair from the underlying segment data. It
// returns io.EOF when no more data remains.
func (c *Cursor) Next() (KVPair, error) {
	pair, err := c.encoder.DecodePair(c.data)
	if errors.Is(err, io.EOF) {
		c.done = true
		return KVPair{}, err
	}

	return pair, err
}

// ReadToEnd reads all remaining decoded KVPair's from the underlying segment
// data. It returns io.EOF if there were no KVPair's left.
func (c *Cursor) ReadToEnd() ([]KVPair, error) {
	var pairs []KVPair
	for {
		pair, err := c.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				return nil, err
			}
		}

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

// Reset will set the internal io.ReadSeeker back to the beginning and allow
// iterating over the decoded KVPair's again after reaching io.EOF.
func (c *Cursor) Reset() error {
	_, err := c.data.Seek(0, io.SeekStart)
	return err
}

func NewCursor(encoder Encoder, data io.ReadSeeker) Cursor {
	return Cursor{
		data:    data,
		encoder: encoder,
	}
}
