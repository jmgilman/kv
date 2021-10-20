package encoders

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/jmgilman/kv"
)

const headerSize = 8
const maxKeySize = math.MaxUint32
const maxValueSize = math.MaxUint32

type byteEncodeHeader struct {
	KeySize   int
	ValueSize int
}

type ByteEncoder struct{}

func NewKVPair(key string, value []byte, tombstone bool) kv.KVPair {
	pair := kv.NewKVPair(key, value)
	pair.Tombstone = tombstone
	return pair
}

func (b ByteEncoder) decodeHeader(data io.Reader) (byteEncodeHeader, error) {
	readBuf := make([]byte, 4)

	// Read key length
	n, err := data.Read(readBuf)
	if errors.Is(err, io.EOF) {
		return byteEncodeHeader{}, err
	} else if n < 4 {
		return byteEncodeHeader{}, io.ErrUnexpectedEOF
	} else if err != nil {
		return byteEncodeHeader{}, err
	}
	keySize := int(binary.BigEndian.Uint32(readBuf))

	// Read value length
	n, err = data.Read(readBuf)
	if errors.Is(err, io.EOF) {
		return byteEncodeHeader{}, io.ErrUnexpectedEOF
	} else if n < 4 {
		return byteEncodeHeader{}, io.ErrUnexpectedEOF
	} else if err != nil {
		return byteEncodeHeader{}, err
	}
	valueSize := int(binary.BigEndian.Uint32(readBuf))

	return byteEncodeHeader{
		KeySize:   keySize,
		ValueSize: valueSize,
	}, nil
}

func (b ByteEncoder) encodeHeader(header byteEncodeHeader) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 8))

	// Write key size
	if err := binary.Write(buf, binary.BigEndian, uint32(header.KeySize)); err != nil {
		return nil, err
	}

	// Write value size
	if err := binary.Write(buf, binary.BigEndian, uint32(header.ValueSize)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b ByteEncoder) DecodePair(data io.Reader) (kv.KVPair, error) {
	var key string
	var value []byte

	// Read header
	header, err := b.decodeHeader(data)
	if err != nil {
		return kv.KVPair{}, err
	}

	// Read key
	if header.KeySize > 0 {
		readBuf := make([]byte, header.KeySize)

		n, err := data.Read(readBuf)
		if n < header.KeySize {
			return kv.KVPair{}, io.ErrUnexpectedEOF
		} else if err != nil {
			return kv.KVPair{}, err
		}

		key = string(readBuf)
	}

	// Read value
	if header.ValueSize > 0 {
		readBuf := make([]byte, header.ValueSize)

		n, err := data.Read(readBuf)
		if n < header.ValueSize {
			return kv.KVPair{}, io.ErrUnexpectedEOF
		} else if err != nil {
			return kv.KVPair{}, err
		}

		value = readBuf
	}

	// Read tombstone
	var tombstone bool
	if err := binary.Read(data, binary.BigEndian, &tombstone); err != nil {
		return kv.KVPair{}, err
	}

	return NewKVPair(key, value, tombstone), nil
}

func (b ByteEncoder) EncodePair(pair kv.KVPair) ([]byte, error) {
	keyBytes := []byte(pair.Key)
	keySize := len(keyBytes)
	valueSize := len(pair.Value)

	// Don't exceed the header capacity
	if keySize > maxKeySize {
		return nil, kv.ErrorKeyTooLarge
	} else if valueSize > maxValueSize {
		return nil, kv.ErrorValueTooLarge
	}

	header := newByteEncodeHeader(keySize, valueSize)
	headerBytes, err := b.encodeHeader(header)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(make([]byte, 0, headerSize+keySize+valueSize))

	// Write header
	if _, err := buf.Write(headerBytes); err != nil {
		return nil, err
	}

	// Write key
	if _, err := buf.Write(keyBytes); err != nil {
		return nil, err
	}

	// Write value
	if _, err := buf.Write(pair.Value); err != nil {
		return nil, err
	}

	// Write tombstone
	if err := binary.Write(buf, binary.BigEndian, pair.Tombstone); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func HeaderSize() int {
	return headerSize
}

func newByteEncodeHeader(keySize, valueSize int) byteEncodeHeader {
	return byteEncodeHeader{
		KeySize:   keySize,
		ValueSize: valueSize,
	}
}

func NewByteEncoder() kv.Encoder {
	return ByteEncoder{}
}
