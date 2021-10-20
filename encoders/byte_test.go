package encoders

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/matryer/is"
)

func TestBasicEncoderDecodeHeader(t *testing.T) {
	is := is.New(t)
	buf := bytes.NewBuffer([]byte{})

	// Valid header
	var keySize uint32 = 12
	var valueSize uint32 = 16

	binary.Write(buf, binary.BigEndian, keySize)
	binary.Write(buf, binary.BigEndian, valueSize)

	encoder := ByteEncoder{}
	result, err := encoder.decodeHeader(buf)
	is.NoErr(err)
	is.Equal(result.KeySize, int(keySize))
	is.Equal(result.ValueSize, int(valueSize))

	// Invalid header
	buf.Reset()
	binary.Write(buf, binary.BigEndian, uint16(keySize))
	binary.Write(buf, binary.BigEndian, uint16(valueSize))
	_, err = encoder.decodeHeader(buf)
	is.True(errors.Is(err, io.ErrUnexpectedEOF))
}

func TestBasicEncoderEncodeHeader(t *testing.T) {
	is := is.New(t)
	header := byteEncodeHeader{12, 16}

	encoder := ByteEncoder{}
	result, err := encoder.encodeHeader(header)
	is.NoErr(err)
	is.Equal(len(result), 8)
}

func TestBasicEncoderDecodePair(t *testing.T) {
	is := is.New(t)
	buf := bytes.NewBuffer([]byte{})

	key := []byte("key")
	value := []byte("value")
	tombstone := false
	encoder := ByteEncoder{}

	// Valid pair
	binary.Write(buf, binary.BigEndian, uint32(len(key)))
	binary.Write(buf, binary.BigEndian, uint32(len(value)))
	buf.Write(key)
	buf.Write(value)
	binary.Write(buf, binary.BigEndian, tombstone)

	result, err := encoder.DecodePair(buf)
	is.NoErr(err)
	is.Equal(result.Key, "key")
	is.Equal(result.Value, []byte("value"))
	is.Equal(result.Tombstone, false)

	// Invalid key
	badKey := []byte("key")
	buf.Reset()
	binary.Write(buf, binary.BigEndian, uint32(len(key)))
	binary.Write(buf, binary.BigEndian, uint32(len(value)))
	buf.Write(badKey)

	_, err = encoder.DecodePair(buf)
	is.True(errors.Is(err, io.ErrUnexpectedEOF))

	// Invalid value
	badValue := []byte("valu")
	buf.Reset()
	binary.Write(buf, binary.BigEndian, uint32(len(key)))
	binary.Write(buf, binary.BigEndian, uint32(len(value)))
	buf.Write(key)
	buf.Write(badValue)

	_, err = encoder.DecodePair(buf)
	is.True(errors.Is(err, io.ErrUnexpectedEOF))

	// Invalid tombstone
	buf.Reset()
	binary.Write(buf, binary.BigEndian, uint32(len(key)))
	binary.Write(buf, binary.BigEndian, uint32(len(value)))
	buf.Write(key)
	buf.Write(value)

	_, err = encoder.DecodePair(buf)
	is.True(errors.Is(err, io.EOF))

	// EOF
	_, err = encoder.DecodePair(buf)
	is.True(errors.Is(err, io.EOF))
}

func TestBasicEncoderEncodePair(t *testing.T) {
	is := is.New(t)
	key := "key"
	value := []byte("value")
	pair := NewKVPair(key, value, false)

	encoder := ByteEncoder{}
	result, err := encoder.EncodePair(pair)
	is.NoErr(err)
	is.Equal(len(result), headerSize+len([]byte(key))+len(value)+1)
}
