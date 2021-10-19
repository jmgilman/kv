package kv

// import (
// 	"fmt"
// 	"reflect"
// 	"testing"

// 	"github.com/matryer/is"
// )

// func TestCompact(t *testing.T) {
// 	const count = 10
// 	const size = 100
// 	const factor = 3
// 	is := is.New(t)

// 	// Test with large random set
// 	var segments []TestSegment
// 	for i := 0; i < count; i++ {
// 		seg, err := NewRandomTestSegment(size, factor)
// 		is.NoErr(err)

// 		segments = append(segments, seg)
// 	}

// 	var cursors []Cursor
// 	for _, seg := range segments {
// 		cursors = append(cursors, seg.segment.Cursor())
// 	}

// 	writer, err := NewTestSegmentWriter()
// 	is.NoErr(err)

// 	err = Compact(cursors, writer.writer)
// 	is.NoErr(err)
// 	result := writer.encoder.EncodedPairs

// 	// Reduce expectation by 10% to account for possibility of duplicates
// 	expectedSize := (count * size) * 0.90
// 	is.True(len(result) > int(expectedSize))

// 	// Perform a sort on the result and then compare it to itself to validate
// 	// that the segments were merged in order
// 	resultPairs := make([]KVPair, len(result))
// 	copy(resultPairs, result)
// 	SortPairs(result)
// 	is.True(reflect.DeepEqual(resultPairs, result))

// 	// Test overwriting duplicates
// 	for i := 0; i < count; i++ {
// 		var pairs []KVPair
// 		for k := 'a'; k < 'd'; k++ {
// 			pairs = append(pairs, NewKVPair(string(k), []byte(fmt.Sprintf("%c%d", k, i+1))))
// 		}

// 		seg, err := NewTestSegment(pairs, factor)
// 		is.NoErr(err)
// 		cursors[i] = seg.segment.Cursor()
// 	}

// 	writer, err = NewTestSegmentWriter()
// 	is.NoErr(err)

// 	err = Compact(cursors, writer.writer)
// 	is.NoErr(err)
// 	result = writer.encoder.EncodedPairs

// 	// Order of precedent is lowest level > highest level
// 	is.Equal(result[0].Value, []byte("a1"))
// 	is.Equal(result[1].Value, []byte("b1"))
// 	is.Equal(result[2].Value, []byte("c1"))
// }
