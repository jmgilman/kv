package kv_test

import (
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock"
	"github.com/jmgilman/kv/mock/helper"
	"github.com/matryer/is"
)

func TestCursorDone(t *testing.T) {
	size := 5
	is := is.New(t)

	// Make mock encoder with random entries
	encoder := mock.MockEncoder{}
	file, _ := encoder.Set(helper.NewRandomSortedPairs(size))

	// Iterate through all entries
	cursor := kv.NewCursor(&encoder, file)
	for i := 0; i < size; i++ {
		cursor.Next()
	}

	// Cursor sets set done to true
	cursor.Next()
	is.True(cursor.Done())
}

func TestCursorNext(t *testing.T) {
	size := 5
	is := is.New(t)

	// Make mock encoder with random entries
	encoder := mock.MockEncoder{}
	file, _ := encoder.Set(helper.NewRandomSortedPairs(size))

	// Cursor returns entries in correct order
	cursor := kv.NewCursor(&encoder, file)
	pairs := encoder.Pairs()
	for i := 0; i < size; i++ {
		pair, err := cursor.Next()
		is.NoErr(err)
		is.Equal(pairs[i].Key, pair.Key)
		is.Equal(pairs[i].Value, pair.Value)
	}

	// Cursor returns EOF
	_, err := cursor.Next()
	is.True(errors.Is(err, io.EOF))
}

func TestCursorReadToEnd(t *testing.T) {
	size := 5
	is := is.New(t)

	// Make mock encoder with random entries
	encoder := mock.MockEncoder{}
	file, _ := encoder.Set(helper.NewRandomSortedPairs(size))
	pairs := encoder.Pairs()

	// Read off the first pair
	cursor := kv.NewCursor(&encoder, file)
	_, err := cursor.Next()
	is.NoErr(err)

	// Returns all remaining pairs
	result, err := cursor.ReadToEnd()
	is.NoErr(err)
	is.True(reflect.DeepEqual(result, pairs[1:]))
}

func TestCursorReset(t *testing.T) {
	size := 5
	is := is.New(t)

	// Make mock encoder with random entries
	encoder := mock.MockEncoder{}
	file, _ := encoder.Set(helper.NewRandomSortedPairs(size))
	pairs := encoder.Pairs()

	// Read all pairs
	cursor := kv.NewCursor(&encoder, file)
	_, err := cursor.ReadToEnd()
	is.NoErr(err)

	// Reset
	err = cursor.Reset()
	is.NoErr(err)

	// Read pair is equal to the first
	pair, err := cursor.Next()
	is.NoErr(err)
	is.Equal(pair.Key, pairs[0].Key)
	is.Equal(pair.Value, pairs[0].Value)

}
