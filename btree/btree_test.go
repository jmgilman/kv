package btree

import (
	"errors"
	"testing"

	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock/helper"
	"github.com/matryer/is"
)

func NewFixedTree() Tree {
	var tree Tree

	tree.Put("m", []byte("m"))
	tree.Put("i", []byte("i"))
	tree.Put("q", []byte("q"))
	tree.Put("t", []byte("t"))
	tree.Put("b", []byte("b"))
	tree.Put("y", []byte("y"))

	return tree
}

func NewRandomTree(size int) (Tree, []kv.KVPair) {
	pairs := helper.NewRandomSortedPairs(size)
	return NewTreeFromSlice(pairs), pairs
}

func TestTreeGet(t *testing.T) {
	is := is.New(t)
	tree, pairs := NewRandomTree(10)

	// Get all keys
	for _, pair := range pairs {
		result, err := tree.Get(pair.Key)
		is.NoErr(err)
		is.Equal(result.Value, pair.Value)
	}

	// Nonexistent key
	_, err := tree.Get("1")
	is.True(errors.Is(err, kv.ErrorNoSuchKey))
}

func TestTreeMax(t *testing.T) {
	is := is.New(t)
	tree, pairs := NewRandomTree(10)

	result := tree.Max()
	is.Equal(pairs[len(pairs)-1].Key, result.Key)
}

func TestTreeMin(t *testing.T) {
	is := is.New(t)
	tree, pairs := NewRandomTree(10)

	result := tree.Min()
	is.Equal(pairs[0].Key, result.Key)
}

func TestTreeRange(t *testing.T) {
	tree := NewFixedTree()
	is := is.New(t)

	// Key is below minimum
	l, r, err := tree.Range("a")
	is.True(errors.Is(err, kv.ErrorOutOfRange))

	// Key is minimum
	l, r, err = tree.Range("b")
	is.Equal(l, nil)
	is.Equal(r.Key, "i")

	// Key in range
	l, r, err = tree.Range("j")
	is.Equal(l.Key, "i")
	is.Equal(r.Key, "m")

	// Key in range
	l, r, err = tree.Range("s")
	is.Equal(l.Key, "q")
	is.Equal(r.Key, "t")

	// Key is max
	l, r, err = tree.Range("y")
	is.Equal(l.Key, "t")
	is.Equal(r, nil)

	// Key is above max
	l, r, err = tree.Range("z")
	is.True(errors.Is(err, kv.ErrorOutOfRange))
}

func TestTreePairs(t *testing.T) {
	is := is.New(t)
	tree, pairs := NewRandomTree(10)

	result := tree.Pairs()
	for i, pair := range pairs {
		is.Equal(*result[i], pair)
	}
}

func TestTreePut(t *testing.T) {
	is := is.New(t)
	var tree Tree

	// First node is root
	pair := kv.NewKVPair("j", []byte("j"))
	tree.Put("j", []byte("j"))
	is.Equal(tree.root.pair.Key, pair.Key)

	// Second node to the left
	pair = kv.NewKVPair("a", []byte("a"))
	tree.Put("a", []byte("a"))
	is.Equal(tree.root.left.pair.Key, pair.Key)

	// Third node to the right
	pair = kv.NewKVPair("z", []byte("z"))
	tree.Put("z", []byte("z"))
	is.Equal(tree.root.right.pair.Key, pair.Key)

	// Updates node
	pair = kv.NewKVPair("j", []byte("test"))
	tree.Put("j", []byte("test"))
	is.Equal(tree.root.pair.Value, []byte("test"))

}

func TestTreeSize(t *testing.T) {
	is := is.New(t)
	tree, pairs := NewRandomTree(10)
	is.Equal(tree.size, len(pairs))
}

func TestNewTreeFromSlice(t *testing.T) {
	is := is.New(t)
	pairs := helper.NewRandomSortedPairs(10)

	tree := NewTreeFromSlice(pairs)
	result := tree.Pairs()
	for i, pair := range pairs {
		is.Equal(*result[i], pair)
	}
}
