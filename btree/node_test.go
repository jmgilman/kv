package btree

import (
	"errors"
	"testing"

	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock/helper"
	"github.com/matryer/is"
)

func NewFixedNode() node {
	var node node

	node.put(kv.NewKVPair("m", []byte("m")))
	node.put(kv.NewKVPair("i", []byte("i")))
	node.put(kv.NewKVPair("q", []byte("q")))
	node.put(kv.NewKVPair("t", []byte("t")))
	node.put(kv.NewKVPair("b", []byte("b")))
	node.put(kv.NewKVPair("y", []byte("y")))

	return node
}

func NewRandomNode(size int) (*node, []kv.KVPair) {
	pairs := helper.NewRandomSortedPairs(size)
	return newNode(pairs), pairs
}

func TestNodeGet(t *testing.T) {
	is := is.New(t)
	node, pairs := NewRandomNode(10)

	// Get all keys
	for _, pair := range pairs {
		result, err := node.get(pair.Key)
		is.NoErr(err)
		is.Equal(result.Value, pair.Value)
	}

	// Nonexistent key
	_, err := node.get("1")
	is.True(errors.Is(err, kv.ErrorNoSuchKey))

	// Deleted key
	pair := kv.DeleteKVPair(pairs[0].Key)
	node.put(pair)
	_, err = node.get(pairs[0].Key)
	is.True(errors.Is(err, kv.ErrorNoSuchKey))
}

func TestNodeGetClosestLeft(t *testing.T) {
	is := is.New(t)
	node := NewFixedNode()

	closest := node.getClosestLeft("c")
	is.Equal(closest.pair.Key, "b")

	closest = node.getClosestLeft("l")
	is.Equal(closest.pair.Key, "i")

	closest = node.getClosestLeft("s")
	is.Equal(closest.pair.Key, "q")
}

func TestNodeGetClosestRight(t *testing.T) {
	node := NewFixedNode()
	is := is.New(t)

	closest := node.getClosestRight("c")
	is.Equal(closest.pair.Key, "i")

	closest = node.getClosestRight("l")
	is.Equal(closest.pair.Key, "m")

	closest = node.getClosestRight("t")
	is.Equal(closest.pair.Key, "y")
}

func TestNodePairs(t *testing.T) {
	is := is.New(t)
	node, pairs := NewRandomNode(10)

	result := node.pairs()
	for i, pair := range pairs {
		is.Equal(*result[i], pair)
	}
}

func TestNodePut(t *testing.T) {
	is := is.New(t)
	node := node{kv.NewKVPair("j", []byte("j")), nil, nil}

	// First node to the left
	pair := kv.NewKVPair("a", []byte("a"))
	node.put(pair)
	is.Equal(node.left.pair.Key, pair.Key)

	// Second node to the right
	pair = kv.NewKVPair("z", []byte("z"))
	node.put(pair)
	is.Equal(node.right.pair.Key, pair.Key)

	// Updates node
	pair = kv.NewKVPair("j", []byte("test"))
	node.put(pair)
	is.Equal(node.pair.Value, []byte("test"))
}

func TestNewNode(t *testing.T) {
	is := is.New(t)

	pairs := make([]kv.KVPair, 0, 3)
	pairs = append(pairs, kv.NewKVPair("a", []byte("a")))
	pairs = append(pairs, kv.NewKVPair("b", []byte("b")))
	pairs = append(pairs, kv.NewKVPair("c", []byte("c")))

	node := newNode(pairs)

	// Nodes should be balanced
	is.Equal(node.pair.Key, "b")
	is.Equal(node.left.pair.Key, "a")
	is.Equal(node.right.pair.Key, "c")
}
