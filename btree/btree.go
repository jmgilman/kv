package btree

import (
	"github.com/jmgilman/kv"
)

// Tree implementsa a MemoryStore using an in-memory BTree for holding data
type Tree struct {
	root *node
	size int
}

func (t *Tree) Delete(key string) error {
	pair := kv.DeleteKVPair(key)
	return t.Put(pair)
}

// Get searches for the given key in the tree structure and returns its
// associated KVPair or kv.ErrorNoSuchKey if the key was not found.
func (t *Tree) Get(key string) (*kv.KVPair, error) {
	return t.root.get(key)
}

// Max returns the KVPair with the highest key in the tree structure.
func (t *Tree) Max() *kv.KVPair {
	node := t.root
	if node == nil {
		return nil
	}
	for {
		if node.right == nil {
			return &node.pair
		}
		node = node.right
	}
}

// Min returns the KVPair with the lowest key in the tree structure.
func (t *Tree) Min() *kv.KVPair {
	node := t.root
	if node == nil {
		return nil
	}
	for {
		if node.left == nil {
			return &node.pair
		}
		node = node.left
	}
}

// Range searches the tree structure and, if possible, returns the two pairs in
// which the given key sits between in left to right order (low to high).
//
// If the given key is less than the smallest key or greater than the largest
// key an ErrorOutOfRange error is returned. If the key is equal to the highest
// or lowest key in the tree, nil will be returned in the respective position.
func (t *Tree) Range(key string) (*kv.KVPair, *kv.KVPair, error) {
	if t == nil || t.root == nil {
		return nil, nil, kv.ErrorOutOfRange
	}

	min := t.Min().Key
	max := t.Max().Key

	if key < min || key > max {
		return nil, nil, kv.ErrorOutOfRange
	} else if key == min {
		rnode := t.root.getClosestRight(key)
		return nil, &rnode.pair, nil
	} else if key == max {
		lnode := t.root.getClosestLeft(key)
		return &lnode.pair, nil, nil
	}

	lnode := t.root.getClosestLeft(key)
	rnode := t.root.getClosestRight(key)

	return &lnode.pair, &rnode.pair, nil
}

// Pairs returns the contents of the tree structure as an ordered slice of
// KVPair's.
func (t *Tree) Pairs() []*kv.KVPair {
	return t.root.pairs()
}

// Put adds a new KVPair into the tree structure or updates the value of the
// associated KVPair if the key already exists.
func (t *Tree) Put(pair kv.KVPair) error {
	if t.root == nil {
		t.root = &node{pair, nil, nil}
		t.size++
	} else {
		if grown := t.root.put(pair); grown {
			t.size++
		}
	}

	return nil
}

func (t *Tree) Size() int {
	return t.size
}

// NewTreeFromSlice returns a tree structure created from a slice of ordered
// KVPair's.
func NewTreeFromSlice(pairs []kv.KVPair) Tree {
	size := len(pairs)
	if size == 0 {
		return Tree{}
	}

	return Tree{
		root: newNode(pairs),
		size: size,
	}
}
