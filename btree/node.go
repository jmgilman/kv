package btree

import "github.com/jmgilman/kv"

// node represents a node in a Tree
type node struct {
	pair  kv.KVPair
	left  *node
	right *node
}

// get searches for the given key in the tree node and returns its associated
// KVPair or ErrorNoSuchKey if the key was not found.
func (n *node) get(key string) (*kv.KVPair, error) {
	if n == nil {
		return &kv.KVPair{}, kv.ErrorNoSuchKey
	}

	if key == n.pair.Key {
		return &n.pair, nil
	}

	if key < n.pair.Key {
		return n.left.get(key)
	} else {
		return n.right.get(key)
	}
}

// getClosestLeft attempts to find the closest left-side node of the given key.
// It is assumed that the key being passed falls within the range of the tree
// and is not the lowest key.
func (n *node) getClosestLeft(key string) *node {
	if key <= n.pair.Key {
		if n.left == nil {
			return n
		} else {
			return n.left.getClosestLeft(key)
		}
	} else {
		if n.right == nil {
			return n
		} else if key <= n.right.pair.Key && n.right.left == nil {
			return n
		} else {
			return n.right.getClosestLeft(key)
		}
	}
}

// getClosestRight attempts to find the closest right-side node of the given
// key. It is assumed that the key being passed falls within the range of the
// tree and is not the highest key.
func (n *node) getClosestRight(key string) *node {
	if key < n.pair.Key {
		if n.left == nil {
			return n
		} else if key >= n.left.pair.Key && n.left.right == nil {
			return n
		} else {
			return n.left.getClosestRight(key)
		}
	} else {
		if n.right == nil {
			return n
		} else {
			return n.right.getClosestRight(key)
		}
	}
}

// pairs returns the contents of the tree node as an ordered slice of
// KVPair's.
func (n *node) pairs() []*kv.KVPair {
	var pairs []*kv.KVPair
	if n == nil {
		return pairs
	}

	left := n.left.pairs()
	right := n.right.pairs()

	pairs = append(pairs, left...)
	pairs = append(pairs, &n.pair)
	pairs = append(pairs, right...)
	return pairs
}

// put adds a new KVPair into the tree node or updates the value of the
// associated KVPair if the key already exists.
func (n *node) put(kv kv.KVPair) bool {
	if kv.Key < n.pair.Key {
		if n.left == nil {
			n.left = &node{kv, nil, nil}
			return true
		} else {
			return n.left.put(kv)
		}
	} else if kv.Key > n.pair.Key {
		if n.right == nil {
			n.right = &node{kv, nil, nil}
			return true
		} else {
			return n.right.put(kv)
		}
	} else {
		n.pair = kv
		return false
	}
}

// newNode returns a new node created from a slice of ordered KVPair's.
func newNode(pairs []kv.KVPair) *node {
	size := len(pairs)
	if size == 0 {
		return nil
	}

	node := &node{
		pair: pairs[size/2],
		left: newNode(pairs[0 : size/2]),
	}
	if i := size/2 + 1; i < size {
		node.right = newNode(pairs[i:size])
	}

	return node
}
