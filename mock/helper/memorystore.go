package helper

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/jmgilman/kv"
	"github.com/jmgilman/kv/mock"
	"github.com/tjarratt/babble"
)

func NewRandomMemoryStore(size int) mock.MockMemoryStore {
	return mock.NewMockMemoryStore(NewRandomSortedPairs(size))
}

func NewRandomPairs(size int) []kv.KVPair {
	pairs := []kv.KVPair{}
	babbler := babble.NewBabbler()
	babbler.Count = 1
	for i := 0; i < size; i++ {
		pair := kv.NewKVPair(
			babbler.Babble(),
			[]byte(babbler.Babble()),
		)
		pairs = append(pairs, pair)
	}

	return pairs
}

func NewRandomSortedPairs(size int) []kv.KVPair {
	pairs := NewRandomPairs(size)
	SortPairs(pairs)
	return pairs
}

func PrintKeys(pairs []kv.KVPair) {
	for _, pair := range pairs {
		fmt.Println(pair.Key)
	}
}

func RandomPair(pairs []kv.KVPair) kv.KVPair {
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(pairs) - 1)
	return pairs[index]
}

func SortPairs(pairs []kv.KVPair) {
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key
	})
}
