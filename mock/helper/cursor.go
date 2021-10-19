package helper

import (
	"github.com/jmgilman/kv/mock"
)

func NewRandomMockCursor(size int) mock.MockCursor {
	return mock.NewMockCursor(NewRandomSortedPairs(size))
}
