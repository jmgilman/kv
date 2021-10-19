package mock

import (
	"io"

	"github.com/jmgilman/kv"
)

type MockCursor struct {
	done  bool
	index int
	pairs []kv.KVPair
}

func (m *MockCursor) Done() bool {
	return m.done
}

func (m *MockCursor) Next() (kv.KVPair, error) {
	m.index++
	if m.index >= len(m.pairs) {
		return kv.KVPair{}, io.EOF
	}

	return m.pairs[m.index], nil
}

func (m *MockCursor) ReadToEnd() ([]kv.KVPair, error) {
	if m.index >= len(m.pairs) {
		return nil, io.EOF
	}

	pairs := m.pairs[m.index:]
	m.index = len(m.pairs)
	return pairs, nil
}

func (m *MockCursor) Reset() error {
	m.index = 0
	return nil
}

func NewMockCursor(pairs []kv.KVPair) MockCursor {
	return MockCursor{
		pairs: pairs,
	}
}
