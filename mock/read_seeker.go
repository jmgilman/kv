package mock

type MockReadSeeker struct {
	ReadFn func(p []byte) (int, error)
	SeekFn func(offset int64, whence int) (int64, error)
}

func (m *MockReadSeeker) Read(p []byte) (int, error) {
	return m.ReadFn(p)
}

func (m *MockReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return m.SeekFn(offset, whence)
}

func NewMockReadSeeker() MockReadSeeker {
	return MockReadSeeker{
		ReadFn: func(p []byte) (int, error) { return 0, nil },
		SeekFn: func(offset int64, whence int) (int64, error) { return 0, nil },
	}
}
