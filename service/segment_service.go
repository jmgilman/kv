package service

import "github.com/jmgilman/kv"

type SegmentService struct {
	backend      kv.SegmentBackend
	buffer       []kv.Segment
	levels       []kv.SegmentLevel
	storeFactory kv.MemoryStoreFactory
}

func (s *SegmentService) New(store kv.MemoryStore) (kv.SegmentID, error) {
	// Create new segment
	id, err := s.backend.New(store)
	if err != nil {
		return id, err
	}

	// Load newly created segment
	segment, err := s.backend.Get(id)
	if err != nil {
		return id, err
	}

	// Add segment to buffer
	s.buffer = append([]kv.Segment{segment}, s.buffer...)

	return id, nil
}

func NewSegmentService(backend kv.SegmentBackend, storeFactory kv.MemoryStoreFactory) SegmentService {
	return SegmentService{
		backend:      backend,
		storeFactory: storeFactory,
	}
}
