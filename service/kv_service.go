package service

import (
	"errors"

	"github.com/jmgilman/kv"
)

type KVService struct {
	memStore kv.MemoryStore
	nvStore  kv.NVStore
}

func (k *KVService) Delete(key string) error {
	return k.memStore.Delete(key)
}

func (k *KVService) Get(key string) (*kv.KVPair, error) {
	// Search memory store first
	pair, err := k.memStore.Get(key)
	if err != nil {
		if !errors.Is(err, kv.ErrorNoSuchKey) {
			return nil, err
		}
	} else {
		return pair, nil
	}

	return nil, kv.ErrorNoSuchKey
	// Next try the non-volatile store
	// pair, err = k.nvStore.Get(key)
	// if err != nil {
	// 	return nil, err
	// }

	// return pair, nil
}

func (k *KVService) Put(key string, value []byte) error {
	return k.memStore.Put(kv.NewKVPair(key, value))
}

func NewKVService(memStore kv.MemoryStore, nvStore kv.NVStore) KVService {
	return KVService{
		memStore: memStore,
		nvStore:  nvStore,
	}
}
