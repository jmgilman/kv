package kv

import (
	"errors"
	"strings"
)

var ErrorKeyTooLarge = errors.New("key exceeds max size")
var ErrorNoSuchKey = errors.New("no such key")
var ErrorOutOfRange = errors.New("key is out of range")
var ErrorValueTooLarge = errors.New("value exceeds max size")

// KVPair is the elementary structure for storing key/value pairs
type KVPair struct {
	Key   string
	Value []byte
}

func NewKVPair(key string, value []byte) KVPair {
	return KVPair{strings.ToLower(key), value}
}
