package kv

import (
	"structs"

	"github.com/anishathalye/porcupine"
)

// input to key value store
type KvInput struct {
	Op      string
	Key     string
	Value   string
	Version uint64
}

// output
type KvOutput struct {
	Value   string
	Version uint64
	Err     string
}

type KvState struct {
	Data map[string]struct {
		Value   string
		Version uint64
	}
}

var kvModel = porcupine.Model{
	Init: func() any {
		return KvState{
			Data: make(map[string]struct {
				Value   string
				Version uint64
			}),
		}
	},
}
