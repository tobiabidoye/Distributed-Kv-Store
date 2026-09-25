package kv

import (
	"maps"

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
	Step: func(state, input, output any) (bool, any) {
		st := state.(KvState)
		inp := input.(KvInput)
		out := output.(KvOutput)

		next := KvState{
			Data: make(map[string]struct {
				Value   string
				Version uint64
			}),
		}

		maps.Copy(next.Data, st.Data)
		entry, exists := next.Data[inp.Key]

		switch inp.Op {
		//what get result is supposed to be and state after a get
		case "Get":
			if !exists {
				return out.Err == "ErrNoKey", next
			}
			match := (out.Err == "OK" && out.Value == entry.Value && out.Version == entry.Version)
			return match, next
		case "Put":
			currentVer := entry.Version
			if !exists {
				currentVer = 0
			}

			if inp.Version == currentVer {
				if out.Err != "OK" {
					return false, next
				}

				next.Data[inp.Key] = struct {
					Value   string
					Version uint64
				}{Value: inp.Value, Version: currentVer + 1}
				return true, next
			} else {
				return out.Err == "ErrVersion", next
			}
		}
		return false, next
	},
}
