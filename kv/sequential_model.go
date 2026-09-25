package kv

import (
	"github.com/anishathalye/porcupine"
	"maps"
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

			if out.Err == "ErrMaybe" {
				if inp.Version == currentVer {
					next.Data[inp.Key] = struct {
						Value   string
						Version uint64
					}{Value: inp.Value, Version: currentVer + 1}
					return true, next
				}
				return true, next
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
	//adding due to bug from equality comparison
	//porcupine will compare maps using == we need to overload and create a equality comparison for our needs
	Equal: func(s1, s2 any) bool {
		st1 := s1.(KvState).Data
		st2 := s2.(KvState).Data
		if len(st1) != len(st2) {
			return false
		}
		for k, v1 := range st1 {
			v2, ok := st2[k]
			if !ok || v1 != v2 {
				return false
			}
		}
		return true
	},
}
