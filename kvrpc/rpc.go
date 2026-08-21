package kvrpc

type Err string

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	ErrMaybe = "ErrMaybe"

	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
	ClerkId int64
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
