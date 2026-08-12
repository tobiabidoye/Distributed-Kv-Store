package raftapi

// The Raft interface
type Raft interface {
	Start(command any) (int, int, bool)

	GetState() (int, bool)
	//getter
	GetLastIncludedIndex() int
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
	//to kill raft process
	KillProcess() bool
}

type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
