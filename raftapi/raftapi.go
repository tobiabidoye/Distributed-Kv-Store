package raftapi

// The Raft interface
type Raft interface {
	Start(command any) (int, int, bool)
	//returns term and isleader
	GetState() (int, bool)
	//getter
	GetLastIncludedIndex() int
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
	//to kill raft process
	KillProcess() bool
	Killed() bool
	GetId() int
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
