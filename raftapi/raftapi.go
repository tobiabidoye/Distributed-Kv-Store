package raftapi

type Raft interface {
	Start(command any) (int, int, bool)
	GetState() (int, bool)
	//getter
	GetLastIncludedIndex() int
	Snapshot(index int, snapshot []byte)
	PersistBytes() int
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
