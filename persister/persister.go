package persister

import (
	"fmt"
	"os"
	"sync"
)

type DiskPersister struct {
	mu           sync.Mutex
	statePath    string
	snapshotPath string
}

func NewDiskPersister(dataDir string, nodeID int) *DiskPersister {
	//create missing parent directories and owner has rwx perms
	os.MkdirAll(dataDir, 0755)
	return &DiskPersister{
		statePath:    fmt.Sprintf("%s/raft_state_%d.bin", dataDir, nodeID),
		snapshotPath: fmt.Sprintf("%s/raft_snapshot_%d.bin", dataDir, nodeID),
	}
}

func (dp *DiskPersister) Save(raftState []byte, snapshotState []byte) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if raftState != nil {
		dp.AtmomicWrite(dp.statePath, raftState)
	}

	if snapshotState != nil {
		dp.AtmomicWrite(dp.snapshotPath, snapshotState)
	}
}

func (dp *DiskPersister) AtmomicWrite(path string, data []byte) {
	tmpPath := path + ".tmp"
	//temp file opening
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}

	f.Write(data)
	f.Sync()
	f.Close()
	os.Rename(tmpPath, path)
}

func (dp *DiskPersister) ReadRaftState() []byte {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	data, _ := os.ReadFile(dp.statePath)
	return data
}

func (dp *DiskPersister) ReadSnapshot() []byte {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	data, _ := os.ReadFile(dp.snapshotPath)
	return data
}

func (dp *DiskPersister) RaftStateSize() int {
	fileInfo, err := os.Stat(dp.statePath)
	if err != nil {
		return 0
	}
	fileSize := fileInfo.Size()
	return int(fileSize)
}
