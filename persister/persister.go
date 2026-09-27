package persister

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type DiskPersister struct {
	mu           sync.Mutex
	statePath    string
	snapshotPath string
	DisableSync  bool
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
		if err := dp.AtomicWrite(dp.statePath, raftState); err != nil {
			log.Fatalf("Persister error from writing to disk %v", err)
		}
	}

	if snapshotState != nil {
		if err := dp.AtomicWrite(dp.snapshotPath, snapshotState); err != nil {
			log.Fatalf("Persister error from writing to disk %v", err)
		}
	}
}

// productionized changes to atomic write for aws
func (dp *DiskPersister) AtomicWrite(path string, data []byte) error {
	tmpPath := path + ".tmp"
	//temp file opening
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	if !dp.DisableSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}

	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}

	//ensure everything is flushed properly to disk
	if !dp.DisableSync {
		dirPath := filepath.Dir(path)
		if df, err := os.Open(dirPath); err == nil {
			_ = df.Sync()
			_ = df.Close()
		}
	}
	return nil
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
