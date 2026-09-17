package kv

import (
	"testing"
	"time"

	"github.com/tobiabidoye/distributed-raft/cmd/util"
	"github.com/tobiabidoye/distributed-raft/kvrpc"
)

func TestReplicationKv(t *testing.T) {
	baseDirs := []string{}
	for i := range 3 {
		baseDir := t.TempDir()
		baseDirs = append(baseDirs, baseDir)
		StartTestKvServer(t, i, baseDir)
	}

	ports := util.DynamicPorts(3)
	myClerk := MakeClerk(ports)
	myClerk.Put("apple", "orange", 0)
	time.Sleep(1 * time.Second)
	t.Log("item put in kv store testing if puts actually work over network")
	val, _, err := myClerk.Get("apple")

	if val == "" || err == kvrpc.ErrNoKey {
		t.Fatal("item not able to be put in the key value store")
	}

	t.Logf("item replicated successfully %s:", val)
}
