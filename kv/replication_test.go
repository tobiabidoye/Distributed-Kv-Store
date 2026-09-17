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

	if val == "" || err == kvrpc.ErrNoKey || val != "orange" {
		t.Fatal("item not able to be put in the key value store")
	}

	t.Logf("item replicated successfully %s:", val)
}

func TestOverWriteKv(t *testing.T) {
	baseDirs := []string{}
	for i := range 3 {
		baseDir := t.TempDir()
		baseDirs = append(baseDirs, baseDir)
		StartTestKvServer(t, i, baseDir)
	}

	ports := util.DynamicPorts(3)
	myClerk := MakeClerk(ports)
	myClerk.Put("apple", "orange", 0)

	val, _, err := myClerk.Get("apple")

	if val == "" || err == kvrpc.ErrNoKey || val != "orange" {
		t.Fatal("item not able to be put in the key value store")
	}

	myClerk.Put("apple", "banana", 0)
	val, _, err = myClerk.Get("apple")

	if val == "" || err == kvrpc.ErrNoKey || val != "orange" {
		t.Fatal("false update in kv store")
	}

	myClerk.Put("apple", "banana", 1)
	val, _, err = myClerk.Get("apple")

	if val == "" || err == kvrpc.ErrNoKey || val != "banana" {
		t.Fatal("update to key not successful")
	}

	t.Log("overwrite logic is shown to be good")
}


