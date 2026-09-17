package kv

import (
	"github.com/tobiabidoye/distributed-raft/cmd/util"
	"github.com/tobiabidoye/distributed-raft/kvrpc"
	"testing"
	"time"
)

func TestReplicationKv(t *testing.T) {
	baseDir := t.TempDir()
	for i := range 3 {
		StartTestKvServer(t, i, baseDir, 3)
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

	baseDir := t.TempDir()
	for i := range 3 {
		StartTestKvServer(t, i, baseDir, 3)
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

func TestFollowerRecovery(t *testing.T) {

	//we need to somehow ensure that a follower that has been killed recovered and has all entries
	//we will do this by killing nodes until our killed node is leader and then querying it
	baseDir := t.TempDir()
	servers := []*KVServer{}
	stopFuncs := []func(){}
	for i := range 3 {
		server, stopFunc := StartTestKvServer(t, i, baseDir, 3)
		servers = append(servers, server)
		stopFuncs = append(stopFuncs, stopFunc)
	}

	ports := util.DynamicPorts(3)
	myClerk := MakeClerk(ports)

	myClerk.Put("apple", "orange", 0)

	val, _, err := myClerk.Get("apple")

	if val == "" || err == kvrpc.ErrNoKey || val != "orange" {
		t.Fatal("item not able to be put in the key value store")
	}

	//now kill follower 2
	leaderId := myClerk.leader
	toKill := (leaderId + 1) % 3
	kill := stopFuncs[toKill]
	kill()

	//mutate state
	myClerk.Put("lanko", "banana", 0)

	val, _, err = myClerk.Get("lanko")

	if val == "" || err == kvrpc.ErrNoKey || val != "banana" {
		t.Fatal("item not able to be put in the key value store")
	}

	//now revive dead follower and kill leader
	StartTestKvServer(t, toKill, baseDir, 3)
	//get leader from cluster
	stopFuncs[leaderId]()
	//now that leader is killed perform a get
	//guaranteed to ensure that follower is caught up since if it happens it submits gets to raft to ensure stale leaders cant serve reads

	val, _, err = myClerk.Get("lanko")

	if err != kvrpc.OK || val != "banana" {
		t.Fatal("item not able to be put in the key value store")
	}

	valApple, _, errApple := myClerk.Get("apple")
	if errApple != kvrpc.OK || valApple != "orange" {
		t.Fatalf("recovered follower missing pre-crash state 'apple': got val=%q, err=%v", valApple, errApple)
	}

	t.Log("Follower Recovery is Successful")
}
