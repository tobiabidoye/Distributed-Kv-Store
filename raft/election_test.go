package raft

import (
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	tc := MakeTestCluster(t, 3)
	leaderId := tc.CheckOneLeader()
	if leaderId == -1 {
		t.Fatal("leader could not be elected")
	}
	t.Logf("Leader elected: Node %d", leaderId)
	//check if leader has changed with stable network
	time.Sleep(time.Second * 1)
	newId := tc.CheckOneLeader()

	if leaderId != newId {
		t.Fatalf("Leader changed unexpectedly from %d to %d without network disruption", leaderId, newId)
	}
}
