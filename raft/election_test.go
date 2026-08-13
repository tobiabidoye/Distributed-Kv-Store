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

	//kill processes
	for _, peer := range tc.peers {
		peer.KillProcess()
	}
}

func TestReelection(t *testing.T) {
	tc := MakeTestCluster(t, 3)
	leaderId := tc.CheckOneLeader()
	t.Log("first check one leader")
	if leaderId == -1 {
		t.Fatal("leader could not be elected")
	}
	t.Logf("Leader elected: Node %d, crashing leader", leaderId)
	leader := tc.peers[leaderId]
	//test to crash the leader
	leader.KillProcess()
	t.Logf("Leader Killed: %t, id: %d", leader.Killed(), leaderId)
	//sleep so that we can check for new leader
	time.Sleep(time.Second * 10)
	newId := tc.CheckOneLeader()

	if leaderId == newId {
		t.Fatalf("leader did not change even under crashed network %d", leaderId)
	}

	t.Log("Leader Changed test passed !")
	for _, peer := range tc.peers {

		id := peer.GetId()
		peer.KillProcess()
		t.Logf("Peer Killed: %t, Peer-id: %d", leader.Killed(), id)
	}

}
