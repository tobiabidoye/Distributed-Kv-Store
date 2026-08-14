package raft

import (
	"testing"
	"time"
)

func TestBasicReplication(t *testing.T) {
	clusterSize := 3
	tc := MakeTestCluster(t, 3)
	leaderId := tc.CheckOneLeader()
	if leaderId == -1 {
		t.Fatal("leader could not be elected")
	}

	t.Logf("Leader elected: Node %d ", leaderId)

	//start basic log replication
	leader := tc.peers[leaderId]
	msg := "test replicate"
	leader.Start(msg)

	//sleep then iterate to verify that followers have log items
	//then check all followers to see whether they got the command

	for ind := range clusterSize {
		curApplyChan := tc.applyChans[ind]
		select {
		case checkVar := <-curApplyChan:
			if checkVar.Command != msg {
				t.Fatal("replication failed")
			}
			if !checkVar.CommandValid {
				t.Fatalf("Node %d applied invalid command message", ind)
			}
		case <-time.After(2 * time.Second):
			// If a follower fails to apply, the test MUST fail here
			t.Fatalf("Node %d failed to apply command within 2s", ind)
		}
	}

	t.Logf("replication successful across all nodes")
}

func TestReplicationDeadFollower(t *testing.T) {
	clusterSize := 5
	tc := MakeTestCluster(t, 5)
	leaderId := tc.CheckOneLeader()
	if leaderId == -1 {
		t.Fatal("leader could not be elected")
	}

	t.Logf("Leader elected: Node %d ", leaderId)

	//start basic log replication
	leader := tc.peers[leaderId]
	msg := "test replicate"
	//kill peer in cluster
	followerId := -1
	for p := range tc.peers {
		if p != leaderId {
			followerId = p
			follower := tc.peers[p]
			follower.KillProcess()
			t.Logf("Follower killed")
			break
		}
	}
	leader.Start(msg)

	//sleep then iterate to verify that followers have log items
	//then check all followers to see whether they got the command

	for ind := range clusterSize {
		if ind == followerId {
			continue
		}
		curApplyChan := tc.applyChans[ind]
		select {
		case checkVar := <-curApplyChan:
			if checkVar.Command != msg {
				t.Fatal("replication failed")
			}
			if !checkVar.CommandValid {
				t.Fatalf("Node %d applied invalid command message", ind)
			}
		case <-time.After(2 * time.Second):
			// If a follower fails to apply, the test MUST fail here
			t.Fatalf("Node %d failed to apply command within 2s", ind)
		}
	}

	t.Logf("replication successful with dead follower")

}
