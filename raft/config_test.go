package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/tobiabidoye/distributed-raft/persister"
	"github.com/tobiabidoye/distributed-raft/raftapi"
)

type TestCluster struct {
	t          *testing.T
	n          int
	peers      []raftapi.Raft
	applyChans []chan raftapi.ApplyMsg
	ports      []string
}

func MakeTestCluster(t *testing.T, n int) *TestCluster {
	ports := []string{}
	start := 8000
	end := start + n
	peers := []raftapi.Raft{}
	filePath := t.TempDir()
	applyChans := []chan raftapi.ApplyMsg{}
	for port := start; port < end; port++ {
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		ports = append(ports, addr)
	}

	//then start up the cluster of raft peers
	for i := range n {

		applyCh := make(chan raftapi.ApplyMsg, 100)
		curPersister := persister.NewDiskPersister(filePath, i)
		curPeer := Make(ports, i, curPersister, applyCh, ports[i])
		applyChans = append(applyChans, applyCh)
		peers = append(peers, curPeer)
	}

	return &TestCluster{
		t:          t,
		n:          n,
		peers:      peers,
		applyChans: applyChans,
		ports:      ports,
	}

}

func (test *TestCluster) KillCluster() {
	for _, p := range test.peers {
		//kill each peer
		p.KillProcess()
	}

	test.t.Log("Raft processes killed!")
}

func (test *TestCluster) CheckOneLeader() int {
	//test with 3 leaders

	start := time.Now()
	for time.Since(start) < time.Second*3 {

		leaderId := -1
		leaderCount := 0
		for ind, peer := range test.peers {
			if peer.Killed() {
				continue
			}
			_, isLeader := peer.GetState()
			if isLeader {
				leaderId = ind
				leaderCount++
			}
		}

		if leaderCount == 1 {
			return leaderId
		}

		time.Sleep(time.Millisecond * 50)
	}
	test.t.Fatalf("expected 1 leader, but consensus failed to stabilize within 3s")
	return -1
}
