package raft

import (
	"log"
	"net"
	"net/rpc"
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
	listeners  []net.Listener
}

func startRpcConnection(rpcServer *rpc.Server, listener net.Listener, nodeId int) {
	log.Printf("KVServer Node %d running on %s", nodeId, listener.Addr().String())
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}

		go rpcServer.ServeConn(conn)
	}
}

func dynamicPorts(t *testing.T, numCluster int) ([]string, []net.Listener) {
	t.Helper()

	ports := []string{}
	listeners := []net.Listener{}

	for range numCluster {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to allocate test port: %v", err)
		}

		ports = append(ports, listener.Addr().String())
		listeners = append(listeners, listener)
	}

	return ports, listeners
}

func MakeTestCluster(t *testing.T, n int) *TestCluster {
	peers := []raftapi.Raft{}
	filePath := t.TempDir()
	applyChans := []chan raftapi.ApplyMsg{}
	ports, listeners := dynamicPorts(t, n)

	for i := range n {
		rpcServer := rpc.NewServer()

		applyCh := make(chan raftapi.ApplyMsg, 100)
		curPersister := persister.NewDiskPersister(filePath, i)
		curPeer := Make(ports, i, curPersister, applyCh, ports[i], rpcServer)

		go startRpcConnection(rpcServer, listeners[i], i)

		applyChans = append(applyChans, applyCh)
		peers = append(peers, curPeer)
	}

	cluster := &TestCluster{
		t:          t,
		n:          n,
		peers:      peers,
		applyChans: applyChans,
		ports:      ports,
		listeners:  listeners,
	}

	t.Cleanup(func() {
		cluster.KillCluster()
	})

	return cluster
}

func (test *TestCluster) KillCluster() {
	for _, p := range test.peers {
		p.KillProcess()
	}

	for _, listener := range test.listeners {
		_ = listener.Close()
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
