package kv

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/tobiabidoye/distributed-raft/cmd/util"
	"github.com/tobiabidoye/distributed-raft/persister"
)

func StartTestKvServer(t *testing.T, nodeId int, baseDir string, numCluster int, maxRaftState int) (*KVServer, func()) {
	//generate servers on dynamic ports 3 peers
	//will generate exact 3 ports
	filePath := baseDir

	ports := util.DynamicPorts(numCluster)
	curPersister := persister.NewDiskPersister(filePath, nodeId)
	curPersister.DisableSync = true
	rpcServer := rpc.NewServer()
	connMap := make(map[net.Conn]struct{})
	var connMu sync.Mutex
	kvSrv := StartKVServer(ports, nodeId, curPersister, maxRaftState, len(ports), rpcServer)

	listener, err := net.Listen("tcp", ports[nodeId])
	if err != nil {
		t.Cleanup(func() {
			kvSrv.rsm.Raft().KillProcess()
		})
		t.Fatalf("failed to listen on %s: %v", ports[nodeId], err)
	}

	t.Logf("KVServer Node %d running on %s", nodeId, ports[nodeId])
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			//store the connection to be closed
			connMu.Lock()
			connMap[conn] = struct{}{}
			connMu.Unlock()

			go func(conny net.Conn) {
				rpcServer.ServeConn(conny)
				connMu.Lock()
				delete(connMap, conny)
				connMu.Unlock()
			}(conn)
		}
	}()
	//auto teadown of resources
	cleanupFunc := func() {
		listener.Close()
		connMu.Lock()
		for c := range connMap {
			c.Close()
			delete(connMap, c)
		}
		connMu.Unlock()
		kvSrv.rsm.Raft().KillProcess()
		time.Sleep(30 * time.Millisecond)
	}
	t.Cleanup(
		cleanupFunc,
	)

	return kvSrv, cleanupFunc
}

func createTestDir(t *testing.T) (string, func()) {
	dir, err := os.MkdirTemp("", "raft-kv-*")
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		for range 10 {
			if err := os.RemoveAll(dir); err == nil || os.IsNotExist(err) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	return dir, cleanup
}
