package kv

import (
	"net"
	"net/rpc"
	"testing"

	"github.com/tobiabidoye/distributed-raft/cmd/util"
	"github.com/tobiabidoye/distributed-raft/persister"
)

func StartTestKvServer(t *testing.T, nodeId int, baseDir string, numCluster int, maxRaftState int) (*KVServer, func()) {
	//generate servers on dynamic ports 3 peers
	//will generate exact 3 ports
	filePath := baseDir

	ports := util.DynamicPorts(numCluster)
	curPersister := persister.NewDiskPersister(filePath, nodeId)
	rpcServer := rpc.NewServer()
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

			go rpcServer.ServeConn(conn)
		}
	}()
	//auto teadown of resources
	cleanupFunc := func() {
		kvSrv.rsm.Raft().KillProcess()
		listener.Close()
	}
	t.Cleanup(
		cleanupFunc,
	)

	return kvSrv, cleanupFunc
}
