package main

import (
	/* "encoding/gob" */
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"

	"github.com/tobiabidoye/distributed-raft/cmd/util"
	"github.com/tobiabidoye/distributed-raft/kv"
	"github.com/tobiabidoye/distributed-raft/persister"
)

func main() {
	/* RegisterGobTypes() */
	filePath := "./raft_logs"
	//random node id so no chance of collisions
	args := os.Args[1:]
	if len(args) < 1 {
		log.Print("must add server id number")
		return
	}
	//statically enter node id
	nodeId, err := strconv.Atoi(args[0])
	if err != nil || nodeId < 0 || nodeId > 2 {
		log.Fatal("node_id must be a valid index: 0, 1, or 2")
	}

	curPersister := persister.NewDiskPersister(filePath, nodeId)
	//unsure where peers list will come from
	ports := util.DynamicPorts(3)
	/* kv.StartKVServer(ports, nodeId, curPersister, -1, len(ports)) */
	rpcServer := rpc.NewServer()
	kv.StartKVServer(ports, nodeId, curPersister, -1, len(ports), rpcServer)
	listener, err := net.Listen("tcp", ports[nodeId])
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("KVServer Node %d running on %s", nodeId, ports[nodeId])
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}

		go rpcServer.ServeConn(conn)
	}
}
