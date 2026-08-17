package main

import (
	/* "encoding/gob" */
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/tobiabidoye/distributed-raft/kv"
	"github.com/tobiabidoye/distributed-raft/persister"
)

/* func RegisterGobTypes() {
gob.Register(rsm.Op{})
gob.Register(rpc.PutArgs{})
gob.Register(rpc.GetArgs{})
gob.Register(rpc.PutReply{})
gob.Register(rpc.GetReply{})
gob.Register(FilterKey{})
gob.Register(VersionErr{})
gob.Register(ValueVersion{})
*/

func dynamicPorts(numPeers int) []string {
	ports := []string{}
	start := 8000
	end := start + numPeers

	for port := start; port < end; port++ {
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		ports = append(ports, addr)
	}

	return ports
}

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
	ports := dynamicPorts(3)
	/* kv.StartKVServer(ports, nodeId, curPersister, -1, len(ports)) */
	kv.StartKVServer(ports, nodeId, curPersister, -1, len(ports))
	log.Printf("KVServer Node %d running on %s", nodeId, ports[nodeId])
	select {}
}
