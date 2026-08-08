package main

import (
	/* "encoding/gob" */
	"log"
	"os"
	"strconv"

	"github.com/tobiabidoye/raft"
	"github.com/tobiabidoye/raft/persister"
	"github.com/tobiabidoye/raft/raftapi"
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
} */

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
	ports := []string{"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"}
	applyCh := make(chan raftapi.ApplyMsg)
	raft.Make(ports, nodeId, curPersister, applyCh, ports[nodeId])
	select {}
}
