package main

import (
	/* "encoding/gob" */
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/tobiabidoye/distributed-raft/cmd/util"
	"github.com/tobiabidoye/distributed-raft/kv"
	"github.com/tobiabidoye/distributed-raft/persister"
)

func main() {
	/* RegisterGobTypes() */
	filePath := "./raft_logs"
	//random node id so no chance of collisions
	nodeId := flag.Int("id", -1, "Node ID index 0, 1, 2")
	peersFlag := flag.String("peers", "", "commma separated peers, if empty falls back to dynamic ports")
	flag.Parse()

	if *nodeId == -1 {
		args := flag.Args()
		if len(args) < 1 {
			log.Fatal("most provide server id number via -id flag or first arg")
		}

		var err error
		//store arg value at dereffed nodeid
		*nodeId, err = strconv.Atoi(args[0])

		if err != nil {
			log.Fatalf("invalid node_id: %v", err)
		}
	}

	var ports []string
	if *peersFlag != "" {
		ports = strings.Split(*peersFlag, ",")
	} else {
		ports = util.DynamicPorts(3)
	}

	if *nodeId < 0 || *nodeId >= len(ports) {
		log.Fatalf("node_id must be between 0 and %d", len(ports)-1)
	}

	curPersister := persister.NewDiskPersister(filePath, *nodeId)
	rpcServer := rpc.NewServer()
	kvServer := kv.StartKVServer(ports, *nodeId, curPersister, -1, len(ports), rpcServer)
	listenAddr := ports[*nodeId]
	if lastCol := strings.LastIndex(listenAddr, ":"); lastCol != -1 {
		listenAddr = listenAddr[lastCol:]
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}

	defer listener.Close()
	log.Printf("KVServer Node %d running, listening on %s (peer advertised as %s)", *nodeId, listenAddr, ports[*nodeId])

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go rpcServer.ServeConn(conn)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Printf("Shutting down Node %d...", *nodeId)
	listener.Close()
	if kvServer != nil {
		kvServer.Kill()
	}
}
