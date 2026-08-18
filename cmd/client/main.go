package main

import (
	"fmt"
	"log"

	"github.com/tobiabidoye/distributed-raft/cmd/util"
	"github.com/tobiabidoye/distributed-raft/kv"
)

func main() {
	ports := util.DynamicPorts(3)
	myClerk := kv.MakeClerk(ports)
	myClerk.Put("apple", "orange", 0)
	log.Println("apple put in store")
	val, _, _ := myClerk.Get("apple")

	fmt.Println(val)
}
