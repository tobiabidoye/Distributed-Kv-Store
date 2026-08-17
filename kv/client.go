package kv

import (
	"math/rand/v2"
	"net/rpc"
	"time"

	"github.com/tobiabidoye/distributed-raft/kvrpc"
)

type Clerk struct {
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
	clerkId int64
	clients []*rpc.Client
}

func MakeClerk(servers []string) *Clerk {
	ck := &Clerk{servers: servers, clerkId: rand.Int64(), leader: 0, clients: make([]*rpc.Client, len(servers))}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) getClient(serverIdx int) (*rpc.Client, error) {
	if ck.clients[serverIdx] != nil {
		return ck.clients[serverIdx], nil
	}

	client, err := rpc.Dial("tcp", ck.servers[serverIdx])

	if err != nil {
		return nil, err
	}

	ck.clients[serverIdx] = client
	return client, nil
}
func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, kvrpc.Tversion, kvrpc.Err) {

	for {
		args := kvrpc.GetArgs{Key: key}
		reply := kvrpc.GetReply{}
		client, err := ck.getClient(ck.leader)
		if err == nil {

			callErr := client.Call("KVServer.Get", &args, &reply)
			if callErr == nil {

				if reply.Err == kvrpc.ErrNoKey {
					return "", 0, kvrpc.ErrNoKey
				}

				//for all other errors retry
				if reply.Err == kvrpc.OK {
					return reply.Value, reply.Version, reply.Err
				}
			} else {
				client.Close()
				ck.clients[ck.leader] = nil
			}

			//loop through the leaders if it fails
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)

	}
}

func (ck *Clerk) Put(key string, value string, version kvrpc.Tversion) kvrpc.Err {
	isRetry := false
	for {
		args := kvrpc.PutArgs{Key: key, Value: value, Version: version, ClerkId: ck.clerkId}
		reply := kvrpc.PutReply{}
		client, err := ck.getClient(ck.leader)
		if err == nil {

			callErr := client.Call("KVServer.Put", &args, &reply)
			if callErr == nil {

				if isRetry == false && reply.Err == kvrpc.ErrVersion {
					return kvrpc.ErrVersion
				} else if isRetry == true && reply.Err == kvrpc.ErrVersion {
					return kvrpc.ErrMaybe
				}

				if reply.Err == kvrpc.OK {
					return reply.Err
				}
			} else {
				client.Close()
				ck.clients[ck.leader] = nil
			}

		}

		isRetry = true
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)

	}
}
