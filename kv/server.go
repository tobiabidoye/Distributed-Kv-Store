package kv

import (
	"bytes"
	"errors"

	"sync"

	"encoding/gob"
	"net/rpc"

	"github.com/tobiabidoye/distributed-raft/kvrpc"
	"github.com/tobiabidoye/distributed-raft/persister"
	"github.com/tobiabidoye/distributed-raft/rsm"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	dedupTracker map[FilterKey]VersionErr
	kvStore      map[string]ValueVersion
	mu           sync.Mutex
}

type FilterKey struct {
	Key      string
	ClientId int64
}
type VersionErr struct {
	VersionNo int
	Err       kvrpc.Err
}

type ValueVersion struct {
	Value     string
	VersionNo int
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case kvrpc.PutArgs:
		//check if it exists in the deduplication map
		if dedupCheck, ok := kv.dedupTracker[FilterKey{Key: r.Key, ClientId: r.ClerkId}]; ok {
			if r.Version == kvrpc.Tversion(dedupCheck.VersionNo) {
				//equivalent versions return cached version
				return kvrpc.PutReply{Err: dedupCheck.Err}
			}
		}

		//check if it is in the store, works for zero values since the map will send zero value if key doesnt exist
		if int(r.Version) == kv.kvStore[r.Key].VersionNo {
			//store new put
			//in dedup tracker store version the client sees
			kv.dedupTracker[FilterKey{Key: r.Key, ClientId: r.ClerkId}] = VersionErr{VersionNo: int(r.Version), Err: kvrpc.OK}
			kv.kvStore[r.Key] = ValueVersion{VersionNo: int(r.Version + 1), Value: r.Value}
			return kvrpc.PutReply{Err: kvrpc.OK}
		}

		/* if r.Version < rpc.Tversion(kv.kvStore[r.Key].versionNo) {
			kv.dedupTracker[FilterKey{key: r.Key, clientId: r.ClerkId}] = VersionErr{versionNo: int(r.Version), Err: rpc.OK}
			return rpc.PutReply{Err: rpc.OK}
		} */
		//cant store non zero version that does not exist
		return kvrpc.PutReply{Err: kvrpc.ErrVersion}
	case kvrpc.GetArgs:
		if val, ok := kv.kvStore[r.Key]; ok {
			return kvrpc.GetReply{Value: val.Value, Version: kvrpc.Tversion(val.VersionNo), Err: kvrpc.OK}
		}
		//fake key in the map
		return kvrpc.GetReply{Err: kvrpc.ErrNoKey}
	default:
		return errors.New("Doop args not recognized unfortunately")
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(kv.kvStore); err != nil {
		panic(err)
	}

	if err := enc.Encode(kv.dedupTracker); err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 {
		return
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	/* kv.dedupTracker = make(map[FilterKey]VersionErr)
	kv.kvStore = make(map[string]ValueVersion) */

	tempStore := make(map[string]ValueVersion)
	tempDedup := make(map[FilterKey]VersionErr)
	if dec.Decode(&tempStore) != nil || dec.Decode(&tempDedup) != nil {
		return
	} else {
		kv.kvStore = tempStore
		kv.dedupTracker = tempDedup
	}
}

func (kv *KVServer) Get(args *kvrpc.GetArgs, reply *kvrpc.GetReply) error {
	err, tempResp := kv.rsm.Submit(*args)
	if err == kvrpc.ErrWrongLeader {
		reply.Err = kvrpc.ErrWrongLeader
		return nil
	}

	resp, ok := tempResp.(kvrpc.GetReply)
	if !ok {
		//early return for failed assertion
		reply.Err = kvrpc.ErrWrongLeader
		return nil
	}

	reply.Err = resp.Err
	reply.Value = resp.Value
	reply.Version = resp.Version
	return nil
}

func (kv *KVServer) Put(args *kvrpc.PutArgs, reply *kvrpc.PutReply) error {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, tempResp := kv.rsm.Submit(*args)

	if err == kvrpc.ErrWrongLeader {
		reply.Err = kvrpc.ErrWrongLeader
		return nil
	}

	resp, ok := tempResp.(kvrpc.PutReply)
	if !ok {
		//early return for failed assertion
		reply.Err = kvrpc.ErrWrongLeader
		return nil
	}

	reply.Err = resp.Err
	return nil
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(ports []string, me int, persister *persister.DiskPersister, maxraftstate int, numClients int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(rsm.Op{})
	gob.Register(kvrpc.PutArgs{})
	gob.Register(kvrpc.GetArgs{})
	gob.Register(FilterKey{})
	gob.Register(VersionErr{})
	gob.Register(ValueVersion{})
	kv := &KVServer{me: me}
	kv.dedupTracker = make(map[FilterKey]VersionErr)
	kv.kvStore = make(map[string]ValueVersion)
	rpc.Register(kv)
	kv.rsm = rsm.MakeRSM(ports, me, persister, maxraftstate, kv, numClients)
	// You may need initialization code here.
	return kv
}

/* func NewServer(ends []*rpc.Client, srv int, persister *persister.DiskPersister, maxRaftState int, numClients int) *KVServer {
	return StartKVServer(ends, srv, persister, maxRaftState, numClients)
} */
