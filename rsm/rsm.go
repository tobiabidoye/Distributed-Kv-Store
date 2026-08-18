package rsm

import (
	"errors"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"

	"github.com/tobiabidoye/distributed-raft/kvrpc"
	"github.com/tobiabidoye/distributed-raft/persister"
	"github.com/tobiabidoye/distributed-raft/raftapi"

	"github.com/tobiabidoye/distributed-raft/raft"
)

var ErrNotLeader = errors.New("not leader")

type Op struct {
	Me  int
	Id  int
	Req any
}

type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	db          map[int]MapValue
	curTerm     int
	LastApplied int
}

type MapValue struct {
	ReaderSignal chan any
	Cmd          Op
}

func MakeRSM(ports []string, me int, persister *persister.DiskPersister, maxraftstate int, sm StateMachine, numClients int, server *rpc.Server) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}

	rsm.rf = raft.Make(ports, me, persister, rsm.applyCh, ports[me], server)
	rsm.db = make(map[int]MapValue)
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	go rsm.Reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (kvrpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	//
	log.Println("submit called!")
	rsm.mu.Lock()
	curId := rand.Uint32()
	safeReq := Op{Me: rsm.me, Id: int(curId), Req: req}
	cmdChan := make(chan any, 1)
	commitIndex, startTerm, isLeader := rsm.rf.Start(safeReq)

	if !isLeader {
		rsm.mu.Unlock()
		return kvrpc.ErrWrongLeader, nil
	}

	//now wait for a response from reader chan
	rsm.db[commitIndex] = MapValue{Cmd: safeReq, ReaderSignal: cmdChan}
	//received command after applying
	rsm.mu.Unlock()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	//submit calls start so command is replicated to log and then waits for result to return to client
	for {
		select {
		case tempCmd := <-cmdChan:
			log.Println("submit exitting!")
			if tempCmd == ErrNotLeader {
				return kvrpc.ErrWrongLeader, nil
			}
			return kvrpc.OK, tempCmd
		//continuously check if you are no longer leader every 100 ms
		case <-ticker.C:
			//lock the channel
			select {
			case tempCmd := <-cmdChan:

				log.Println("submit exitting!")
				if tempCmd == ErrNotLeader {
					return kvrpc.ErrWrongLeader, nil
				}
				return kvrpc.OK, tempCmd
			default:
			}
			rsm.mu.Lock()
			newTerm, isLeader := rsm.rf.GetState()
			if newTerm != startTerm || !isLeader {
				//clean up the index for that entry since no longer leader

				log.Println("submit exitting!")
				delete(rsm.db, commitIndex)
				rsm.mu.Unlock()
				return kvrpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
	//signal that leader stepped down
	//return received command to the client
}

func (rsm *RSM) Reader() {
	//gets applychannel to apply to state machine and calls doop
	for {
		applyMsg, ok := <-rsm.applyCh
		log.Printf("[RSM %d] apply received: index=%d snapshot=%v command=%#v", rsm.me, applyMsg.CommandIndex, applyMsg.SnapshotValid, applyMsg.Command)
		if !ok {
			return
		}
		//now from here
		rsm.mu.Lock()

		//is not a command its a snapshot
		if applyMsg.SnapshotValid {
			rsm.sm.Restore(applyMsg.Snapshot)
			rsm.LastApplied = applyMsg.SnapshotIndex
			rsm.mu.Unlock()
			continue
		}

		op, ok := applyMsg.Command.(Op)

		if !ok {
			rsm.mu.Unlock()
			continue
		}

		//now compare
		rsm.LastApplied = applyMsg.CommandIndex
		toSend := rsm.sm.DoOp(op.Req)
		//then send

		if rsm.rf.GetLastIncludedIndex() < rsm.LastApplied && (rsm.maxraftstate/2) < rsm.rf.PersistBytes() && rsm.maxraftstate != -1 {
			snapshot := rsm.sm.Snapshot()
			rsm.rf.Snapshot(rsm.LastApplied, snapshot)
		}

		curValue, ok := rsm.db[applyMsg.CommandIndex]
		if ok {
			delete(rsm.db, applyMsg.CommandIndex)

			var tempSend any

			if op.Id != curValue.Cmd.Id {
				//send -1 if leader steps down
				tempSend = ErrNotLeader
			} else {
				tempSend = toSend
			}
			log.Printf("[RSM %d] signaling waiter for index=%d", rsm.me, applyMsg.CommandIndex)
			rsm.mu.Unlock()
			curValue.ReaderSignal <- tempSend
			continue
		}

		rsm.mu.Unlock()
	}

}
