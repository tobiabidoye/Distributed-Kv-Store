package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"runtime"
	"slices"
	"sync"

	"time"

	"github.com/tobiabidoye/raft/persister"
	"github.com/tobiabidoye/raft/raftapi"
)

var ProgramStart = time.Now()

type Role string

const (
	LEADER    Role = "leader"
	FOLLOWER  Role = "follower"
	CANDIDATE Role = "candidate"
)

type LogValue struct {
	Term int
	//interface which implements zero methods, empty interface, esssentially a template
	Item any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex    // Lock to protect shared access to this peer's state
	peers     []*rpc.Client // RPC end points of all peers
	ports     []string
	persister *persister.DiskPersister // Object to hold this peer's persisted state
	me        int                      // this peer's index into peers[]
	connMu    sync.Mutex
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastRpcContact  time.Time
	allowedDuration time.Duration
	currentTerm     int
	//which index you voted for in peers array
	votedFor    int
	currentRole Role
	//not sure what log stores yet
	log         []LogValue
	commitIndex int
	lastApplied int
	//next index to send to each peer
	nextIndex []int
	//index of highest match to send to each peer
	matchIndex        []int
	lastIncludedIndex int
	lastIncludedTerm  int
	curSnapshot       []byte
	applyCh           chan raftapi.ApplyMsg
	signalAE          chan struct{}
}

func (rf *Raft) StartRpcServer(port string) error {

	server := rpc.NewServer()
	if err := server.Register(rf); err != nil {
		return err
	}

	l, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	return nil
}

func (rf *Raft) GetLastIncludedIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	//
	if rf.currentRole == LEADER {
		isleader = true
	}

	term = rf.currentTerm
	return term, isleader
}

func (rf *Raft) SendRpc(server int, args any, method string, reply any) bool {
	client, err := rf.GetPeerConn(server)
	if err != nil {
		return false
	}

	err = client.Call(method, args, reply)
	if err != nil {
		rf.connMu.Lock()
		if rf.peers[server] == client {
			client.Close()
			rf.peers[server] = nil
		}
		rf.connMu.Unlock()
		return false
	}
	return true
}

func (rf *Raft) GetPeerConn(server int) (*rpc.Client, error) {
	if server < 0 || server >= len(rf.ports) {
		return nil, fmt.Errorf("peer index %d out of bounds (max %d)", server, len(rf.ports)-1)
	}

	if server == rf.me {
		return nil, fmt.Errorf("node %d cannot dial itself", rf.me)
	}
	rf.connMu.Lock()
	defer rf.connMu.Unlock()
	if rf.peers[server] != nil {
		return rf.peers[server], nil
	}
	port := rf.ports[server]

	//dial that specific port
	//dial with timeout
	conn, err := net.DialTimeout("tcp", port, 200*time.Millisecond)
	if err != nil {
		return nil, err
	}

	newClient := rpc.NewClient(conn)
	rf.peers[server] = newClient
	return newClient, nil
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.log)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.lastIncludedIndex)
	enc.Encode(rf.lastIncludedTerm)
	raftState := buf.Bytes()
	rf.persister.Save(raftState, rf.curSnapshot)
	DPrintf(dInfo, "persisted raft state size: %d bytes, log len: %d, term: %d", rf.persister.RaftStateSize(), len(rf.log), rf.currentTerm)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var oldLog []LogValue
	var oldTerm int
	var votedFor int
	var lastIncludedInd int
	var lastIncTerm int
	if dec.Decode(&oldLog) != nil || dec.Decode(&oldTerm) != nil || dec.Decode(&votedFor) != nil || dec.Decode(&lastIncludedInd) != nil || dec.Decode(&lastIncTerm) != nil {
		//probably add a log here about what can and cannot be decoded
		return
	} else {
		rf.log = oldLog
		rf.currentTerm = oldTerm
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedInd
		rf.lastIncludedTerm = lastIncTerm
		DPrintf(dInfo, "S%d restored log len=%d", rf.me, len(rf.log))
	}

}

func (rf *Raft) ConvertLogicalToPhysical(logicalIndex int) int {
	return logicalIndex - rf.lastIncludedIndex
}

func (rf *Raft) GetTermFromLogicalIndex(logicalIndex int) int {
	if logicalIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	curInd := logicalIndex - rf.lastIncludedIndex
	return rf.log[curInd].Term
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	DPrintf(dInfo, "log size prior to trimming %d", len(rf.log))
	physicalInd := rf.ConvertLogicalToPhysical(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[physicalInd].Term
	//trim everything up until index
	// rf.log = rf.log[]
	tempLog := make([]LogValue, 0)
	tempLog = append(tempLog, LogValue{Item: "dummy", Term: rf.lastIncludedTerm})
	tempLog = append(tempLog, rf.log[physicalInd+1:]...)
	rf.log = tempLog
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)
	rf.curSnapshot = snapshot
	rf.persist()
	DPrintf(dInfo, "log size after trimming %d", len(rf.log))
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	//term for a lagging candidate to update itself
	Term        int
	VoteGranted bool
}

// append entries rpc struct
type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogValue
	LeaderCommit int
}

//append entries rpc response struct

type AppendEntriesResponse struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// rpc handler
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	return rf.SendRpc(server, args, "Raft.AppendEntries", reply)
	/* err := rf.peers[server].Call("Raft.AppendEntries", args, reply) */
}

// rpc handler helper
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) error {
	//now for vote requests
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// curLogTerm := rf.currentTerm
	DPrintf(dLog2, "S%d <- S%d AE T%d prevIdx=%d prevTerm=%d nEntries=%d", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	//dont append entry from stale leader
	if rf.currentTerm > args.Term {
		DPrintf(dDrop, "S%d rejected AE from S%d: stale term", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	prevInd := rf.ConvertLogicalToPhysical(args.PrevLogIndex)

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.currentRole = FOLLOWER
		//demote role
		DPrintf(dTerm, "S%d demoted to FOLLOWER by AE from S%d at T%d", rf.me, args.LeaderId, args.Term)
	}

	//stale leader sending out of date indices
	// compare logical to logical for clarity since you can get negative items which works but isnt ideal
	if args.PrevLogIndex < rf.lastIncludedIndex {
		DPrintf(dDrop, "S%d rejected AE from S%d: stale previous index", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	//send back for decrementing
	if prevInd >= len(rf.log) {
		DPrintf(dDrop, "S%d rejected AE from S%d: sent prevLogIndex is greater than length of log", rf.me, args.LeaderId)
		rf.lastRpcContact = time.Now()
		durationInt := rand.Intn(800-400) + 400
		rf.allowedDuration = time.Duration(durationInt) * time.Millisecond
		reply.Term = rf.currentTerm
		reply.Success = false
		//apply the optimization
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = (len(rf.log)) + rf.lastIncludedIndex
		return nil
	}

	//divergence in logs
	//physical index should be now within the same range of this followers log
	if prevInd > 0 && args.PrevLogTerm != rf.log[prevInd].Term {
		//maybe ill write a helper function or not if im not feeling lazy
		DPrintf(dDrop, "S%d rejected AE from S%d: prevLogIndex does not match", rf.me, args.LeaderId)
		rf.lastRpcContact = time.Now()
		durationInt := rand.Intn(800-400) + 400
		rf.allowedDuration = time.Duration(durationInt) * time.Millisecond
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = rf.log[prevInd].Term
		//index of first entry with that term
		// should return logical index which is calculated by adding last index to the physical index
		reply.XIndex = rf.GetTermFirstEntry(rf.log[prevInd].Term)

		reply.XLen = (len(rf.log)) + rf.lastIncludedIndex
		return nil
	}

	for i := 0; i < len(args.Entries); i++ {
		//actual log index for the local log
		logIdx := prevInd + 1 + i
		//at first logidx will be equal to then eventually greater than
		if logIdx >= len(rf.log) {
			// append the entire log instead of one at a time
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		} else if args.Entries[i].Term != rf.log[logIdx].Term {
			//then truncate log
			rf.log = rf.log[:logIdx]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}

	}
	//reset leader election time out
	rf.lastRpcContact = time.Now()
	durationInt := rand.Intn(800-400) + 400
	rf.allowedDuration = time.Duration(durationInt) * time.Millisecond
	if args.LeaderCommit > rf.commitIndex {
		//only ever advance monotonically
		//this is so follower does not say it has logs it does not have or say it has committed indices which conflict with leader
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1+rf.lastIncludedIndex)
	}

	//successful append entries and inform leader of followers term
	rf.persist()
	reply.Success = true
	reply.Term = rf.currentTerm
	DPrintf(dLog2, "S%d AE success prevIdx=%d logLen=%d commitIndex=%d", rf.me, args.PrevLogIndex, len(rf.log), rf.commitIndex)
	return nil
}

func (rf *Raft) GetTermFirstEntry(term int) int {
	for i := 1; i < len(rf.log); i++ {
		if rf.log[i].Term == term {
			//return logical index
			return i + rf.lastIncludedIndex
		}
	}
	//return -1 if no item in that term exists
	return -1
}

func (rf *Raft) AppendEntriesRoutine() {
	//send rpc every 10 milliseconds
	for {
		/* sleepTime := time.Duration(100) */
		//only send rpc if leader
		/* rf.mu.Unlock() */
		select {
		case <-rf.signalAE:
		//dont hold lock and sleep for 100ms
		case <-time.After(100 * time.Millisecond):
		}
		rf.mu.Lock()
		if rf.currentRole == LEADER {
			/* sleepTime = time.Duration(10) */
			savedTerm := rf.currentTerm
			for ind, _ := range rf.peers {
				//send an append entries request to each peer
				if ind == rf.me {
					continue
				}
				DPrintf(dInfo, "S%d checking peer %d nextIndex=%d lastIncludedIndex=%d", rf.me, ind, rf.nextIndex[ind], rf.lastIncludedIndex)
				if rf.nextIndex[ind] <= rf.lastIncludedIndex {
					installSnapshotArgs := InstallSnapshotRequest{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Data:              rf.curSnapshot,
						Done:              true,
					}

					installSnapshotResp := InstallSnapshotResponse{}
					go func(curIndex int, snapArgs InstallSnapshotRequest, snapResp InstallSnapshotResponse) {
						ok := rf.SendInstallSnapshot(curIndex, &snapArgs, &snapResp)
						DPrintf(dInfo, "S%d InstallSnapshot RPC to S%d ok=%v", rf.me, curIndex, ok)
						if !ok {
							return
						}
						rf.mu.Lock()
						defer rf.mu.Unlock()

						if rf.currentRole != LEADER {

							DPrintf(dInfo, "leader no longer leader world changed after install snapshot leader: %d", rf.me)
							return
						}

						if savedTerm != rf.currentTerm {
							DPrintf(dInfo, "saved term is not equal to leader current term saved term: %d, current term: %d", savedTerm, rf.currentTerm)
							return
						}
						//step down
						if snapResp.Term > rf.currentTerm {
							rf.currentRole = FOLLOWER
							DPrintf(dInfo, "leader stale compared to follower after installsnapshot rpc response term: %d, leaderTerm: %d", snapResp.Term, rf.currentTerm)
							rf.currentTerm = snapResp.Term
							rf.votedFor = -1
							rf.persist()
							return
						}

						//on success update
						DPrintf(dInfo, "leader sent successful installsnapshotrpc, leaderid %d, followerid %d", rf.me, curIndex)

						rf.nextIndex[curIndex] = snapArgs.LastIncludedIndex + 1
						rf.matchIndex[curIndex] = snapArgs.LastIncludedIndex
					}(ind, installSnapshotArgs, installSnapshotResp)
					continue
				}
				//check if world has changed

				//append real entries
				realEntries := []LogValue{}
				physicalLowerBound := rf.ConvertLogicalToPhysical(rf.nextIndex[ind])
				for i := physicalLowerBound; i < len(rf.log); i++ {
					realEntries = append(realEntries, rf.log[i])
				}
				//previous log index of specific peer
				prevLogIndex := rf.nextIndex[ind] - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 {
					prevLogTerm = rf.log[rf.ConvertLogicalToPhysical(prevLogIndex)].Term
				}

				leaderCommit := rf.commitIndex

				appendEntriesReq := AppendEntriesRequest{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: leaderCommit,
					Entries:      realEntries,
				}
				//then after
				appendEntriesResp := AppendEntriesResponse{}
				//unlock prior to rpc
				go func(serverId int, args AppendEntriesRequest, resp AppendEntriesResponse) {
					//dont lock before rpc call
					//loop for decrementing index
					//send in loop and decremen
					ok := rf.SendAppendEntries(serverId, &args, &resp)
					rf.mu.Lock()
					//only case we have to not worry about success
					defer rf.mu.Unlock()

					if rf.currentRole != LEADER {
						return
					}

					if savedTerm != rf.currentTerm {
						return
					}

					if !ok {
						//if rpc call never received response
						return
					}
					if resp.Term > rf.currentTerm {
						rf.currentRole = FOLLOWER
						rf.currentTerm = resp.Term
						rf.votedFor = -1
						rf.persist()
						return
					}

					if resp.Success {
						rf.matchIndex[serverId] = max(rf.matchIndex[serverId], args.PrevLogIndex+len(args.Entries))
						rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1

						//update match index
						rf.matchIndex[rf.me] = rf.lastIncludedIndex + (len(rf.log) - 1)
						copyMatchIndices := make([]int, len(rf.matchIndex))
						copy(copyMatchIndices, rf.matchIndex)
						slices.Sort(copyMatchIndices)
						//calculate median
						medianIndex := (len(copyMatchIndices) - 1) / 2
						toCommit := copyMatchIndices[medianIndex]
						if toCommit <= rf.lastIncludedIndex {
							return
						}
						if rf.log[rf.ConvertLogicalToPhysical(toCommit)].Term == rf.currentTerm && rf.commitIndex < toCommit {
							rf.commitIndex = toCommit
						}

						return
					}

					//if none of these cases have matched we know we can decrement the nextIndex
					if !resp.Success {
						//now for the optimization
						if resp.XTerm == -1 {
							//logical space since i converted this in append entries handler
							rf.nextIndex[ind] = resp.XLen
						} else if rf.GetTermFirstEntry(resp.XTerm) == -1 {
							//works since xindex is also converted to logical space unless i am wrong
							if resp.XIndex != 0 {
								rf.nextIndex[ind] = resp.XIndex
							} else {
								rf.nextIndex[ind] = 1
							}
						} else if rf.GetTermFirstEntry(resp.XTerm) != -1 {
							term := resp.XTerm
							tempInd := -1
							for curInd, val := range rf.log {
								//keep switching until last term
								if curInd == 0 {
									continue
								}

								if val.Term == term {
									tempInd = curInd
								}
							}

							if tempInd != -1 {
								rf.nextIndex[ind] = rf.lastIncludedIndex + tempInd + 1
							} else {
								//also logical index i think
								rf.nextIndex[ind] = resp.XIndex
							}
						}
						return
					}
					//dont update term

				}(ind, appendEntriesReq, appendEntriesResp)

			}
			//unlock prior to goroutines scheduled
			rf.mu.Unlock()
		} else {
			//unlock if not leader
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	curLogTerm := 0
	reply.Term = rf.currentTerm
	DPrintf(dVote, "S%d <- S%d RV req T%d lastIdx=%d lastTerm=%d", rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf(dDrop, "S%d denied vote to S%d: stale term (their T%d < my T%d)", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return nil
	} else if args.Term > rf.currentTerm {
		rf.currentRole = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		DPrintf(dDrop, "S%d denied vote to S%d: already voted for S%d", rf.me, args.CandidateId, rf.votedFor)
		return nil
	}

	//accounting for the dummy entry
	if len(rf.log) > 1 {
		curLogTerm = rf.log[len(rf.log)-1].Term
	} else {
		curLogTerm = rf.lastIncludedTerm
	}

	//term check here
	if args.LastLogTerm < curLogTerm {
		reply.VoteGranted = false
		DPrintf(dDrop, "S%d denied vote to S%d: log not up-to-date", rf.me, args.CandidateId)
		return nil
	}

	//index check here
	if args.LastLogTerm == curLogTerm && args.LastLogIndex < rf.lastIncludedIndex+len(rf.log)-1 {
		//length of log is wrong
		DPrintf(dDrop, "S%d denied vote to S%d: log not up-to-date", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return nil
	}
	//now we know log is at least as up to date
	//we can grant vote here

	reply.VoteGranted = true
	DPrintf(dVote, "S%d granted vote to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
	rf.votedFor = args.CandidateId
	rf.lastRpcContact = time.Now()
	durationInt := rand.Intn(800-400) + 400
	//reset random duration to be between 300 and 500 milliseconds, likely have to change it but forgot what values mit specified
	rf.allowedDuration = time.Duration(durationInt) * time.Millisecond
	rf.currentRole = FOLLOWER
	rf.persist()

	return nil
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//ok so this is blocking
	return rf.SendRpc(server, args, "Raft.RequestVote", reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := true
	if rf.currentRole != LEADER {
		isLeader = false
		return -1, -1, isLeader
	}

	// Your code here (3B).
	index := rf.lastIncludedIndex + len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogValue{Term: rf.currentTerm, Item: command})
	rf.persist()
	//if leader
	select {
	//signal leader to wake up
	case rf.signalAE <- struct{}{}:
	default:
	}
	return index, term, isLeader
}

func (rf *Raft) ticker() {
	DPrintf(dInfo, "S%d ticker started", rf.me)
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		//lock to proect shared state
		rf.mu.Lock()
		elapsedTime := time.Since(rf.lastRpcContact)

		if rf.currentRole != LEADER && elapsedTime > rf.allowedDuration {
			//then send list of append entries rpcs to peers
			// vote for yourself

			DPrintf(dTimer, "S%d election timeout, becoming candidate at T%d, time passed: %s, allowed duration: %s, num goroutines: %d", rf.me, rf.currentTerm, elapsedTime, rf.allowedDuration, runtime.NumGoroutine())
			rf.votedFor = rf.me
			rf.currentTerm += 1
			rf.currentRole = CANDIDATE
			rf.persist()
			//reset allowed duration for current term
			durationInt := rand.Intn(800-400) + 400
			rf.allowedDuration = time.Millisecond * time.Duration(durationInt)
			rf.lastRpcContact = time.Now()
			lastTerm := 0

			// if len(rf.log) > 0 {
			// 	lastTerm = rf.log[len(rf.log)-1].Term
			// }
			//
			if len(rf.log) > 1 {
				lastTerm = rf.log[len(rf.log)-1].Term
			} else {
				lastTerm = rf.lastIncludedTerm
			}

			curVoteReq := RequestVoteArgs{Term: rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.lastIncludedIndex + len(rf.log) - 1,
				LastLogTerm:  lastTerm,
			}

			//send the rpcs non blocking, never hold lock before sending an rpc
			numVotes := 1
			curTerm := rf.currentTerm
			//unlock before rpc call
			rf.mu.Unlock()
			for ind, _ := range rf.peers {

				if ind == rf.me {
					continue
				}

				curVoteReply := RequestVoteReply{}
				go func(peerNum int, voteReply RequestVoteReply, voteReq RequestVoteArgs) {
					//do this synchronously
					rf.sendRequestVote(peerNum, &voteReq, &voteReply)
					rf.mu.Lock()

					if voteReply.Term > rf.currentTerm {
						DPrintf(dTerm, "S%d stepping down (RV reply T%d > my T%d)", rf.me, voteReply.Term, rf.currentTerm)
						rf.currentRole = FOLLOWER
						rf.currentTerm = voteReply.Term
						rf.votedFor = -1
						rf.persist()
						rf.mu.Unlock()
						return
					}

					//check if world has changed
					if rf.currentTerm != curTerm || rf.currentRole != CANDIDATE {
						DPrintf(dTrace, "S%d stepped down world has changed and no longer leader", rf.me)
						rf.mu.Unlock()
						return
					}

					//if everything cool then check if you got vote
					if voteReply.VoteGranted {
						DPrintf(dVote, "S%d got RV reply from S%d: granted=%v term=%d", rf.me, peerNum, voteReply.VoteGranted, voteReply.Term)
						numVotes += 1
					}

					if rf.currentRole == LEADER {
						rf.mu.Unlock()
						return
					}
					if (numVotes) >= (len(rf.peers)/2)+1 {
						//you have majority at this point become leader
						DPrintf(dLeader, "S%d became leader at T%d with %d votes", rf.me, rf.currentTerm, numVotes)
						rf.currentRole = LEADER
						//initialize match indices to be zero
						rf.lastRpcContact = time.Now()

						for i := 0; i < len(rf.matchIndex); i++ {

							if i == rf.me {
								rf.matchIndex[i] = rf.lastIncludedIndex + len(rf.log) - 1
							} else {
								rf.matchIndex[i] = 0
							}

							rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.log)
						}

						rf.persist()
						select {
						//signal leader to send append entries
						case rf.signalAE <- struct{}{}:
						default:
						}

						rf.mu.Unlock()

						return
					}
					//lock when exiting a function like so
					rf.mu.Unlock()
				}(ind, curVoteReply, curVoteReq)

			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) Apply(applyCh chan raftapi.ApplyMsg) {
	for {
		rf.mu.Lock()

		if rf.lastApplied < rf.lastIncludedIndex {
			//we can apply the snapshot
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.curSnapshot,
				SnapshotIndex: rf.lastIncludedIndex,
				SnapshotTerm:  rf.lastIncludedTerm,
			}
			//update highest applied index
			rf.lastApplied = rf.lastIncludedIndex
			//unlock so that the whole system does not freeze due to just one channel send
			DPrintf(dInfo, "Apply sending snapshot lastApplied=%d lastIncludedIndex=%d", rf.lastApplied, rf.lastIncludedIndex)
			rf.mu.Unlock()
			applyCh <- msg
		} else if rf.commitIndex > rf.lastApplied {
			//gather into local slice
			// copy to avoid race condition
			logicalStart := rf.lastApplied + 1
			start := rf.ConvertLogicalToPhysical(rf.lastApplied + 1)
			end := rf.ConvertLogicalToPhysical(rf.commitIndex)
			toApply := make([]LogValue, end-start+1)
			copy(toApply, rf.log[start:end+1])
			//loop over messages to apply
			prevCommitIndex := rf.commitIndex
			rf.lastApplied = prevCommitIndex
			rf.mu.Unlock()
			for ind, val := range toApply {
				msg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      val.Item,
					CommandIndex: logicalStart + ind,
				}

				//send message to be applied
				applyCh <- msg
				//advance the lastapplied index
			}
		} else {
			rf.mu.Unlock()
		}
		//sleep for 10 milliseconds
		time.Sleep(time.Millisecond * 10)
	}
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotResponse struct {
	Term int
}

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotRequest, reply *InstallSnapshotResponse) error {

	rf.mu.Lock()
	DPrintf(dInfo, "S%d InstallSnapshotHandler called from S%d", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		DPrintf(dError, "Leader term is not up to date or leaders last index not up to date me: %d, currentTerm: %d, leaderTerm %d, leaderLastIndex: %d, mylastIndex %d", rf.me, rf.currentTerm, args.Term, args.LastIncludedIndex, rf.lastIncludedIndex)
		rf.mu.Unlock()
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.currentRole = FOLLOWER
		rf.persist()
	}

	rf.currentRole = FOLLOWER
	rf.lastRpcContact = time.Now()
	durationInt := rand.Intn(800-400) + 400
	rf.allowedDuration = time.Duration(durationInt) * time.Millisecond

	rf.curSnapshot = args.Data
	//now loop through log
	logToCopy := make([]LogValue, 0)
	logToCopy = append(logToCopy, LogValue{Term: args.LastIncludedTerm, Item: "dummy"})

	for ind, val := range rf.log {
		//compare logical indices directly
		if val.Term == args.LastIncludedTerm && args.LastIncludedIndex == rf.lastIncludedIndex+ind {
			//copy all items 1 past the matching last index
			logToCopy = append(logToCopy, rf.log[ind+1:]...)
			break
		}
	}

	//now set the old log to equal this log either it zeroes out or it gets the actual valid remaining log entries
	rf.log = logToCopy
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	/* rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex) */
	//not sure how to update commit index before resetting state machine
	//now reset state machine
	/* msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	} */
	rf.persist()
	//unlock so items waiting on the lock dont deadlock due to a unusually long channel send
	rf.mu.Unlock()

	//update highest applied index
	//unlock so that the whole system does not freeze due to just one channel send
	/* rf.applyCh <- msg */
	return nil
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
	return rf.SendRpc(server, args, "Raft.InstallSnapshotHandler", reply)
}

// the service or tester wants to create a Raft server. the ports
//
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(ports []string, me int,
	persister *persister.DiskPersister, applyCh chan raftapi.ApplyMsg, port string) raftapi.Raft {
	rf := &Raft{}
	rf.ports = ports
	rf.persister = persister
	rf.me = me
	rf.lastRpcContact = time.Now()
	durationInt := rand.Intn(800-400) + 400
	rf.allowedDuration = time.Duration(durationInt) * time.Millisecond
	rf.currentTerm = 0
	rf.currentRole = FOLLOWER
	rf.votedFor = -1
	rf.log = make([]LogValue, 0)
	rf.signalAE = make(chan struct{}, 1)
	//append dummy value to the log 0th index
	rf.log = append(rf.log, LogValue{Term: -1, Item: "dummy"})
	rf.applyCh = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.ports = ports
	rf.peers = make([]*rpc.Client, len(ports))

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	DPrintf(dInfo, "S%d started at T%d", rf.me, rf.currentTerm)
	// Your initialization code here (3A, 3B, 3C).

	if err := rf.StartRpcServer(port); err != nil {
		log.Fatalf("Failed to start Raft RPC listener on port %s: %v", port, err)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.curSnapshot = persister.ReadSnapshot()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.AppendEntriesRoutine()
	go rf.Apply(applyCh)
	return rf
}
