package raft

import (
	//	"bytes"
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"net/rpc"
	"runtime"
	"slices"
	"sync"

	"time"

	"github.com/tobiabidoye/distributed-raft/persister"
	"github.com/tobiabidoye/distributed-raft/raftapi"
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
	Item any
}

type Raft struct {
	mu              sync.Mutex    // Lock to protect shared access to this peer's state
	peers           []*rpc.Client // RPC end points of all peers
	ports           []string
	persister       *persister.DiskPersister // Object to hold this peer's persisted state
	me              int                      // this peer's index into peers[]
	connMu          sync.Mutex
	lastRpcContact  time.Time
	allowedDuration time.Duration
	currentTerm     int
	//which index you voted for in peers array
	votedFor    int
	currentRole Role
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
