package raft

import (
	"math/rand"
	"time"
)

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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {

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
	rf.persist()
	rf.mu.Unlock()

	return nil
}
