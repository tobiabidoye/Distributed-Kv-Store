package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"time"
)

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

func (rf *Raft) RegisterRpc(server *rpc.Server) error {
	return server.Register(rf)
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//ok so this is blocking
	return rf.SendRpc(server, args, "Raft.RequestVote", reply)
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
	return rf.SendRpc(server, args, "Raft.InstallSnapshotHandler", reply)
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	return rf.SendRpc(server, args, "Raft.AppendEntries", reply)
	/* err := rf.peers[server].Call("Raft.AppendEntries", args, reply) */
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
