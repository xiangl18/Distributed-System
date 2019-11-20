package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state             int
	currentTerm       int
	votedFor          int
	requestVoteChan   chan bool
	appendEntriesChan chan bool
	commitChan        chan bool
	leaderElectdChan  chan bool
	applyCh           chan ApplyMsg
	heatbeatTimeout   time.Duration
	electionTimeout   time.Duration
	timer             *time.Timer
	log               []LogEntry
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
}

// structure for log entry, with term and command
type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// state for each server
const (
	FOLLOWER     = 0
	CANDIDATE    = -1
	LEADER       = 1
	HEATBEATTIME = 100
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.mu.Unlock()
}

func (rf *Raft) readSnapshot(data []byte) {
	rf.readPersist(rf.persister.ReadRaftState())
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var LastIncludedIndex int
	var LastIncludedTerm int
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)
	applyMsg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	go func() {
		rf.applyCh <- applyMsg
	}()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	offset := rf.log[0].Index
	lastIndex := rf.getLastLogIndex()
	if index <= offset || index > lastIndex {
		return
	}
	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: rf.log[index-offset].Term})
	for i := index + 1; i <= lastIndex; i++ {
		newLogEntries = append(newLogEntries, rf.log[i-offset])
	}
	rf.log = newLogEntries
	rf.persist()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(newLogEntries[0].Index)
	e.Encode(newLogEntries[0].Term)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)
}

func truncateLog(lastIncludedIndex int, lastIncludedTerm int, log []LogEntry) []LogEntry {
	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})
	for index := len(log) - 1; index >= 0; index-- {
		if log[index].Index == lastIncludedIndex && log[index].Term == lastIncludedTerm {
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}
	return newLogEntries
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	go func() {
		rf.appendEntriesChan <- true
	}()
	rf.state = FOLLOWER
	rf.currentTerm = rf.currentTerm
	rf.persister.SaveSnapshot(args.Data)
	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)
	applyMsg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()
	rf.applyCh <- applyMsg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			return ok
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	switch {
	case args.Term < rf.currentTerm:
		// Reply false if term < currentTerm
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	case args.Term >= rf.currentTerm:
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
			((args.LastLogTerm == rf.getLastLogTerm() &&
				args.LastLogIndex >= rf.getLastLogIndex()) ||
				args.LastLogTerm > rf.getLastLogTerm()) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.state = FOLLOWER
			go func() {
				rf.requestVoteChan <- true
			}()
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("Server %v -> Server %v, RequestVoteArgs", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("Server %v <- Server %v, RequestVoteReply, ok?:%v", rf.me, server, ok)
	return ok
}

// structure for AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// structure for AppendEntries RPC reply
type AppendEntriesReply struct {
	Term      int
	Success   bool
	ConflictEntryIndex int
}

// function for AppendEngtries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.ConflictEntryIndex = rf.getLastLogIndex() + 1
		return
	}
	go func() {
		rf.appendEntriesChan <- true

	}()
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.ConflictEntryIndex = rf.getLastLogIndex() + 1
		return
	}
	// find and return conflict entry index to leader
	offset := rf.log[0].Index
	if args.PrevLogIndex > offset {
		term := rf.log[args.PrevLogIndex-offset].Term
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= offset; i-- {
				if rf.log[i-offset].Term != term {
					reply.ConflictEntryIndex = i + 1
					break
				}
			}
			return
		}
	}
	// append entries
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it:
	if args.PrevLogIndex >= offset {
		rf.log = rf.log[:args.PrevLogIndex+1-offset]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.ConflictEntryIndex = rf.getLastLogIndex() + 1
	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastLogIndex()
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitChan <- true
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Server %v -> Server %v, AppendEntriesArgs", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("Server %v <- Server %v, AppendEntriesReply, ok?:%v", rf.me, server, ok)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry
			rf.nextIndex[server] = reply.ConflictEntryIndex
		}
	}
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := -1
	term := rf.currentTerm
	// Your code here (2B).
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.getLastLogIndex() + 1
		logEntry := LogEntry{term, command, index}
		rf.log = append(rf.log, logEntry)
		rf.persist()
	}
	rf.mu.Unlock()
	return index, term, isLeader

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("kill Server %v", rf.me)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.applyCh = applyCh

	rf.requestVoteChan = make(chan bool)
	rf.appendEntriesChan = make(chan bool)
	rf.leaderElectdChan = make(chan bool)
	rf.commitChan = make(chan bool)
	rf.electionTimeout = time.Duration(HEATBEATTIME*10+rand.Intn(HEATBEATTIME)) * time.Millisecond
	rf.heatbeatTimeout = time.Duration(HEATBEATTIME) * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	// state machine for each server
	go func() {
		for {
			rf.timer = time.NewTimer(rf.electionTimeout)
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch state {
			case FOLLOWER:
				select {
				case <-rf.appendEntriesChan:
					// reset counting when there is a leader
					rf.timer.Reset(rf.electionTimeout)
				case <-rf.requestVoteChan:
					// reset counting when there is an election
					rf.timer.Reset(rf.electionTimeout)
				case <-rf.timer.C:
					// become candidate when times out
					rf.mu.Lock()
					rf.state = CANDIDATE
					rf.currentTerm += 1
					rf.votedFor = rf.me
					go rf.startElection()
					rf.mu.Unlock()
				}
			case CANDIDATE:
				select {
				case <-rf.timer.C:
					//start new election when times out
					rf.mu.Lock()
					rf.timer.Reset(rf.electionTimeout)
					rf.state = CANDIDATE
					rf.currentTerm += 1
					rf.votedFor = rf.me
					go rf.startElection()
					rf.mu.Unlock()
				case <-rf.appendEntriesChan:
					rf.state = FOLLOWER
				case <-rf.requestVoteChan:
				case <-rf.leaderElectdChan:
					// become leader when recieve majority votes
				}
			case LEADER:
				// send heartbeat
				DPrintf("Server %v -> leader", rf.me)
				rf.startHeartbeat()
				time.Sleep(rf.heatbeatTimeout)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.commitChan:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				offset := rf.log[0].Index
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i - offset].Command}
					applyCh <- msg

					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	totalVote := 1
	args := RequestVoteArgs{rf.currentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm()}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			server := i
			args := args
			go func() {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					rf.mu.Lock()
					// If RPC request or response contains term T > currentTerm:
					// set currentTerm = T, convert to follower
					if reply.Term > rf.currentTerm {
						rf.state = FOLLOWER
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}
					if reply.VoteGranted {
						totalVote += 1
					}
					// receives more than a half of votes. becomes leader
					if totalVote > len(rf.peers)/2 {
						DPrintf("server %v --> leader\n", rf.me)
						if rf.state == CANDIDATE {
							rf.state = LEADER
							// nextIndex, matchIndex are reset.
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.getLastLogIndex() + 1
								rf.matchIndex[i] = 0
							}
						}
						go func() {
							rf.leaderElectdChan <- true
						}()
					}
					rf.mu.Unlock()
				}
			}()
		}
	}
}

func (rf *Raft) startHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	N := rf.commitIndex
	last := rf.getLastLogIndex()
	offset := rf.log[0].Index
//  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)	
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i - offset].Term == rf.currentTerm {
				num++
			}
		}
		if 2 * num > len(rf.peers) {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.commitChan <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			if rf.nextIndex[i] > offset {
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := rf.log[prevLogIndex - offset].Term
				var entries []LogEntry
				entries = rf.log[prevLogIndex + 1 - offset:]
				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					entries,
					rf.commitIndex}
				go func(i int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					rf.sendAppendEntries(i, &args, &reply)
				}(i, args)
			} else {
				lastIncludedIndex := rf.log[0].Index
				lastIncludedTerm := rf.log[0].Term
				data := rf.persister.snapshot
				args := InstallSnapshotArgs{
					rf.currentTerm,
					rf.me,
					lastIncludedIndex,
					lastIncludedTerm,
					data}
				go func(server int, args InstallSnapshotArgs) {
					reply := InstallSnapshotReply{}
					rf.sendInstallSnapshot(server, &args, &reply)
				}(i, args)
			}
		}
	}
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log) - 1].Index
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}