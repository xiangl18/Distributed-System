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
	"sort"
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
	applyCh           chan ApplyMsg
	heatbeatTimeout   time.Duration
	electionTimeout   time.Duration
	totalVote         int
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
		// DPrintf("server %v not vote for server %v: T < currentTerm\n", rf.me, args.CandidateID)
	case rf.votedFor == -1 || rf.votedFor == args.CandidateID:
		// convert to follower when request contains term T > currentTerm
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		if args.LastLogTerm > rf.getLastLogTerm() ||
			(args.LastLogTerm == rf.getLastLogTerm() &&
				args.LastLogIndex >= rf.getLastLogIndex()) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			go func() {
				rf.requestVoteChan <- true
			}()
		}
		DPrintf("server %v --> FOLLOWER: T > currentTerm\n", rf.me)
		// fmt.Printf("server%v becomes follower\n", rf.me)
	default:
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	}

	DPrintf("server %d reply RequestVote to server %d: term %d granted? %t\n",
		rf.me, args.CandidateID, reply.Term, reply.VoteGranted)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("server %d send RequestVote to server %d ok?: %t\n", args.CandidateID, server, ok)
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
	Term          int
	Success       bool
	ConflictIndex int "the first index it stores for that conflict term"
	ConflictTerm  int "the term of the conflicting entry"
}

// function for AppendEngtries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	switch {
	case args.Term < rf.currentTerm:
		// Reply false if term < currentTerm
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		// DPrintf("server %v not reply to current leader %v: T < currentTerm\n",
		// 	rf.me, args.LeaderID)
	case args.Term >= rf.currentTerm:
		// convert to follower when request contains term T > currentTerm:
		rf.state = FOLLOWER
		rf.votedFor = -1
		reply.Term = args.Term
		rf.currentTerm = args.Term
		if args.PrevLogIndex > rf.getLastLogIndex() {
			reply.Success = false
			reply.ConflictIndex = rf.getLastLogIndex() + 1
			break
		} else if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			reply.Success = false
			i := args.PrevLogIndex
			for ; rf.log[i].Term == rf.log[args.PrevLogIndex].Term; i-- {
			}
			reply.ConflictIndex = i + 1
			break
		}
		diff := -1
		if rf.getLastLogIndex() < args.PrevLogIndex+len(args.Entries) {
			diff = args.PrevLogIndex + 1
		} else {
			for i := 0; i < len(args.Entries); i++ {
				if rf.log[i+args.PrevLogIndex+1].Term != args.Entries[i].Term {
					diff = i + args.PrevLogIndex + 1
					break
				}
			}
		}
		if diff != -1 {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		}
		//  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
		}
		rf.applyLogs()
		reply.Success = true
		// DPrintf("server %d --> FOLLOWER: T > currentTerm", rf.me)
	}

	DPrintf("server %d reply AppendEntries to server %d: term %d success? %t\n",
		rf.me, args.LeaderID, reply.Term, reply.Success)
	go func() {
		rf.appendEntriesChan <- true
	}()
	rf.persist()
	rf.mu.Unlock()

}

// function for send AppendEngtries RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("server %d send AppendEntries to server %d ok?: %t\n", args.LeaderID, server, ok)
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
		logEntry := LogEntry{term, command}
		rf.log = append(rf.log, logEntry)
		rf.persist()
		rf.startHeartbeat()
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
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1 //-1 indicates that there is not voting happened right now
	rf.state = FOLLOWER
	rf.requestVoteChan = make(chan bool)
	rf.appendEntriesChan = make(chan bool)
	rf.heatbeatTimeout = time.Duration(HEATBEATTIME) * time.Millisecond
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.electionTimeout = time.Duration(HEATBEATTIME*10+rand.Intn(HEATBEATTIME)) * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {
		rf.timer = time.NewTimer(rf.electionTimeout)
		for {
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
					// reset counting when there is a election
					rf.timer.Reset(rf.electionTimeout)
				case <-rf.timer.C:
					// become candidate when times out
					rf.mu.Lock()
					rf.state = CANDIDATE
					rf.timer.Reset(rf.electionTimeout)
					rf.startElection()
					rf.mu.Unlock()
				}
			case CANDIDATE:
				rf.mu.Lock()
				select {
				case <-rf.appendEntriesChan:
				case <-rf.timer.C:
					//start new election when times out
					rf.timer.Reset(rf.electionTimeout)
					rf.startElection()
				default:
					// become leader when recieve majority votes
					if rf.totalVote > len(rf.peers)/2 {
						rf.state = LEADER
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers)) //initialized to 0
						for i := 0; i < len(rf.nextIndex); i++ {   //(initialized to leader last log index + 1)
							rf.nextIndex[i] = rf.getLastLogIndex() + 1
						}
						DPrintf("server %v --> leader\n", rf.me)
						// fmt.Printf("server %v --> leader\n", rf.me)
					}
				}
				rf.mu.Unlock()
			case LEADER:
				// send heartbeat
				rf.mu.Lock()
				rf.startHeartbeat()
				rf.mu.Unlock()
				time.Sleep(rf.heatbeatTimeout)

			}
		}
	}()
	return rf
}

func (rf *Raft) startElection() {
	rf.totalVote = 1
	rf.votedFor = rf.me
	rf.currentTerm += 1
	LastLogIndex := rf.getLastLogIndex()
	LastLogTerm := rf.getLastLogTerm()
	args := RequestVoteArgs{rf.currentTerm, rf.me, LastLogIndex, LastLogTerm}
	for i := range rf.peers {
		if i != rf.me {
			server := i
			go func() {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if ok {
					if reply.VoteGranted {
						rf.mu.Lock()
						rf.totalVote++
						rf.mu.Unlock()
					}
				}
			}()
		}
	}
}

func (rf *Raft) startHeartbeat() {
	for i := range rf.peers {
		if i != rf.me {
			server := i
			go func() {
				for {
					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						return
					}
					args := AppendEntriesArgs{rf.currentTerm, rf.me,
						rf.getPrevLogIndex(server),
						rf.getPrevLogTerm(server),
						append(make([]LogEntry, 0), rf.log[rf.nextIndex[server]:]...),
						rf.commitIndex,
					}
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					switch {
					case rf.state != LEADER || rf.currentTerm != args.Term:
						rf.mu.Unlock()
						return
					case reply.Term > rf.currentTerm:
						rf.state = FOLLOWER
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.mu.Unlock()
						return
					case reply.Success:
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						rf.updateCommitIndex()
						rf.mu.Unlock()
						return
					default:
						rf.nextIndex[server] = Max(1, reply.ConflictIndex)
						rf.mu.Unlock()
					}
				}
			}()
		}
	}

}

func (rf *Raft) getPrevLogIndex(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIndex := rf.getPrevLogIndex(i)
	return rf.log[prevLogIndex].Term
}

func (rf *Raft) getLastLogTerm() int {
	ret := -1
	if rf.getLastLogIndex() != 0 {
		ret = rf.log[rf.getLastLogIndex()].Term
	}
	return ret
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) updateCommitIndex() {
	matchIndexes := make([]int, len(rf.matchIndex))
	copy(matchIndexes, rf.matchIndex)
	matchIndexes[rf.me] = len(rf.log) - 1
	sort.Ints(matchIndexes)

	N := matchIndexes[len(rf.peers)/2]
	DPrintf("matchIndexes:%v, N:%v", matchIndexes, N)

	if rf.state == LEADER && N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		DPrintf("Server(%v) advanceCommitIndex (%v => %v)", rf.me, rf.commitIndex, N)
		rf.commitIndex = N
		rf.applyLogs()
	}
}

func (rf *Raft) applyLogs() {
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{
				Index:   i,
				Command: rf.log[i].Command,
			}
			DPrintf("Apply logs: commitIndex %v, lastapplied %v, command %v", i, rf.lastApplied, rf.log[i].Command)
			rf.applyCh <- msg
		}
	}
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
