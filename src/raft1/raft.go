package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const (
	heartbeatInterval  = 100 * time.Millisecond // tester allows <= 10 heartbeats/sec
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers (Figure 2)
	currentTerm int
	votedFor    int        // candidateId that received vote in currentTerm, or -1
	log         []LogEntry // log[0] is a dummy entry with term 0

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders, reinitialized after election
	nextIndex  []int
	matchIndex []int

	// volatile state for leader election
	role             Role
	electionDeadline time.Time // start an election if nothing heard from a leader/candidate by then
	lastHeartbeat    time.Time // leader only: when AppendEntries were last broadcast

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond // signaled when commitIndex advances
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	// election restriction (section 5.4.1): the candidate's log must be
	// at least as up-to-date as ours.
	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// only reset the timer when granting a vote, so that a peer
		// with a stale log can't keep others from timing out.
		rf.resetElectionTimer()
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// on rejection, lets the leader skip back over a whole
	// conflicting term instead of one entry per round trip.
	XTerm  int // term of the conflicting entry, or -1 if log too short
	XIndex int // index of the first entry with XTerm
	XLen   int // follower's log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}
	// a valid leader exists for this term; a candidate steps down.
	rf.role = Follower
	rf.resetElectionTimer()

	// consistency check: our log must contain an entry at
	// PrevLogIndex whose term matches PrevLogTerm.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		xIndex := args.PrevLogIndex
		for xIndex > 1 && rf.log[xIndex-1].Term == reply.XTerm {
			xIndex--
		}
		reply.XIndex = xIndex
		reply.XLen = len(rf.log)
		return
	}

	// append new entries, truncating only at the first real conflict.
	// a stale (reordered) RPC must not cut off entries we already
	// accepted from a later one.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx > rf.lastLogIndex() || rf.log[idx].Term != entry.Term {
			rf.log = append(rf.log[:idx], args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNew)
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// caller must hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = Follower
}

// caller must hold rf.mu.
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// caller must hold rf.mu.
func (rf *Raft) lastLogTerm() int {
	return rf.log[rf.lastLogIndex()].Term
}

// caller must hold rf.mu.
func (rf *Raft) resetElectionTimer() {
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionDeadline = time.Now().Add(electionTimeoutMin + time.Duration(rand.Int63n(spread)))
}

// caller must hold rf.mu.
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	votes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			// ignore stale replies from an earlier election
			if rf.role != Candidate || rf.currentTerm != args.Term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeader()
				}
			}
		}(i)
	}
}

// caller must hold rf.mu.
func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.broadcastAppendEntries()
}

// send AppendEntries to every follower. doubles as the heartbeat:
// followers that are caught up get an empty Entries.
// caller must hold rf.mu.
func (rf *Raft) broadcastAppendEntries() {
	rf.lastHeartbeat = time.Now()
	for i := range rf.peers {
		if i != rf.me {
			rf.replicateTo(i)
		}
	}
}

// caller must hold rf.mu.
func (rf *Raft) replicateTo(server int) {
	prevLogIndex := rf.nextIndex[server] - 1
	// copy, so later truncation of rf.log can't race with RPC encoding.
	entries := make([]LogEntry, len(rf.log[prevLogIndex+1:]))
	copy(entries, rf.log[prevLogIndex+1:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		// ignore replies to RPCs sent in an earlier term
		if rf.role != Leader || rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			// max() guards against reordered replies moving us backwards.
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.advanceCommitIndex()
			return
		}

		// rejected by the consistency check: back up nextIndex and retry now.
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			lastOfXTerm := -1
			for i := rf.lastLogIndex(); i > 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					lastOfXTerm = i
					break
				}
				if rf.log[i].Term < reply.XTerm {
					break
				}
			}
			if lastOfXTerm != -1 {
				rf.nextIndex[server] = lastOfXTerm + 1
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}
		rf.nextIndex[server] = max(1, min(rf.nextIndex[server], rf.lastLogIndex()+1))
		rf.replicateTo(server)
	}()
}

// commit the highest index N stored on a majority, but only if
// log[N] is from the current term (Figure 2, and Figure 8 for why).
// caller must hold rf.mu.
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			break // earlier entries have even older terms
		}
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

// send newly committed entries to the service, in order.
// runs in its own goroutine so applyCh is never written with rf.mu held.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		start, end := rf.lastApplied+1, rf.commitIndex
		msgs := make([]raftapi.ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			})
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, end)
	}
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.broadcastAppendEntries()
	return rf.lastLogIndex(), rf.currentTerm, true
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role == Leader {
			if time.Since(rf.lastHeartbeat) >= heartbeatInterval {
				rf.broadcastAppendEntries()
			}
		} else if time.Now().After(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()

		// the randomness lives in electionDeadline, so a short
		// fixed tick is enough here.
		time.Sleep(tickInterval)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.resetElectionTimer()
	rf.log = []LogEntry{{Term: 0}}
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
