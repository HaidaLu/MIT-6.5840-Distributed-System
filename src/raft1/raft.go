package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
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
	votedFor    int // candidateId that received vote in currentTerm, or -1

	// log[0] is a dummy entry standing for the last entry covered by
	// the snapshot: its index is lastIncludedIndex and its Term is
	// lastIncludedTerm. log[i] holds the entry at index lastIncludedIndex+i.
	log               []LogEntry
	lastIncludedIndex int
	snapshot          []byte // latest snapshot, saved alongside raft state

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

	applyCh         chan raftapi.ApplyMsg
	applyCond       *sync.Cond // signaled when commitIndex advances or a snapshot arrives
	pendingSnapshot bool       // an installed snapshot not yet sent on applyCh
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
//
// caller must hold rf.mu, and must call this before replying to an
// RPC or sending one that depends on the new state.
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&log) != nil {
		panic("readPersist: failed to decode raft state")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.log = log
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

	if index <= rf.lastIncludedIndex || index > rf.lastLogIndex() {
		return
	}
	rf.trimLogTo(index, rf.termAt(index))
	rf.snapshot = snapshot
	rf.persist()
}

// discard log entries up to and including index, which becomes the new
// dummy entry at log[0]. entries after index are kept if we have them.
// caller must hold rf.mu.
func (rf *Raft) trimLogTo(index int, term int) {
	var tail []LogEntry
	if index < rf.lastLogIndex() {
		tail = rf.log[index-rf.lastIncludedIndex+1:]
	}
	// copy into a fresh slice so the old backing array can be GC'd.
	newLog := make([]LogEntry, 1, 1+len(tail))
	newLog[0] = LogEntry{Term: term}
	newLog = append(newLog, tail...)
	rf.log = newLog
	rf.lastIncludedIndex = index
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.role = Follower
	rf.resetElectionTimer()

	// everything the snapshot covers is already committed here;
	// installing it would only move us backwards.
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// keep our log tail only if it agrees with the snapshot's last entry
	if args.LastIncludedIndex <= rf.lastLogIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.trimLogTo(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
		rf.lastIncludedIndex = args.LastIncludedIndex
	}
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.persist()

	// the applier delivers it, so it stays ordered with command applies.
	rf.pendingSnapshot = true
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
		rf.persist()
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

	prevLogIndex, prevLogTerm, entries := args.PrevLogIndex, args.PrevLogTerm, args.Entries
	if prevLogIndex < rf.lastIncludedIndex {
		// the start of this RPC is already in our snapshot. those entries
		// are committed, so they match the leader's; skip past them.
		skip := min(rf.lastIncludedIndex-prevLogIndex, len(entries))
		entries = entries[skip:]
		prevLogIndex, prevLogTerm = rf.lastIncludedIndex, rf.log[0].Term
	}

	// consistency check: our log must contain an entry at
	// PrevLogIndex whose term matches PrevLogTerm.
	if prevLogIndex > rf.lastLogIndex() {
		reply.XTerm = -1
		reply.XLen = rf.lastLogIndex() + 1
		return
	}
	if rf.termAt(prevLogIndex) != prevLogTerm {
		reply.XTerm = rf.termAt(prevLogIndex)
		xIndex := prevLogIndex
		for xIndex > rf.lastIncludedIndex+1 && rf.termAt(xIndex-1) == reply.XTerm {
			xIndex--
		}
		reply.XIndex = xIndex
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	// append new entries, truncating only at the first real conflict.
	// a stale (reordered) RPC must not cut off entries we already
	// accepted from a later one.
	for i, entry := range entries {
		idx := prevLogIndex + 1 + i
		if idx > rf.lastLogIndex() || rf.termAt(idx) != entry.Term {
			rf.log = append(rf.log[:idx-rf.lastIncludedIndex], entries[i:]...)
			rf.persist()
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
	rf.persist()
}

// caller must hold rf.mu.
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// caller must hold rf.mu.
func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// term of the entry at absolute index i; i must be >= lastIncludedIndex.
// caller must hold rf.mu.
func (rf *Raft) termAt(i int) int {
	return rf.log[i-rf.lastIncludedIndex].Term
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
	rf.persist()
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
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		// the entries this follower needs are gone; send the snapshot.
		rf.sendSnapshotTo(server)
		return
	}
	prevLogIndex := rf.nextIndex[server] - 1
	// copy, so later truncation of rf.log can't race with RPC encoding.
	tail := rf.log[prevLogIndex+1-rf.lastIncludedIndex:]
	entries := make([]LogEntry, len(tail))
	copy(entries, tail)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.termAt(prevLogIndex),
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
			for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
				if rf.termAt(i) == reply.XTerm {
					lastOfXTerm = i
					break
				}
				if rf.termAt(i) < reply.XTerm {
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

// caller must hold rf.mu.
func (rf *Raft) sendSnapshotTo(server int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.snapshot,
	}

	go func() {
		reply := &InstallSnapshotReply{}
		if !rf.sendInstallSnapshot(server, args, reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if rf.role != Leader || rf.currentTerm != args.Term {
			return
		}
		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}()
}

// commit the highest index N stored on a majority, but only if
// log[N] is from the current term (Figure 2, and Figure 8 for why).
// caller must hold rf.mu.
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
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
		for !rf.pendingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		if rf.pendingSnapshot {
			rf.pendingSnapshot = false
			if rf.lastIncludedIndex <= rf.lastApplied {
				continue // service is already past this snapshot
			}
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, msg.SnapshotIndex)
			continue
		}

		start, end := rf.lastApplied+1, rf.commitIndex
		msgs := make([]raftapi.ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.lastIncludedIndex].Command,
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
	rf.persist()
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
	rf.snapshot = persister.ReadSnapshot()
	// the service restores the snapshot itself on restart, so don't
	// resend it; resume applying right after it.
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
