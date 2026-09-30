package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"

)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int   // server that submitted the op
	Id  int64 // random, so ops from before a restart can't match new ones
	Req any
}

// a Submit() waiting for the op it passed to Start() to be applied.
type waiter struct {
	id   int64
	term int         // term returned by Start()
	ch   chan result // buffered, so the reader never blocks on it
}

type result struct {
	err rpc.Err
	rep any
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
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
	waiters map[int]*waiter // log index -> Submit() waiting on it
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiters:      make(map[int]*waiter),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	op := Op{Me: rsm.me, Id: rand.Int63(), Req: req}

	// hold rsm.mu across Start() so the reader can't apply the op
	// before its waiter is registered.
	rsm.mu.Lock()
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	w := &waiter{id: op.Id, term: term, ch: make(chan result, 1)}
	rsm.waiters[index] = w
	rsm.mu.Unlock()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case res := <-w.ch:
			return res.err, res.rep
		case <-ticker.C:
			// if we lost leadership the op may never commit.
			if curTerm, isLeader := rsm.rf.GetState(); curTerm != term || !isLeader {
				rsm.mu.Lock()
				if rsm.waiters[index] == w {
					delete(rsm.waiters, index)
				}
				rsm.mu.Unlock()
				// the reader may have delivered just before we removed w.
				select {
				case res := <-w.ch:
					return res.err, res.rep
				default:
					return rpc.ErrWrongLeader, nil
				}
			}
		}
	}
}

// apply committed ops to the state machine, on every peer, and hand
// each result to the Submit() waiting for it, if any.
func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if !msg.CommandValid {
			continue // snapshots are handled in 4C
		}
		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}
		rep := rsm.sm.DoOp(op.Req)

		rsm.mu.Lock()
		if w, ok := rsm.waiters[msg.CommandIndex]; ok {
			delete(rsm.waiters, msg.CommandIndex)
			if op.Me == rsm.me && op.Id == w.id {
				w.ch <- result{rpc.OK, rep}
			} else {
				// a different op was committed at our index: we lost leadership.
				w.ch <- result{rpc.ErrWrongLeader, nil}
			}
		}
		rsm.mu.Unlock()
	}
}
