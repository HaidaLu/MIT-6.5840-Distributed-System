package kvraft

import (
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"

)

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu   sync.Mutex
	data map[string]Entry
}

// capitalized so it can be gob-encoded into a snapshot in 4C.
type Entry struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(&args)
	case rpc.PutArgs:
		return kv.doPut(&args)
	}
	return nil
}

func (kv *KVServer) doGet(args *rpc.GetArgs) rpc.GetReply {
	e, ok := kv.data[args.Key]
	if !ok {
		return rpc.GetReply{Err: rpc.ErrNoKey}
	}
	return rpc.GetReply{Value: e.Value, Version: e.Version, Err: rpc.OK}
}

// same semantics as Lab 2: install the value only if args.Version
// matches; version 0 creates a new key.
func (kv *KVServer) doPut(args *rpc.PutArgs) rpc.PutReply {
	e, ok := kv.data[args.Key]
	if !ok && args.Version != 0 {
		return rpc.PutReply{Err: rpc.ErrNoKey}
	}
	if e.Version != args.Version {
		return rpc.PutReply{Err: rpc.ErrVersion}
	}
	kv.data[args.Key] = Entry{Value: args.Value, Version: e.Version + 1}
	return rpc.PutReply{Err: rpc.OK}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	// submit by value: that's the type DoOp sees after the log's gob round trip.
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, data: make(map[string]Entry)}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
