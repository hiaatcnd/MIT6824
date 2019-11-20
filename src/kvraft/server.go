package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ClientReqID struct {
	Clerk   int64
	Request int64
}

type OpType int

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    OpType
	Key   string
	Value string
	CrID  ClientReqID
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db            map[string]string
	clerkID2reqID map[int64]int64
	appliedChs    map[ClientReqID]chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//DPrintf("receive a Get request from client %v of request %v: get %v", args.ClerkID, args.RequestID, args.Key)
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	kv.mu.Lock()
	if kv.clerkID2reqID[args.ClerkID] >= args.RequestID {
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
		return
	} else {
		kv.clerkID2reqID[args.ClerkID] = args.RequestID
	}
	kv.mu.Unlock()

	crID := ClientReqID{args.ClerkID, args.RequestID}
	op := Op{OpGet, args.Key, "", crID}
	kv.rf.Start(op)
	appliedCh := make(chan struct{})
	kv.mu.Lock()
	kv.appliedChs[crID] = appliedCh
	kv.mu.Unlock()
	defer func() {
		close(appliedCh)
		kv.mu.Lock()
		delete(kv.appliedChs, crID)
		kv.mu.Unlock()
	}()

	select {
	case <-appliedCh:
	case <-time.After(3 * time.Second):
		reply.Err = "Timeout"
		return
	}

	DPrintf("applied a Get request from client %v of request %v: get %v", args.ClerkID, args.RequestID, args.Key)

	kv.mu.Lock()
	v := kv.db[args.Key]
	reply.Value = v
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("receive a PUTAPPEND request from client %v of request %v: %v key=%v value=%v", args.ClerkID, args.RequestID, args.Op, args.Key, args.Value)
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false

	kv.mu.Lock()
	if kv.clerkID2reqID[args.ClerkID] >= args.RequestID {
		kv.mu.Unlock()
		return
	} else {
		kv.clerkID2reqID[args.ClerkID] = args.RequestID
	}
	kv.mu.Unlock()

	crID := ClientReqID{args.ClerkID, args.RequestID}
	opType := OpPut
	if args.Op == "Append" {
		opType = OpAppend
	}
	op := Op{opType, args.Key, args.Value, crID}
	kv.rf.Start(op)
	appliedCh := make(chan struct{})
	kv.mu.Lock()
	kv.appliedChs[crID] = appliedCh
	kv.mu.Unlock()
	defer func() {
		close(appliedCh)
		kv.mu.Lock()
		delete(kv.appliedChs, crID)
		kv.mu.Unlock()
	}()

	select {
	case <-appliedCh:
	case <-time.After(3 * time.Second):
		reply.Err = "Timeout"
		return
	}

	DPrintf("applied a PUTAPPEND request from client %v of request %v: %v key=%v value=%v", args.ClerkID, args.RequestID, args.Op, args.Key, args.Value)

	kv.mu.Lock()
	if opType == OpPut {
		kv.db[args.Key] = args.Value
	} else if opType == OpAppend {
		kv.db[args.Key] = kv.db[args.Key] + args.Value
	} else {
		reply.Err = "Unknown Op type"
	}
	DPrintf("reply: %v", reply)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ClientReqID{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.clerkID2reqID = make(map[int64]int64)
	kv.appliedChs = make(map[ClientReqID]chan struct{})

	// You may need initialization code here.
	go func() {
		for {
			select {
			case applyMsg := <-kv.applyCh:
				DPrintf("apply: %v", applyMsg)
				op := applyMsg.Command.(Op)
				kv.mu.Lock()
				appliedCh, exist := kv.appliedChs[op.CrID]
				if exist {
					appliedCh <- struct{}{}
				}
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
