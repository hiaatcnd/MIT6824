package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0
const boundRatio = 0.9

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
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
	Op        OpType
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type MergeID struct {
	c int64
	r int64
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db            map[string]string
	clerkID2reqID map[int64]int64
	appliedChs    map[MergeID]*chan struct{}
	killCh        chan struct{}
}

func (kv *KVServer) getCh(clientID, requestID int64) *chan struct{} {
	mID := MergeID{clientID, requestID}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.appliedChs[mID]; !ok {
		ch := make(chan struct{})
		kv.appliedChs[mID] = &ch
	}
	return kv.appliedChs[mID]
}

func timeoutCh(ch *chan struct{}) bool {
	select {
	case <-*ch:
		return true
	case <-time.After(time.Second):
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//DPrintf("kv %v receive a Get request from client %v of request %v: get %v", kv.me, args.ClerkID, args.RequestID, args.Key)
	// Your code here.
	reply.WrongLeader = true

	op := Op{OpGet, args.Key, "", args.ClerkID, args.RequestID}
	ch := kv.getCh(args.ClerkID, args.RequestID)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	if timeoutCh(ch) {
		DPrintf("kv %v applied a Get request from client %v of request %v: get %v", kv.me, args.ClerkID, args.RequestID, args.Key)
		kv.mu.Lock()
		reply.WrongLeader = false
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
	}

	DPrintf("kv %v reply: %v", kv.me, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("kv %v receive a PUTAPPEND request from client %v of request %v: %v key=%v value=%v", kv.me, args.ClerkID, args.RequestID, args.Op, args.Key, args.Value)
	// Your code here.
	reply.WrongLeader = true

	opType := OpPut
	if args.Op == "Append" {
		reply.WrongLeader = true
		opType = OpAppend
	}
	op := Op{opType, args.Key, args.Value, args.ClerkID, args.RequestID}
	ch := kv.getCh(args.ClerkID, args.RequestID)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	if timeoutCh(ch) {
		DPrintf("kv %v applied a PUTAPPEND request from client %v of request %v: %v key=%v value=%v", kv.me, args.ClerkID, args.RequestID, args.Op, args.Key, args.Value)
		reply.WrongLeader = false
	}

	DPrintf("kv %v reply: %v", kv.me, reply)
}

func (kv *KVServer) checkSnapshot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.maxraftstate > 0 && float64(kv.persister.RaftStateSize()) >= float64(kv.maxraftstate)*boundRatio
}

func (kv *KVServer) Snapshot(idx int) {
	if !kv.checkSnapshot() {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.clerkID2reqID)
	kv.rf.Compaction(idx, w.Bytes())
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var clerkID2reqID map[int64]int64

	if d.Decode(&db) != nil || d.Decode(&clerkID2reqID) != nil {
		DPrintf("readSnapshot decode error")
	} else {
		kv.mu.Lock()
		kv.db = db
		kv.clerkID2reqID = clerkID2reqID
		kv.mu.Unlock()
	}
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
	kv.killCh <- struct{}{}
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	kv.db = make(map[string]string)
	kv.clerkID2reqID = make(map[int64]int64)
	kv.appliedChs = make(map[MergeID]*chan struct{})
	kv.killCh = make(chan struct{}, 1)

	kv.readSnapshot(kv.persister.ReadSnapshot())

	// You may need initialization code here.
	go func() {
		for {
			var applyMsg raft.ApplyMsg
			select {
			case <-kv.killCh:
				return
			case applyMsg = <-kv.applyCh:
			}
			DPrintf("kv %v apply: %v", kv.me, applyMsg)
			if applyMsg.CommandValid {
				op := applyMsg.Command.(Op)

				kv.mu.Lock()
				reqID, exist := kv.clerkID2reqID[op.ClientID]
				if !exist || reqID < op.RequestID {
					if op.Op == OpPut {
						kv.db[op.Key] = op.Value
					} else if op.Op == OpAppend {
						kv.db[op.Key] += op.Value
					}
					kv.clerkID2reqID[op.ClientID] = op.RequestID
				}
				kv.mu.Unlock()

				mID := MergeID{op.ClientID, op.RequestID}
				kv.mu.Lock()
				if appliedCh, exist := kv.appliedChs[mID]; exist {
					close(*appliedCh)
					delete(kv.appliedChs, mID)
				}
				kv.mu.Unlock()

				kv.Snapshot(applyMsg.CommandIndex)
				DPrintf("kv %v done %v", kv.me, applyMsg)
			} else if applyMsg.UseSnapShot {
				kv.readSnapshot(applyMsg.Snapshot)
			}
		}
	}()

	return kv
}
