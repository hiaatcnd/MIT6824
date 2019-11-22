package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id         int64
	requestNum int64
	leaderIdx  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.requestNum = 0
	ck.leaderIdx = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.requestNum++
	DPrintf("Client %v Request %v Get %v", ck.id, ck.requestNum, key)
	for {
		args := GetArgs{
			Key:       key,
			ClerkID:   ck.id,
			RequestID: ck.requestNum,
		}
		var reply GetReply
		//DPrintf("Client %v Request %v call server %v args: %v", ck.id, ck.requestNum, ck.leaderIdx, args)
		if !ck.servers[ck.leaderIdx].Call("KVServer.Get", &args, &reply) || reply.WrongLeader {
			//DPrintf("Client %v Request %v Wrong Leader or cannot connected", ck.id, ck.requestNum)
			ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		DPrintf("Client %v Request %v receive :%v", ck.id, ck.requestNum, reply)
		if reply.Err != "" {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestNum++
	//DPrintf("Client %v Request %v %v: %v->%v", ck.id, ck.requestNum, op, key, value)
	for {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        op,
			ClerkID:   ck.id,
			RequestID: ck.requestNum,
		}
		var reply PutAppendReply
		//DPrintf("Client %v Request %v call server %v args: %v", ck.id, ck.requestNum, ck.leaderIdx, args)
		if !ck.servers[ck.leaderIdx].Call("KVServer.PutAppend", &args, &reply) || reply.WrongLeader {
			//DPrintf("Client %v Request %v Wrong Leader or cannot connected", ck.id, ck.requestNum)
			ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			continue
		}
		DPrintf("Client %v Request %v receive :%v", ck.id, ck.requestNum, reply)
		if reply.Err != "" {
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
