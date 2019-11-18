package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int
	OpID       int
	ClientID   int64
	// You will have to modify this struct.
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
	ck.lastLeader = 0
	ck.OpID = 0
	ck.ClientID = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key: key, 
		ClientID: ck.ClientID, 
		OpID: ck.OpID,
	}
	ck.OpID++
	reply := GetReply{}
	leaderID := ck.lastLeader
	ok := ck.servers[leaderID].Call("RaftKV.Get", &args, &reply)
	DPrintf("Client GET (%v, %d) operation, result is %v, %v", args.ClientID, args.OpID, ok, reply.WrongLeader)
	for !ok || reply.WrongLeader {
		leaderID = (leaderID + 1) % len(ck.servers)
		reply = GetReply{}
		ok = ck.servers[leaderID].Call("RaftKV.Get", &args, &reply)
		DPrintf("Client GET (%v, %d) operation, result is %v, %v", args.ClientID, args.OpID, ok, reply.WrongLeader)
	}
	ck.lastLeader = leaderID
	DPrintf("Client GET operation (%v, %d) completed", args.ClientID, args.OpID)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.ClientID, OpID: ck.OpID}
	ck.OpID++
	leaderID := ck.lastLeader
	reply := PutAppendReply{}
	ok := ck.servers[leaderID].Call("RaftKV.PutAppend", &args, &reply)
	DPrintf("Client %v operation (%v, %d), result is %v, %v", op, args.ClientID, args.OpID, ok, reply.WrongLeader)
	for !ok || reply.WrongLeader {
		leaderID = (leaderID + 1) % len(ck.servers)
		reply = PutAppendReply{}
		ok = ck.servers[leaderID].Call("RaftKV.PutAppend", &args, &reply)
		DPrintf("Client %v operation (%v, %d), result is %v, %v", op, args.ClientID, args.OpID, ok, reply.WrongLeader)
	}
	DPrintf("Client %v operation (%v %d) completed", op, args.ClientID, args.OpID)
	ck.lastLeader = leaderID
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
