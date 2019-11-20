package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	OpID      int
	ClientID  int64
}

type Result struct {
	OpID     int
	ClientID int64
	Value    string
	Error    Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB map[string]string
	clientOpRecord map[int64]int
	result        map[int]chan Result
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server %d received 'Get' Operation: %s, %v, %d", kv.me, args.Key, args.ClientID, args.OpID)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("Server %d is not leader", kv.me)
		reply.WrongLeader = true
		reply.Value = ""
		return
	} else {
		command := Op{Operation: "Get", Key: args.Key, Value: "", OpID: args.OpID, ClientID: args.ClientID}
		index, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			reply.WrongLeader = true
			reply.Value = ""
			return
		}
		kv.mu.Lock()
		ch, ok := kv.result[index]
		DPrintf("Server %d putappend %v %d check index %d, ok? %v", kv.me, args.ClientID, args.OpID, index, ok)
		if !ok {
			ch = make(chan Result, 1)
			kv.result[index] = ch
		}
		kv.mu.Unlock()

		select {
		case result := <-ch:
			if result.ClientID == args.ClientID && result.OpID == args.OpID {
				reply.WrongLeader = false
				reply.Value = result.Value
				reply.Err = result.Error
				return
			} else {
				reply.WrongLeader = true
				return
			}
		case <-time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server %d reveived %s Operation: %v, %d, %v, %s", args.Op, kv.me, args.ClientID, args.OpID, args.Key, args.Value)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("%d is not leader", kv.me)
		reply.WrongLeader = true
		return
	} else {
		command := Op{Operation: args.Op, Key: args.Key, Value: args.Value, OpID: args.OpID, ClientID: args.ClientID}
		index, _, isLeader := kv.rf.Start(command)
		if !isLeader {
			reply.WrongLeader = true
			return
		}
		kv.mu.Lock()
		ch, ok := kv.result[index]
		DPrintf("Server %d putappend %v %d check index %d, ok? %v", kv.me, args.ClientID, args.OpID, index, ok)
		if !ok {
			ch = make(chan Result, 1)
			kv.result[index] = ch
		}
		kv.mu.Unlock()

		select {
		case result := <-ch:
			if result.ClientID == args.ClientID && result.OpID == args.OpID {
				reply.WrongLeader = false
				reply.Err = result.Error
				return
			} else {
				reply.WrongLeader = true
				return
			}
		case <-time.After(1000 * time.Millisecond):
			reply.WrongLeader = true
			return
		}
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvDB = make(map[string]string)
	kv.clientOpRecord = make(map[int64]int)
	kv.result = make(map[int]chan Result)
	go kv.updateDB()
	return kv
}

func (kv *RaftKV) opCheck(clientID int64, opID int) bool {
	op, ok := kv.clientOpRecord[clientID]
	if !ok {
		return false
	}
	if op < opID {
		return false
	} else {
		return true
	}
}

func (kv *RaftKV) updateDB() {
	for {
		a := <-kv.applyCh
		if a.UseSnapshot {
			var lastIncludedIndex int
			var lastIncludedTerm int
			snapshot := bytes.NewBuffer(a.Snapshot)
			decoder := gob.NewDecoder(snapshot)
			decoder.Decode(&lastIncludedIndex)
			decoder.Decode(&lastIncludedTerm)
			kv.mu.Lock()
			kv.kvDB = make(map[string]string)
			kv.clientOpRecord = make(map[int64]int)
			decoder.Decode(&kv.kvDB)
			decoder.Decode(&kv.clientOpRecord)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Lock()
		log := a.Command.(Op)
		var result Result
		result.OpID = log.OpID
		result.ClientID = log.ClientID
		if !kv.opCheck(log.ClientID, log.OpID) && log.Operation == "Append" {
			v, ok := kv.kvDB[log.Key]
			if ok {
				kv.kvDB[log.Key] = v + log.Value
			} else {
				kv.kvDB[log.Key] = log.Value
			}
		} else if !kv.opCheck(log.ClientID, log.OpID) && log.Operation == "Put" {
			kv.kvDB[log.Key] = log.Value
		} else {
			value, ok := kv.kvDB[log.Key]
			if ok {
				result.Error = OK
				result.Value = value
			} else {
				result.Value = ""
				result.Error = ErrNoKey
			}
		}
		if !kv.opCheck(log.ClientID, log.OpID) {
			kv.clientOpRecord[log.ClientID] = log.OpID
		}
		ch, ok := kv.result[a.Index]
		DPrintf("index: %d, %v", a.Index, ok)
		if ok {
			select {
			case <-ch:
			default:
			}
			ch <- result
		} else {
			kv.result[a.Index] = make(chan Result, 1)
		}

		if kv.maxraftstate != -1 && kv.maxraftstate < kv.rf.GetPersistSize() {
			buf := new(bytes.Buffer)
			encoder := gob.NewEncoder(buf)
			encoder.Encode(kv.kvDB)
			encoder.Encode(kv.clientOpRecord)
			go kv.rf.StartSnapshot(buf.Bytes(), a.Index)
		}
		kv.mu.Unlock()
	}
}


