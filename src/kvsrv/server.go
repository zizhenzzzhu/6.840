package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store       map[string]string
	processedID map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.AckRequest {
		delete(kv.processedID, args.RequestID)
		return
	}
	val, ok := kv.processedID[args.RequestID]
	if ok {
		reply.Value = val
		return
	}

	val, ok = kv.store[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = val
	}
	kv.processedID[args.RequestID] = reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Op != "Put" {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.AckRequest {
		delete(kv.processedID, args.RequestID)
		return
	}
	val, ok := kv.processedID[args.RequestID]
	if ok {
		reply.Value = val
		return
	}
	kv.store[args.Key] = args.Value
	reply.Value = args.Value
	kv.processedID[args.RequestID] = reply.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Op != "Append" {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.AckRequest {
		delete(kv.processedID, args.RequestID)
		return
	}
	val, ok := kv.processedID[args.RequestID]
	if ok {
		reply.Value = val
		return
	}
	oldVal := kv.store[args.Key]
	kv.store[args.Key] = oldVal + args.Value
	reply.Value = oldVal
	kv.processedID[args.RequestID] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.processedID = make(map[int64]string)

	return kv
}
