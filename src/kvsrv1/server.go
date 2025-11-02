package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Value struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvStore map[string]*Value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		kvStore: make(map[string]*Value),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, found := kv.kvStore[args.Key]
	if !found {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = v.Value
	reply.Version = v.Version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = rpc.OK

	v, found := kv.kvStore[args.Key]
	if !found {
		// If key doesn't exist and version is 0 create entry
		if args.Version == 0 {
			kv.kvStore[args.Key] = &Value{
				Value:   args.Value,
				Version: 1,
			}
			return
		}

		reply.Err = rpc.ErrNoKey
		return
	}

	// Check version
	if v.Version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	// Update value
	v.Value = args.Value
	v.Version++
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
