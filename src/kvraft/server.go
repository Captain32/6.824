package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string // "Get" or "Put" or "Append"
	ClientId  int64
	CommandId int64
}

type LastContext struct {
	commandId int64
	reply     *CommandReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied  int
	kvState      map[string]string          //KV状态
	duplicateMap map[int64]LastContext      //存每一个clientId上一个请求的commandId和reply
	waitApply    map[int]chan *CommandReply //每一次Command请求，用来等待apply后返回给客户端
}

func (kv *KVServer) isDuplicate(clientId int64, commandId int64) bool { //判断重复请求
	lastContext, ok := kv.duplicateMap[clientId]
	if !ok {
		return false
	}
	return lastContext.commandId >= commandId
}

func (kv *KVServer) getWaitApplyChan(index int) chan *CommandReply { //获得第index个entry的获得apply通知管道
	_, ok := kv.waitApply[index]
	if !ok {
		kv.waitApply[index] = make(chan *CommandReply, 1)
	}
	return kv.waitApply[index]
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("KVNode %v process CommandArgs %v\n", kv.me, args)
	if args.Op != OpGet && kv.isDuplicate(args.ClientId, args.CommandId) { //处理Put、Append重复请求，这里是为了效率提前过滤一部分，applier中将过滤完所有的
		DPrintf("KVNode %v process duplicate CommandArgs %v\n", kv.me, args)
		reply.Value = kv.duplicateMap[args.ClientId].reply.Value
		reply.Err = kv.duplicateMap[args.ClientId].reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	})
	if !isLeader { //不是leader，返回错误
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	waitApplyChan := kv.getWaitApplyChan(index)
	DPrintf("KVNode %v get wait apply chan %v %v\n", kv.me, index, waitApplyChan)
	kv.mu.Unlock()
	select { //等待Entry被apply
	case commandReply := <-waitApplyChan:
		DPrintf("waitApplyChan receive commandReply %v\n", commandReply)
		reply.Value, reply.Err = commandReply.Value, commandReply.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go func() { //清除该管道，GC
		kv.mu.Lock()
		delete(kv.waitApply, index)
		kv.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyEntry(op Op) *CommandReply { //将OP应用到KVState上
	reply := CommandReply{}
	if op.Op == OpGet {
		val, ok := kv.kvState[op.Key]
		if !ok {
			reply.Value = ""
			reply.Err = ErrNoKey
			return &reply
		}
		reply.Value, reply.Err = val, OK
	} else if op.Op == OpPut {
		kv.kvState[op.Key] = op.Value
		reply.Value, reply.Err = "", OK
	} else if op.Op == OpAppend {
		kv.kvState[op.Key] += op.Value
		reply.Value, reply.Err = "", OK
	}
	return &reply
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case applyMessage := <-kv.applyCh:
			if applyMessage.CommandValid { //apply命令
				kv.mu.Lock()
				DPrintf("KVNode %v start apply command message %v\n", kv.me, applyMessage)
				if applyMessage.CommandIndex <= kv.lastApplied {
					DPrintf("KVNode %v discard outdated command message %v\n", kv.me, applyMessage)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = applyMessage.CommandIndex
				op := applyMessage.Command.(Op)
				var reply *CommandReply
				if op.Op != OpGet && kv.isDuplicate(op.ClientId, op.CommandId) { //处理Put、Append重复请求
					DPrintf("KVNode %v process duplicate OP %v\n", kv.me, op)
					reply = kv.duplicateMap[op.ClientId].reply
				} else {
					reply = kv.applyEntry(op)
					DPrintf("KVNode %v apply OP %v to KVState with reply %v\n", kv.me, op, reply)
					if op.Op != OpGet { //更新duplicate检查map
						kv.duplicateMap[op.ClientId] = LastContext{
							commandId: op.CommandId,
							reply:     reply,
						}
					}
				}

				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && currentTerm == applyMessage.CommandTerm { //只有Leader应该回复客户端，还需命令所在Term等于当前Term，防止Leader->Follower->Leader变换，index不是对应的Entry，在client超时时间大于选举时间时可能发生
					ch := kv.getWaitApplyChan(applyMessage.CommandIndex)
					ch <- reply
					DPrintf("KVNode %v send reply %v to waitApply channel %v %v done\n", kv.me, reply, applyMessage.CommandIndex, ch)
				}
				if kv.maxraftstate != -1 {
					//TODO:make snapshot
				}
				kv.mu.Unlock()
			} else if applyMessage.SnapshotValid { //apply快照
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(applyMessage.SnapshotTerm, applyMessage.SnapshotIndex, applyMessage.Snapshot) {
					//TODO:read snapshot
					kv.lastApplied = applyMessage.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
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

	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		rf:           nil,
		applyCh:      make(chan raft.ApplyMsg),
		dead:         0,
		maxraftstate: maxraftstate,
		lastApplied:  0,
		kvState:      make(map[string]string),
		duplicateMap: make(map[int64]LastContext),
		waitApply:    make(map[int]chan *CommandReply),
	}

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
