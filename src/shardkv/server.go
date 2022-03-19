package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

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
	Op        string // "Get" or "Put" or "Append" or "Pull" or "Delete"
	ClientId  int64
	CommandId int64
	ConfigNum int
	Config    shardctrler.Config
	ShardIds  []int
	Shards    map[int]ShardState
}

type GeneralReply struct { //用于waitApply管道多个类型回复复用
	Value string
	Err   Err
}

type LastContext struct {
	CommandId int64
	Reply     GeneralReply
}

type ShardState struct {
	KVState      map[string]string     //存KV状态
	DuplicateMap map[int64]LastContext //存每一个clientId上一个请求的commandId和reply
	Mode         ShardMode             //Shard当前模式
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck         *shardctrler.Clerk               //Controller客户端
	config      shardctrler.Config               //当前配置
	lastconfig  shardctrler.Config               //上一个配置
	lastApplied int                              //上一个applied entry索引
	serverState [shardctrler.NShards]*ShardState //KV状态
	waitApply   map[int]chan *GeneralReply       //每一次请求，用来等待apply后返回给客户端
}

func (kv *ShardKV) isDuplicate(clientId int64, commandId int64, shardId int) bool { //判断重复请求
	lastContext, ok := kv.serverState[shardId].DuplicateMap[clientId]
	if !ok {
		return false
	}
	return lastContext.CommandId >= commandId
}

func (kv *ShardKV) getWaitApplyChan(index int) chan *GeneralReply { //获得第index个entry的apply通知管道
	_, ok := kv.waitApply[index]
	if !ok {
		kv.waitApply[index] = make(chan *GeneralReply, 1)
	}
	return kv.waitApply[index]
}

func (kv *ShardKV) waitForApply(index int, reply *GeneralReply) { //等待提交的command被apply
	kv.mu.Lock()
	waitApplyChan := kv.getWaitApplyChan(index)
	me := kv.me
	gid := kv.gid
	DPrintf("Group %v KVNode %v get wait apply chan %v %v\n", kv.gid, kv.me, index, waitApplyChan)
	kv.mu.Unlock()
	select { //等待被apply
	case generalReply := <-waitApplyChan:
		DPrintf("Group %v KVNode %v waitApplyChan receive CommandReply %v\n", gid, me, generalReply)
		reply.Value, reply.Err = generalReply.Value, generalReply.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go func() { //清除该管道，GC
		kv.mu.Lock()
		delete(kv.waitApply, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) canServe(shardId int) bool { //自己能否处理该shard请求
	return kv.config.Shards[shardId] == kv.gid && (kv.serverState[shardId].Mode == ShardServing || kv.serverState[shardId].Mode == ShardDeleting)
}

func (kv *ShardKV) KVCommand(args *KVCommandArgs, reply *KVCommandReply) { //处理客户端的Get、Put、Append请求
	// Your code here.
	kv.mu.Lock()
	DPrintf("Group %v KVNode %v process KVCommandArgs %v\n", kv.gid, kv.me, args)
	shardID := key2shard(args.Key)
	if !kv.canServe(shardID) {
		DPrintf("Group %v KVNode %v process wrong group CommandArgs %v\n", kv.gid, kv.me, args)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.Op != OpGet && kv.isDuplicate(args.ClientId, args.CommandId, shardID) { //处理Put、Append重复请求，这里是为了效率提前过滤一部分，applier中将过滤完所有的
		DPrintf("Group %v KVNode %v process duplicate CommandArgs %v\n", kv.gid, kv.me, args)
		reply.Value = kv.serverState[shardID].DuplicateMap[args.ClientId].Reply.Value
		reply.Err = kv.serverState[shardID].DuplicateMap[args.ClientId].Reply.Err
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
	generalReply := GeneralReply{}
	kv.waitForApply(index, &generalReply)
	reply.Value, reply.Err = generalReply.Value, generalReply.Err
}

func newShardState() *ShardState {
	return &ShardState{
		KVState:      make(map[string]string),
		DuplicateMap: make(map[int64]LastContext),
		Mode:         ShardInit,
	}
}

func (ss *ShardState) getCopy() *ShardState { //深复制
	ssCopy := &ShardState{
		KVState:      make(map[string]string),
		DuplicateMap: make(map[int64]LastContext),
		Mode:         ShardServing,
	}
	for k, v := range ss.KVState {
		ssCopy.KVState[k] = v
	}
	for k, v := range ss.DuplicateMap {
		ssCopy.DuplicateMap[k] = v
	}

	return ssCopy
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) { //拉取需要的shard，做为被调用方，返回调用者需要的shard
	if _, isLeader := kv.rf.GetState(); !isLeader { //只能从leader获取shard
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Group %v KVNode %v process PullShardArgs %v while my config num %v\n", kv.gid, kv.me, args, kv.config.Num)
	if kv.config.Num < args.ConfigNum { //调用者的config比我的新，我的数据还没有准备好，本轮配置可能还有已经commit但没有apply的操作，不能发
		DPrintf("Group %v KVNode %v PullShardArgs config num %v bigger than mine %v\n", kv.gid, kv.me, args.ConfigNum, kv.config.Num)
		reply.Err = ErrWrongConfig
		return
	}

	reply.ConfigNum = kv.config.Num
	reply.Shards = make(map[int]ShardState)
	for _, shardId := range args.ShardIds {
		reply.Shards[shardId] = *kv.serverState[shardId].getCopy()
	}
	reply.Err = OK
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) { //Pull shard完成后，调用者调用delete shard接口删除原group中的shard，释放内存
	if _, isLeader := kv.rf.GetState(); !isLeader { //只能从leader获取shard
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("Group %v KVNode %v process DeleteShardArgs %v while my config num %v \n", kv.gid, kv.me, args, kv.config.Num)
	if kv.config.Num > args.ConfigNum { //我的config比调用者的新，它可能是网络导致的重发请求，上一轮配置一定处理过了(否则pulled状态进入不了这一轮)，直接返回OK
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(Op{
		ShardIds:  args.ShardIds,
		ConfigNum: args.ConfigNum,
		Op:        OpDelete,
	})
	if !isLeader { //不是leader，返回错误
		reply.Err = ErrWrongLeader
		return
	}
	generalReply := GeneralReply{}
	kv.waitForApply(index, &generalReply)
	reply.Err = generalReply.Err
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) applyKVCommand(op Op) *GeneralReply { //将KVCommand应用到KVState上
	shardId := key2shard(op.Key)
	if !kv.canServe(shardId) {
		DPrintf("Group %v KVNode %v process OP %v wrong group error\n", kv.gid, kv.me, op)
		return &GeneralReply{Err: ErrWrongGroup}
	}
	if op.Op != OpGet && kv.isDuplicate(op.ClientId, op.CommandId, shardId) { //处理Put、Append重复请求
		DPrintf("Group %v KVNode %v process duplicate OP %v\n", kv.gid, kv.me, op)
		reply := kv.serverState[shardId].DuplicateMap[op.ClientId].Reply
		return &reply
	}

	reply := GeneralReply{}
	if op.Op == OpGet {
		val, ok := kv.serverState[shardId].KVState[op.Key]
		if !ok {
			reply.Value = ""
			reply.Err = ErrNoKey
			return &reply
		} else {
			reply.Value, reply.Err = val, OK
		}
	} else if op.Op == OpPut {
		kv.serverState[shardId].KVState[op.Key] = op.Value
		reply.Value, reply.Err = "", OK
	} else if op.Op == OpAppend {
		kv.serverState[shardId].KVState[op.Key] += op.Value
		reply.Value, reply.Err = "", OK
	}
	DPrintf("Group %v KVNode %v apply OP %v to KVState with shardId %v and reply %v\n", kv.gid, kv.me, op, shardId, reply)
	if op.Op == OpAppend || op.Op == OpPut { //更新duplicate检查map
		kv.serverState[shardId].DuplicateMap[op.ClientId] = LastContext{
			CommandId: op.CommandId,
			Reply:     reply,
		}
	}
	return &reply
}

func (kv *ShardKV) applyConfigCommand(op Op) *GeneralReply { //应用新的配置
	DPrintf("Group %v KVNode %v apply OP %v to KVState\n", kv.gid, kv.me, op)
	if op.Config.Num == kv.config.Num+1 {
		kv.lastconfig = kv.config
		kv.config = op.Config
		DPrintf("Group %v KVNode %v old config %v new config %v\n", kv.gid, kv.me, kv.lastconfig, kv.config)
		debugStr := ""
		for i := 0; i < shardctrler.NShards; i++ { //更新shard状态
			debugStr += fmt.Sprintf("%v:%v->", i, kv.serverState[i].Mode)
			if kv.config.Shards[i] == kv.gid && kv.lastconfig.Shards[i] != kv.gid { //新配置中属于本group，老配置中不属于，需要pull
				if kv.lastconfig.Shards[i] == 0 { //shard在上个配置中尚未分配，不用pull，直接serving
					kv.serverState[i].Mode = ShardServing //ShardInit -> ShardServing
				} else {
					kv.serverState[i].Mode = ShardPulling //ShardInit -> ShardPulling
				}
			} else if kv.config.Shards[i] != kv.gid && kv.lastconfig.Shards[i] == kv.gid { //新配置中不属于本group，老配置中属于，需要被pull
				kv.serverState[i].Mode = ShardPulled //ShardServing -> ShardPulled
			}
			debugStr += fmt.Sprintf("%v,", kv.serverState[i].Mode)
		}
		DPrintf("Group %v KVNode %v update shards mode [%v]\n", kv.gid, kv.me, debugStr)
	}
	return &GeneralReply{}
}

func (kv *ShardKV) applyDeleteCommand(op Op) *GeneralReply { //应用删除shard
	DPrintf("Group %v KVNode %v apply OP %v to KVState\n", kv.gid, kv.me, op)
	reply := GeneralReply{}
	if op.ConfigNum == kv.config.Num {
		debugStr := ""
		for _, shardId := range op.ShardIds {
			debugStr += fmt.Sprintf("%v:%v->", shardId, kv.serverState[shardId].Mode)
			shard := kv.serverState[shardId]
			if shard.Mode == ShardPulled { //删除shard内容
				kv.serverState[shardId] = newShardState() //ShardPulled -> ShardInit
			}
			debugStr += fmt.Sprintf("%v,", kv.serverState[shardId].Mode)
		}
		DPrintf("Group %v KVNode %v update shards mode [%v]\n", kv.gid, kv.me, debugStr)
		reply.Err = OK
	} else { //只可能是比我小的，因为比我大的已经在rpc接口中过滤了，比我小的肯定在上一轮配置中删过了，直接返回OK
		reply.Err = OK
	}
	return &reply
}

func (kv *ShardKV) applyPullCommand(op Op) *GeneralReply { //应用拉取的shard
	DPrintf("Group %v KVNode %v apply OP %v to KVState\n", kv.gid, kv.me, op)
	if op.ConfigNum == kv.config.Num {
		debugStr := ""
		for shardId, shardState := range op.Shards {
			debugStr += fmt.Sprintf("%v:%v->", shardId, kv.serverState[shardId].Mode)
			shard := kv.serverState[shardId]
			if shard.Mode == ShardPulling {
				kv.serverState[shardId] = shardState.getCopy()
				kv.serverState[shardId].Mode = ShardDeleting //进入Pull成功，删除远端愿数据状态，ShardPulling -> ShardDeleting
			}
			debugStr += fmt.Sprintf("%v,", kv.serverState[shardId].Mode)
		}
		DPrintf("Group %v KVNode %v update shards mode [%v]\n", kv.gid, kv.me, debugStr)
	}
	return &GeneralReply{}
}

func (kv *ShardKV) applyServeCommand(op Op) *GeneralReply { //删除远端shard成功，顺换状态
	DPrintf("Group %v KVNode %v apply OP %v to KVState\n", kv.gid, kv.me, op)
	if op.ConfigNum == kv.config.Num {
		debugStr := ""
		for _, shardId := range op.ShardIds {
			debugStr += fmt.Sprintf("%v:%v->", shardId, kv.serverState[shardId].Mode)
			shard := kv.serverState[shardId]
			if shard.Mode == ShardDeleting {
				kv.serverState[shardId].Mode = ShardServing //删除远端数据源成功，ShardDeleting -> ShardServing
			}
			debugStr += fmt.Sprintf("%v,", kv.serverState[shardId].Mode)
		}
		DPrintf("Group %v KVNode %v update shards mode [%v]\n", kv.gid, kv.me, debugStr)
	}
	return &GeneralReply{}
}

func (kv *ShardKV) applyEmptyCommand(op Op) *GeneralReply {
	//空操作，为了避免活锁，极端情况下比如group中三个主机A、B、C，A为leader，raft层的日志假设为[1,2]，A commit并且apply了2，但是B和C还没有，
	//这时整个group宕机了，重启后B成为新leader，虽然raft层的日志也为[1,2]但是由于不知道2是否已被提交，raft层会一直等待本任期的entry被提交顺带
	//将之前任期的entry提交，假如说2在本服务里是把shard的mode从Pulled设置为Init，A其实已经apply所以可以进一步的拉取新的config，但是B由于一直
	//等待本任期的entry，不敢commit以及apply这个之前任期的entry，所以那个shard会一直处于Pulled状态，因此无法更进一步拉取新的配置，形成了活锁，
	//因此上任后发送一个空操作可以避免这种情况，快速地将之前任期的entry一并commit以及apply
	DPrintf("Group %v KVNode %v apply OP %v to KVState\n", kv.gid, kv.me, op)
	return &GeneralReply{}
}

func (kv *ShardKV) applier() { //从raft层获得commit的Entry，应用到KVState上
	for kv.killed() == false {
		select {
		case applyMessage := <-kv.applyCh:
			if applyMessage.CommandValid { //apply命令
				kv.mu.Lock()
				DPrintf("Group %v KVNode %v start apply command message %v\n", kv.gid, kv.me, applyMessage)
				if applyMessage.CommandIndex <= kv.lastApplied {
					DPrintf("Group %v KVNode %v discard outdated command message %v\n", kv.gid, kv.me, applyMessage)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = applyMessage.CommandIndex
				op := applyMessage.Command.(Op)
				var reply *GeneralReply
				if op.Op == OpGet || op.Op == OpPut || op.Op == OpAppend {
					reply = kv.applyKVCommand(op)
				} else if op.Op == OpConfig {
					reply = kv.applyConfigCommand(op)
				} else if op.Op == OpDelete {
					reply = kv.applyDeleteCommand(op)
				} else if op.Op == OpPull {
					reply = kv.applyPullCommand(op)
				} else if op.Op == OpServe {
					reply = kv.applyServeCommand(op)
				} else if op.Op == OpEmpty {
					reply = kv.applyEmptyCommand(op)
				}

				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && currentTerm == applyMessage.CommandTerm { //只有Leader应该回复客户端，还需命令所在Term等于当前Term，防止Leader->Follower->Leader变换，index不是对应的Entry，在client超时时间大于选举时间时可能发生
					ch := kv.getWaitApplyChan(applyMessage.CommandIndex)
					ch <- reply
					DPrintf("Group %v KVNode %v send reply %v to waitApply channel %v %v done\n", kv.gid, kv.me, reply, applyMessage.CommandIndex, ch)
				}
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate { //达到阈值，制作快照
					kv.rf.Snapshot(applyMessage.CommandIndex, kv.makeSnapshot())
					DPrintf("Group %v KVNode %v make snapshot to index %v\n", kv.gid, kv.me, applyMessage.CommandIndex)
				}
				kv.mu.Unlock()
			} else if applyMessage.SnapshotValid { //apply快照
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(applyMessage.SnapshotTerm, applyMessage.SnapshotIndex, applyMessage.Snapshot) { //让raft持久化新的raft state和快照
					kv.installSnapshot(applyMessage.Snapshot)
					kv.lastApplied = applyMessage.SnapshotIndex
					DPrintf("Group %v KVNode %v install snapshot with index %v\n", kv.gid, kv.me, kv.lastApplied)
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) updater() { //更新配置
	complete := true //本轮是否已经所有Shard都到了稳定状态(Init和Serving)，其他状态则表示本轮配置还没有完成
	kv.mu.Lock()
	for i, shard := range kv.serverState {
		if shard.Mode != ShardInit && shard.Mode != ShardServing {
			complete = false
			DPrintf("Group %v KVNode %v check config update with config num %v fail with shard %v mode %v\n", kv.gid, kv.me, kv.config.Num, i, shard.Mode)
			break
		}
	}
	configNum := kv.config.Num
	me := kv.me
	gid := kv.gid
	DPrintf("Group %v KVNode %v check config update with config num %v and complete flag %v\n", kv.gid, kv.me, configNum, complete)
	kv.mu.Unlock()

	if complete { //可以进行下一轮配置获取
		newConfig := kv.mck.Query(configNum + 1)
		DPrintf("Group %v KVNode %v get new config %v\n", gid, me, newConfig)
		if newConfig.Num == configNum+1 {
			index, _, isLeader := kv.rf.Start(Op{
				Op:     OpConfig,
				Config: newConfig,
			})
			DPrintf("Group %v KVNode %v wait config %v apply with isLeader %v\n", gid, me, newConfig, isLeader)
			if isLeader {
				kv.waitForApply(index, &GeneralReply{}) //等待config操作被apply
			}
		}
	}
}

func (kv *ShardKV) getShardIdsByMode(mode ShardMode) map[int][]int { //根据mode获得shard id数组，以及它们上一轮对应的gid
	gid2sidsMap := make(map[int][]int)
	for shardId, shard := range kv.serverState {
		if shard.Mode != mode {
			continue
		}
		gid := kv.lastconfig.Shards[shardId] //从上一个配置获得请求目标gid
		gid2sidsMap[gid] = append(gid2sidsMap[gid], shardId)
	}
	return gid2sidsMap
}

func (kv *ShardKV) puller() { //检查是否有pulling状态的shard需要被拉取
	kv.mu.Lock()
	me := kv.me
	myGid := kv.gid
	gid2sidsMap := kv.getShardIdsByMode(ShardPulling)
	DPrintf("Group %v KVNode %v puller gid2sidsMap %v\n", myGid, me, gid2sidsMap)
	wg := sync.WaitGroup{}                   //用于等待本轮所有pull shard请求完成
	for gid, shardIds := range gid2sidsMap { //向每个group发送pull shard请求
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			args := PullShardArgs{
				ConfigNum: configNum,
				ShardIds:  shardIds,
			}
			for _, server := range servers { //遍历group中每个server，逐个尝试是否是leader
				reply := PullShardReply{}
				DPrintf("Group %v KVNode %v send pull shard args %v to server %v\n", myGid, me, args, server)
				if kv.make_end(server).Call("ShardKV.PullShard", &args, &reply) && reply.Err == OK { //拉取数据成功，开始raft共识
					DPrintf("Group %v KVNode %v call pull shard args %v with reply %v\n", myGid, me, args, reply)
					index, _, isLeader := kv.rf.Start(Op{
						Op:        OpPull,
						ConfigNum: reply.ConfigNum,
						Shards:    reply.Shards,
					})
					DPrintf("Group %v KVNode %v wait pull shardIds %v apply with isLeader %v\n", myGid, me, shardIds, isLeader)
					if isLeader {
						kv.waitForApply(index, &GeneralReply{}) //等待pull操作被apply
					}
					break
				}
			}
		}(kv.lastconfig.Groups[gid], kv.config.Num, shardIds)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) deleter() { //检查是否有deleting状态的shard需要将远端数据删除
	kv.mu.Lock()
	me := kv.me
	myGid := kv.gid
	gid2sidsMap := kv.getShardIdsByMode(ShardDeleting)
	DPrintf("Group %v KVNode %v deleter gid2sidsMap %v\n", myGid, me, gid2sidsMap)
	wg := sync.WaitGroup{}                   //用于等待本轮所有pull shard请求完成
	for gid, shardIds := range gid2sidsMap { //向每个group发送pull shard请求
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			args := DeleteShardArgs{
				ConfigNum: configNum,
				ShardIds:  shardIds,
			}
			for _, server := range servers { //遍历group中每个server，逐个尝试是否是leader
				reply := DeleteShardReply{}
				DPrintf("Group %v KVNode %v send delete shard args %v to server %v\n", myGid, me, args, server)
				if kv.make_end(server).Call("ShardKV.DeleteShard", &args, &reply) && reply.Err == OK { //删除数据成功，开始raft共识，转换shard mode到serving
					DPrintf("Group %v KVNode %v call delete shard args %v with reply %v\n", myGid, me, args, reply)
					index, _, isLeader := kv.rf.Start(Op{
						Op:        OpServe,
						ConfigNum: configNum,
						ShardIds:  shardIds,
					})
					DPrintf("Group %v KVNode %v wait delete shardIds %v apply with isLeader %v\n", myGid, me, shardIds, isLeader)
					if isLeader {
						kv.waitForApply(index, &GeneralReply{}) //等待pull操作被apply
					}
					break
				}
			}
		}(kv.lastconfig.Groups[gid], kv.config.Num, shardIds)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) emptySender() { //检查是否需要发送空操作，新leader上任时假如没有当前任期的entry，发送一个空操作，避免活锁
	kv.mu.Lock()
	me := kv.me
	myGid := kv.gid
	kv.mu.Unlock()
	DPrintf("Group %v KVNode %v check if need to send empty op\n", myGid, me)
	if !kv.rf.HasCurrentLog() {
		index, _, isLeader := kv.rf.Start(Op{
			Op: OpEmpty,
		})
		DPrintf("Group %v KVNode %v wait empty op apply with isLeader %v\n", myGid, me, isLeader)
		if isLeader {
			kv.waitForApply(index, &GeneralReply{}) //等待empty操作被apply
		}
	}
}

func (kv *ShardKV) Monitor(action func(), timeout time.Duration) { //定时检查模版函数
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.serverState)
	e.Encode(kv.config)
	e.Encode(kv.lastconfig)
	data := w.Bytes()
	return data
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var serverState [shardctrler.NShards]*ShardState
	var curConfig shardctrler.Config
	var lastConfig shardctrler.Config
	if d.Decode(&serverState) != nil ||
		d.Decode(&curConfig) != nil ||
		d.Decode(&lastConfig) != nil {
		fmt.Println("Decode Error! ")
		return
	}
	kv.serverState = serverState
	kv.config = curConfig
	kv.lastconfig = lastConfig
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &ShardKV{
		mu:           sync.Mutex{},
		me:           me,
		rf:           nil,
		applyCh:      make(chan raft.ApplyMsg),
		dead:         0,
		make_end:     make_end,
		gid:          gid,
		maxraftstate: maxraftstate,
		mck:          shardctrler.MakeClerk(ctrlers),
		config:       shardctrler.Config{Num: 0, Groups: map[int][]string{}},
		lastconfig:   shardctrler.Config{Num: 0, Groups: map[int][]string{}},
		lastApplied:  0,
		serverState:  [shardctrler.NShards]*ShardState{},
		waitApply:    make(map[int]chan *GeneralReply),
	}

	// Your initialization code here.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.serverState[i] = newShardState()
	}

	// You may need initialization code here.
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		DPrintf("Group %v KVNode %v initialize with snapshot\n", kv.gid, kv.me)
		kv.installSnapshot(snapshot)
	}
	go kv.applier()

	go kv.Monitor(kv.updater, 100*time.Millisecond)

	go kv.Monitor(kv.puller, 100*time.Millisecond)

	go kv.Monitor(kv.deleter, 100*time.Millisecond)

	go kv.Monitor(kv.emptySender, 100*time.Millisecond)

	return kv
}
