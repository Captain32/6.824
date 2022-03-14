package shardctrler

import (
	"6.824/labgob"
	"6.824/raft"
	"log"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type LastContext struct {
	CommandId int64
	Reply     CommandReply
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs      []Config                   // indexed by config num
	duplicateMap map[int64]LastContext      //存每一个clientId上一个请求的commandId和reply
	waitApply    map[int]chan *CommandReply //每一次Command请求，用来等待apply后返回给客户端
}

type Op struct {
	// Your data here.
	Op        string // "Join" or "Leave" or "Move" or "Query"
	ClientId  int64
	CommandId int64
	Servers   map[int][]string //Join
	GIDs      []int            //Leave
	Shard     int              //Move
	GID       int              //Move
	Num       int              //Query
}

func (sc *ShardCtrler) isDuplicate(clientId int64, commandId int64) bool { //判断重复请求
	lastContext, ok := sc.duplicateMap[clientId]
	if !ok {
		return false
	}
	return lastContext.CommandId >= commandId
}

func (sc *ShardCtrler) getWaitApplyChan(index int) chan *CommandReply { //获得第index个entry的apply通知管道
	_, ok := sc.waitApply[index]
	if !ok {
		sc.waitApply[index] = make(chan *CommandReply, 1)
	}
	return sc.waitApply[index]
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	sc.mu.Lock()
	DPrintf("ShardCtrler %v process CommandArgs %v\n", sc.me, args)
	if args.Op != OpQuery && sc.isDuplicate(args.ClientId, args.CommandId) { //处理重复写请求，这里是为了效率提前过滤一部分，applier中将过滤完所有的
		DPrintf("ShardCtrler %v process duplicate CommandArgs %v\n", sc.me, args)
		reply.Config = sc.duplicateMap[args.ClientId].Reply.Config
		reply.Err = sc.duplicateMap[args.ClientId].Reply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(Op{
		Op:        args.Op,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
		Servers:   args.Servers,
		GIDs:      args.GIDs,
		Shard:     args.Shard,
		GID:       args.GID,
		Num:       args.Num,
	})
	if !isLeader { //不是leader，返回错误
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	waitApplyChan := sc.getWaitApplyChan(index)
	DPrintf("ShardCtrler %v get wait apply chan %v %v\n", sc.me, index, waitApplyChan)
	sc.mu.Unlock()
	select { //等待Entry被apply
	case commandReply := <-waitApplyChan:
		DPrintf("waitApplyChan receive commandReply %v\n", commandReply)
		reply.Config, reply.Err = commandReply.Config, commandReply.Err
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go func() { //清除该管道，GC
		sc.mu.Lock()
		delete(sc.waitApply, index)
		sc.mu.Unlock()
	}()
}

func getCopyConfig(oldConfig *Config) *Config {
	newConfig := &Config{
		Num:    oldConfig.Num,
		Shards: oldConfig.Shards,
		Groups: make(map[int][]string),
	}
	for gid, serverList := range oldConfig.Groups { //老的group加进来
		newServerList := make([]string, len(serverList))
		copy(newServerList, serverList)
		newConfig.Groups[gid] = newServerList
	}
	return newConfig
}

func getGroup2ShardsMap(config *Config) map[int][]int { //获得group id到其上shards的map
	g2sMap := make(map[int][]int)
	for gid := range config.Groups { //所有gid初始化
		g2sMap[gid] = []int{}
	}
	for shard, gid := range config.Shards {
		g2sMap[gid] = append(g2sMap[gid], shard)
	}
	return g2sMap
}

func GetGIDWithMinShards(g2sMap map[int][]int) int {
	//map遍历有随机性，先输出到slice中，确定性遍历
	var keys []int
	for k := range g2sMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(g2sMap[gid]) < min { //gid为0代表未分配，尽可能地分配
			index, min = gid, len(g2sMap[gid])
		}
	}
	return index
}

func GetGIDWithMaxShards(g2sMap map[int][]int) int {
	//gid为0代表未分配，尽可能地分配
	if shards, ok := g2sMap[0]; ok && len(shards) > 0 {
		return 0
	}
	//map遍历有随机性，先输出到slice中，确定性遍历
	var keys []int
	for k := range g2sMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, max := -1, -1
	for _, gid := range keys {
		if len(g2sMap[gid]) > max {
			index, max = gid, len(g2sMap[gid])
		}
	}
	return index
}

func getBalanceShards(g2sMap map[int][]int) [NShards]int { //获得负载均衡后的shards数组
	for { //不断把负责shard数最多的服务器均衡一个到负责shard数最少的服务器
		src, dst := GetGIDWithMaxShards(g2sMap), GetGIDWithMinShards(g2sMap)
		if src == -1 || dst == -1 || (src != 0 && len(g2sMap[src])-len(g2sMap[dst]) <= 1) { //shard全部分配完毕，且最多比最少的多一个分区或0个
			break
		}
		g2sMap[dst] = append(g2sMap[dst], g2sMap[src][0])
		g2sMap[src] = g2sMap[src][1:]
	}

	var shards [NShards]int
	for gid, shardList := range g2sMap {
		for _, shard := range shardList {
			shards[shard] = gid
		}
	}
	return shards
}

func (sc *ShardCtrler) newJoinConfig(servers map[int][]string) *Config {
	newConfig := getCopyConfig(&sc.configs[len(sc.configs)-1]) //复制一份老的
	newConfig.Num += 1

	for gid, serverList := range servers { //新的group加进来
		newServerList := make([]string, len(serverList))
		copy(newServerList, serverList)
		newConfig.Groups[gid] = newServerList
	}

	g2sMap := getGroup2ShardsMap(newConfig)
	newConfig.Shards = getBalanceShards(g2sMap)
	return newConfig
}

func (sc *ShardCtrler) newLeaveConfig(gids []int) *Config {
	newConfig := getCopyConfig(&sc.configs[len(sc.configs)-1]) //复制一份老的
	newConfig.Num += 1

	g2sMap := getGroup2ShardsMap(newConfig)
	for _, gid := range gids { //删除离开的group
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shardList, ok := g2sMap[gid]; ok {
			g2sMap[0] = append(g2sMap[0], shardList...) //把原本在group上负责的shard放到未分配
			delete(g2sMap, gid)
		}
	}

	newConfig.Shards = getBalanceShards(g2sMap)
	return newConfig
}

func (sc *ShardCtrler) newMoveConfig(shard int, gid int) *Config {
	newConfig := getCopyConfig(&sc.configs[len(sc.configs)-1]) //复制一份老的
	newConfig.Num += 1
	newConfig.Shards[shard] = gid
	return newConfig
}

func (sc *ShardCtrler) applyEntry(op Op) *CommandReply { //将OP应用到configs上
	reply := CommandReply{}
	if op.Op == OpQuery {
		if op.Num < 0 || op.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[op.Num]
		}
		reply.Err = OK
	} else if op.Op == OpJoin {
		sc.configs = append(sc.configs, *sc.newJoinConfig(op.Servers))
		reply.Err = OK
	} else if op.Op == OpLeave {
		sc.configs = append(sc.configs, *sc.newLeaveConfig(op.GIDs))
		reply.Err = OK
	} else if op.Op == OpMove {
		sc.configs = append(sc.configs, *sc.newMoveConfig(op.Shard, op.GID))
		reply.Err = OK
	}
	return &reply
}

func (sc *ShardCtrler) applier() { //从raft层获得commit的Entry，应用到configs上
	for applyMessage := range sc.applyCh {
		sc.mu.Lock()
		DPrintf("ShardCtrler %v start apply command message %v\n", sc.me, applyMessage)
		op := applyMessage.Command.(Op)
		var reply CommandReply
		if op.Op != OpQuery && sc.isDuplicate(op.ClientId, op.CommandId) { //处理重复写请求
			DPrintf("ShardCtrler %v process duplicate OP %v\n", sc.me, op)
			reply = sc.duplicateMap[op.ClientId].Reply
		} else {
			reply = *sc.applyEntry(op)
			DPrintf("ShardCtrler %v apply OP %v to Configs with reply %v\n", sc.me, op, reply)
			if op.Op != OpQuery { //更新duplicate检查map
				sc.duplicateMap[op.ClientId] = LastContext{
					CommandId: op.CommandId,
					Reply:     reply,
				}
			}
		}

		currentTerm, isLeader := sc.rf.GetState()
		if isLeader && currentTerm == applyMessage.CommandTerm { //只有Leader应该回复客户端，还需命令所在Term等于当前Term，防止Leader->Follower->Leader变换，index不是对应的Entry，在client超时时间大于选举时间时可能发生
			ch := sc.getWaitApplyChan(applyMessage.CommandIndex)
			ch <- &reply
			DPrintf("ShardCtrler %v send reply %v to waitApply channel %v %v done\n", sc.me, reply, applyMessage.CommandIndex, ch)
		}
		sc.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := &ShardCtrler{
		mu:           sync.Mutex{},
		me:           me,
		rf:           nil,
		applyCh:      make(chan raft.ApplyMsg),
		configs:      []Config{{Groups: map[int][]string{}}},
		duplicateMap: make(map[int64]LastContext),
		waitApply:    make(map[int]chan *CommandReply),
	}

	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.applier()

	return sc
}
