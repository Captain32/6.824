package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderMap map[int]int
	clientId  int64
	commandId int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		leaderMap: make(map[int]int),
		clientId:  nrand(),
		commandId: 0,
	}
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := KVCommandArgs{
		Key:       key,
		Op:        OpGet,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := KVCommandReply{}
	ck.sendCommand(&args, &reply)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	args := KVCommandArgs{
		Key:       key,
		Value:     value,
		Op:        OpPut,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := KVCommandReply{}
	ck.sendCommand(&args, &reply)
}
func (ck *Clerk) Append(key string, value string) {
	args := KVCommandArgs{
		Key:       key,
		Value:     value,
		Op:        OpAppend,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := KVCommandReply{}
	ck.sendCommand(&args, &reply)
}

func (ck *Clerk) sendCommand(args *KVCommandArgs, reply *KVCommandReply) {
	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			oldLeaderId := ck.leaderMap[gid]
			for {
				srv := ck.make_end(servers[ck.leaderMap[gid]])
				DPrintf("Client %v send Command args %v to group %v server %v\n", ck.clientId, args, gid, servers[ck.leaderMap[gid]])
				if srv.Call("ShardKV.KVCommand", args, reply) {
					if reply.Err == OK || reply.Err == ErrNoKey {
						DPrintf("Client %v send Command args %v success with reply %v\n", ck.clientId, args, reply)
						ck.commandId++
						return
					} else if reply.Err == ErrWrongGroup {
						DPrintf("Client %v send Command args %v fail with reply %v\n", ck.clientId, args, reply)
						break
					}
				}
				DPrintf("Client %v send Command args %v fail with reply %v\n", ck.clientId, args, reply)
				ck.leaderMap[gid] = (ck.leaderMap[gid] + 1) % len(servers)
				if oldLeaderId == ck.leaderMap[gid] { //避免旧config中的group已经shutdown，但是call失败会一直在本group切换机器，同样会call失败，一直陷入for循环，这里call一整轮都失败了的话就跳出获取新config
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
