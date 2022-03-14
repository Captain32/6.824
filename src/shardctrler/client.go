package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int
	clientId  int64
	commandId int64
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
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
}

func (ck *Clerk) sendCommand(args *CommandArgs, reply *CommandReply) {
	for {
		DPrintf("Client %v send Command args %v\n", ck.clientId, args)
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			DPrintf("Client %v send Command args %v fail with reply %v\n", ck.clientId, args, reply)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("Client %v send Command args %v success with reply %v\n", ck.clientId, args, reply)
		ck.commandId++
		return
	}
}

func (ck *Clerk) Query(num int) Config {
	args := CommandArgs{
		Num:       num,
		Op:        OpQuery,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := CommandReply{}
	ck.sendCommand(&args, &reply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := CommandArgs{
		Servers:   servers,
		Op:        OpJoin,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := CommandReply{}
	ck.sendCommand(&args, &reply)
	return
}

func (ck *Clerk) Leave(gids []int) {
	args := CommandArgs{
		GIDs:      gids,
		Op:        OpLeave,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := CommandReply{}
	ck.sendCommand(&args, &reply)
	return
}

func (ck *Clerk) Move(shard int, gid int) {
	args := CommandArgs{
		Shard:     shard,
		GID:       gid,
		Op:        OpMove,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := CommandReply{}
	ck.sendCommand(&args, &reply)
	return
}
