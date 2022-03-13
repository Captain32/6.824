package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
		if !ck.servers[ck.leaderId].Call("KVServer.Command", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			DPrintf("Client %v send Command args %v fail with reply %v\n", ck.clientId, args, reply)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("Client %v send Command args %v success with reply %v\n", ck.clientId, args, reply)
		ck.commandId++
		return
	}
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
	args := CommandArgs{
		Key:       key,
		Op:        OpGet,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := CommandReply{}
	ck.sendCommand(&args, &reply)
	return reply.Value
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

func (ck *Clerk) Put(key string, value string) {
	args := CommandArgs{
		Key:       key,
		Value:     value,
		Op:        OpPut,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := CommandReply{}
	ck.sendCommand(&args, &reply)
}
func (ck *Clerk) Append(key string, value string) {
	args := CommandArgs{
		Key:       key,
		Value:     value,
		Op:        OpAppend,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	reply := CommandReply{}
	ck.sendCommand(&args, &reply)
}
