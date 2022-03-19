package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongConfig = "ErrWrongConfig"
	ErrTimeout     = "ErrTimeout"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
	OpConfig = "Config" //Update onfig
	OpPull   = "Pull"   //Pull some shards
	OpDelete = "Delete" //Delete some shards
	OpServe  = "Serve"  //Change shards' mode to serve
	OpEmpty  = "Empty"  //Empty op, for liveness
)

//shard状态机，从头到尾依次转移状态，再回到Init
const (
	ShardInit     ShardMode = iota //空状态
	ShardPulling                   //数据拉取中
	ShardDeleting                  //数据拉取完成，删除远端数据源中
	ShardServing                   //正常服务中
	ShardPulled                    //被拉取中
)

type Err string

type ShardMode int

type KVCommandArgs struct {
	Key   string
	Value string
	Op    string // "Get" or "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
}

type KVCommandReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	ConfigNum int
	ShardIds  []int
}

type PullShardReply struct {
	Err       Err
	ConfigNum int
	Shards    map[int]ShardState
}

type DeleteShardArgs struct {
	ConfigNum int
	ShardIds  []int
}

type DeleteShardReply struct {
	Err Err
}
