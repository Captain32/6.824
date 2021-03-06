package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OpJoin  = "Join"
	OpLeave = "Leave"
	OpMove  = "Move"
	OpQuery = "Query"
)

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type CommandArgs struct {
	Servers   map[int][]string //Join
	GIDs      []int            //Leave
	Shard     int              //Move
	GID       int              //Move
	Num       int              //Query
	Op        string           // "Join" or "Leave" or "Move" or "Query"
	ClientId  int64
	CommandId int64
}

type CommandReply struct {
	Err    Err
	Config Config //Query
}
