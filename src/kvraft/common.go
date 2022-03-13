package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type Err string

// Put or Append
type CommandArgs struct {
	Key   string
	Value string
	Op    string // "Get" or "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
}

type CommandReply struct {
	Err   Err
	Value string
}
