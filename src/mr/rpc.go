package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type AskForMapArgs struct {
}

type AskForMapReply struct {
	TaskId   int
	NReduce  int
	FileName string
	Done     bool
}

type MapTaskDoneArgs struct {
	TaskId int
}

type MapTaskDoneReply struct {
}

type AskForReduceArgs struct {
}

type AskForReduceReply struct {
	TaskId int
	NMap   int
	Done   bool
}

type ReduceTaskDoneArgs struct {
	TaskId int
}

type ReduceTaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
