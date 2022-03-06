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
	TaskId   int    //任务id
	NReduce  int    //Reduce分区数目
	FileName string //本map任务对应的文件
	Done     bool   //Map阶段是否结束
}

type MapTaskDoneArgs struct {
	TaskId int //任务id
}

type MapTaskDoneReply struct {
}

type AskForReduceArgs struct {
}

type AskForReduceReply struct {
	TaskId int  //任务id
	NMap   int  //Map任务数目(需要读中间文件的个数)
	Done   bool //Reduce阶段是否结束
}

type ReduceTaskDoneArgs struct {
	TaskId int //任务id
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
