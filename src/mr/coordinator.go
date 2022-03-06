package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type task struct {
	state     int       //0:未开始 1:运行中 2:已完成
	startTime time.Time //本次运行开始时间
}

type Coordinator struct {
	files       []string
	nReduce     int
	mu          sync.Mutex
	mapTasks    []task
	reduceTasks []task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskForMap(args *AskForMapArgs, reply *AskForMapReply) error {
	unFinished := len(c.files)
	reply.NReduce = c.nReduce
	reply.Done = false
	reply.TaskId = -1
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, task := range c.mapTasks {
		if task.state == 0 || (task.state == 1 && time.Now().Sub(task.startTime).Seconds() > 10) {
			c.mapTasks[i].state = 1
			c.mapTasks[i].startTime = time.Now()
			reply.FileName = c.files[i]
			reply.TaskId = i
			fmt.Printf("MapTask %v dipatched, file name: %v\n", i, c.files[i])
			break
		} else if task.state == 2 {
			unFinished--
		}
	}
	if unFinished == 0 {
		reply.Done = true
	}
	return nil
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapTasks[args.TaskId].state = 2
	fmt.Printf("MapTask %v done, file name: %v\n", args.TaskId, c.files[args.TaskId])
	return nil
}

func (c *Coordinator) AskForReduce(args *AskForReduceArgs, reply *AskForReduceReply) error {
	unFinished := c.nReduce
	reply.Done = false
	reply.NMap = len(c.files)
	reply.TaskId = -1
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, task := range c.reduceTasks {
		if task.state == 0 || (task.state == 1 && time.Now().Sub(task.startTime).Seconds() > 10) {
			c.reduceTasks[i].state = 1
			c.reduceTasks[i].startTime = time.Now()
			reply.TaskId = i
			fmt.Printf("ReduceTask %v dipatched\n", i)
			break
		} else if task.state == 2 {
			unFinished--
		}
	}
	if unFinished == 0 {
		reply.Done = true
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTasks[args.TaskId].state = 2
	fmt.Printf("ReduceTask %v done\n", args.TaskId)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.reduceTasks {
		if task.state != 2 {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mu:          sync.Mutex{},
		mapTasks:    make([]task, len(files)),
		reduceTasks: make([]task, nReduce),
	}

	for i, _ := range c.mapTasks {
		c.mapTasks[i] = task{
			state:     0,
			startTime: time.Time{},
		}
	}

	for i, _ := range c.reduceTasks {
		c.reduceTasks[i] = task{
			state:     0,
			startTime: time.Time{},
		}
	}

	c.server()
	return &c
}
