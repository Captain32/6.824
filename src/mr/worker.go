package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for true { //循环要Map任务
		args := AskForMapArgs{}
		reply := AskForMapReply{}
		if !call("Coordinator.AskForMap", &args, &reply) {
			return
		}
		fmt.Printf("AskForMapReply: %v\n", reply)

		if reply.Done { //Map阶段结束
			break
		}

		if reply.TaskId != -1 {
			reduceContent := make([][]KeyValue, reply.NReduce)
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName, string(content))
			for _, kv := range kva { //将中间kv对分散到n个Reduce任务
				idx := ihash(kv.Key) % reply.NReduce
				reduceContent[idx] = append(reduceContent[idx], kv)
			}

			for i, content := range reduceContent {
				tmpFile, _ := ioutil.TempFile("", "mr-tmp-*.json")
				jsonByte, _ := json.Marshal(content)
				tmpFile.Write(jsonByte)
				os.Rename(tmpFile.Name(), fmt.Sprintf("mr-%v-%v.json", reply.TaskId, i))
			}

			doneArgs := MapTaskDoneArgs{TaskId: reply.TaskId}
			doneReply := MapTaskDoneReply{}
			if !call("Coordinator.MapTaskDone", &doneArgs, &doneReply) {
				return
			}
		}
		time.Sleep(time.Second)
	}

	for true { //循环要Reduce任务
		args := AskForReduceArgs{}
		reply := AskForReduceReply{}
		if !call("Coordinator.AskForReduce", &args, &reply) {
			return
		}
		fmt.Printf("AskForReduceReply: %v\n", reply)

		if reply.Done { //Reduce阶段结束
			break
		}

		if reply.TaskId != -1 {
			intermediate := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				file, err := os.Open(fmt.Sprintf("mr-%v-%v.json", i, reply.TaskId))
				if err != nil {
					log.Fatalf("cannot open %v", fmt.Sprintf("mr-%v-%v.json", i, reply.TaskId))
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", fmt.Sprintf("mr-%v-%v.json", i, reply.TaskId))
				}
				file.Close()
				var kva []KeyValue
				_ = json.Unmarshal(content, &kva)
				intermediate = append(intermediate, kva...)
			}
			sort.Sort(ByKey(intermediate))

			tmpFile, _ := ioutil.TempFile("", "mr-tmp-*.txt")
			for i := 0; i < len(intermediate); {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%v", reply.TaskId))
			doneArgs := ReduceTaskDoneArgs{TaskId: reply.TaskId}
			doneReply := ReduceTaskDoneReply{}
			if !call("Coordinator.ReduceTaskDone", &doneArgs, &doneReply) {
				return
			}
		}
		time.Sleep(time.Second)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
