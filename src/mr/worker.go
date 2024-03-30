package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := RegisterSelf()
	if workerId < 0 {
		fmt.Printf("failed to register, quitting")
		return
	}

	// declare an argument structure.
	args := workerId

	// declare a reply structure.
	reply := MapRequestReply{}

	intermediate := []KeyValue{}
	for {
		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		ok := call("Coordinator.MapRequest", &args, &reply)
		if ok {
			if reply.Action == ProcessFile {
				file, err := os.Open(reply.Filepath)
				if err != nil {
					log.Fatalf("cannot open %v", reply.Filepath)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.Filepath)
				}
				file.Close()
				kva := mapf(reply.Filepath, string(content))
				intermediate = append(intermediate, kva...)
			} else if reply.Action == Terminate {
				fmt.Printf("all done")
				break
			}
		} else {
			fmt.Printf("call failed!\n")
		}
		time.Sleep(1 * time.Second)
	}

}

func RegisterSelf() int {

	// declare an argument structure.
	args := 0
	id := 0

	ok := call("Coordinator.WorkerRegister", &args, &id)
	if ok {
		return id
	} else {
		fmt.Printf("call failed!\n")
		return -1
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
