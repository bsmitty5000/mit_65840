package mr

import (
	"encoding/json"
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

	// declare an argument structure.
	args := 0

	// declare a reply structure.
	reply := WorkerRequestReply{}

	for {
		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		ok := call("Coordinator.WorkerRequest", &args, &reply)
		if ok {
			if reply.Action == Map {
				file, err := os.Open(reply.ActionFilepath)
				if err != nil {
					log.Fatalf("cannot open %v", reply.ActionFilepath)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.ActionFilepath)
				}
				file.Close()

				kva := mapf(reply.ActionFilepath, string(content))

				reduceFileArray := make([][]KeyValue, reply.TotalReduce)
				for _, kv := range kva {
					reduceId := ihash(kv.Key) % reply.TotalReduce
					reduceFileArray[reduceId] = append(reduceFileArray[reduceId], kv)
				}

				for i, interKva := range reduceFileArray {
					intermediateFile := fmt.Sprintf("mr-%d-%d.json", reply.ActionId, i)
					file, err = os.Create(intermediateFile)
					if err != nil {
						log.Fatalf("cannot create %v", intermediateFile)
					}
					enc := json.NewEncoder(file)
					for _, kv := range interKva {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("could not write %s to json", kv)
						}
					}
				}
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
