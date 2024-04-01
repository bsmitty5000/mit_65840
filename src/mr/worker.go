package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
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
	var emptyArg struct{}

	for {
		// send the RPC request, wait for the request.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		request := WorkerRequestReply{}
		ok := call("Coordinator.WorkerRequest", &emptyArg, &request)
		if ok {
			if request.Action == Map {
				/* map task overview (heavily borrowed from mrsequential.go):
				 * 	- open the input file and read it all into a string
				 *	- call the mapf plugin function on the string
				 *	- create a temporary array of KeyValue arrays for each reduce task
				 *	- use the hash function to determine which reduce task a key should belong to
				 *	- write out the reduce task input into temp files then use os.rename
				 *	- send done status back to coordinator with the map taskId
				 */
				file, err := os.Open(request.ActionFilepath)
				if err != nil {
					log.Fatal(err)
				}
				defer file.Close()

				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", request.ActionFilepath)
				}

				kva := mapf(request.ActionFilepath, string(content))

				reduceFileArray := make([][]KeyValue, request.TotalReduce)
				for _, kv := range kva {
					reduceId := ihash(kv.Key) % request.TotalReduce
					reduceFileArray[reduceId] = append(reduceFileArray[reduceId], kv)
				}

				for i, interKva := range reduceFileArray {
					t, err := os.CreateTemp("", "")
					if err != nil {
						log.Fatal(err)
					}
					defer os.Remove(t.Name())

					enc := json.NewEncoder(t)
					for _, kv := range interKva {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("could not write %s to json", kv)
						}
					}

					intermediateFile := fmt.Sprintf("mr-%d-%d.json", request.ActionId, i)
					err = os.Rename(t.Name(), intermediateFile)
					if err != nil {
						log.Fatal(err)
					}
				}

				status := WorkerStatusArg{Action: Map, ActionId: request.ActionId, Status: Done}
				ok := call("Coordinator.WorkerStatusUpdate", &status, &emptyArg)
				if !ok {
					fmt.Printf("status update call failed!\n")
					return
				}
			} else if request.Action == Reduce {

				/* reduce task overview (heavily borrowed from mrsequential.go):
				 * 	- gather up all the input files (from a map task) that belong to this reduce task
				 *	- foreach file read & append to a KeyValue array
				 *	- sort the KeyValue array
				 *	- create a temp output file and print the output of reducef for each
				 *		unique key
				 *	- tell the coordinator this task is done
				 *	- rename the temp output file and remove all the input files
				 */
				reduceFilePattern := fmt.Sprintf("mr-*-%d.json", request.ActionId)
				reduceFiles, err := filepath.Glob(reduceFilePattern)
				if err != nil {
					log.Fatal(err)
				}

				var intermediate []KeyValue
				for _, filepath := range reduceFiles {

					file, err := os.Open(filepath)
					if err != nil {
						log.Fatal(err)
					}
					defer file.Close()

					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}

				sort.Sort(ByKey(intermediate))

				t, err := os.CreateTemp("", "")
				if err != nil {
					log.Fatal(err)
				}
				defer os.Remove(t.Name())

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(t, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				status := WorkerStatusArg{Action: Reduce, ActionId: request.ActionId, Status: Done}
				ok := call("Coordinator.WorkerStatusUpdate", &status, &emptyArg)
				if !ok {
					fmt.Printf("status update call failed!\n")
					return
				}

				oname := fmt.Sprintf("mr-out-%d", request.ActionId)
				err = os.Rename(t.Name(), oname)
				if err != nil {
					log.Fatal(err)
				}
				for _, filepath := range reduceFiles {
					os.Remove(filepath)
				}
			} else if request.Action == Wait {
				/* arbitrary 50ms sleep */
				time.Sleep(50 * time.Millisecond)
			} else {
				return
			}
		} else {
			fmt.Printf("call failed!\n")
			return
		}
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
