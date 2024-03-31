package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mu      sync.Mutex
	files   []string
	nReduce int
	currMap int
}

func (c *Coordinator) WorkerRequest(args *int, reply *WorkerRequestReply) error {
	c.mu.Lock()
	if len(c.files) > 0 {
		reply.Action = Map
		reply.ActionId = c.currMap
		reply.ActionFilepath = c.files[0]
		reply.TotalReduce = c.nReduce
		c.files = c.files[1:]
		c.currMap++
	} else {
		reply.Action = Terminate
	}
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce

	c.server()
	return &c
}
