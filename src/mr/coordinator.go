package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu sync.Mutex
	/* input files to process. left untouched */
	files []string
	/* number of reduce jobs to allow. given to workers, left untouched */
	nReduce int

	/* the 'tasks' here indicate indices into the files array for the map
	 * tasks and task numbers for the reduce tasks which tell the reduce
	 * task which files to process, ie 'mr-{reduce task #}-Y.json'
	 * When a task id is assigned it's popped off the stack. if a task
	 * doesn't complete in time the task id is pushed back onto the stack
	 */
	mapTasksToAssign    []int
	reduceTasksToAssign []int

	/* will be initialized to len(files) and nReduce respectively
	 * but will be decremented when a task reports back complete
	 * Separate from checking len(tasksToAssign) since those are
	 * only used to check if we have any more tasks to assign, not
	 * if we have any  more tasks in flight
	 */
	mapTasksRemaining    int
	reduceTasksRemaining int

	/* when tasks get assigned a chan will be added with the task id as key
	 * to be used by the go routines timing the tasks. these channels will be
	 * used to interrupt the timers when the tasks update as done
	 */
	reduceChannels map[int]chan bool
	mapChannels    map[int]chan bool
}

func (c *Coordinator) WaitForMap(mapIdx int) {
	c.mu.Lock()
	taskDoneChannel := c.mapChannels[mapIdx]
	c.mu.Unlock()
	timer1 := time.NewTimer(10 * time.Second)
	select {
	case result := <-taskDoneChannel:
		/* drain the timer channel if it happened to have stopped
		* at the same moment
		 */
		if !timer1.Stop() {
			<-timer1.C
		}
		if !result {
			c.mu.Lock()
			c.reduceTasksToAssign = append(c.reduceTasksToAssign, mapIdx)
			c.mu.Unlock()
		}
	case <-timer1.C:
		c.mu.Lock()
		c.mapTasksToAssign = append(c.mapTasksToAssign, mapIdx)
		c.mu.Unlock()
	}
	c.mu.Lock()
	delete(c.mapChannels, mapIdx)
	close(taskDoneChannel)
	c.mu.Unlock()
}
func (c *Coordinator) WaitForReduce(redIdx int) {
	c.mu.Lock()
	taskDoneChannel := c.reduceChannels[redIdx]
	c.mu.Unlock()
	timer1 := time.NewTimer(10 * time.Second)
	select {
	case result := <-taskDoneChannel:
		/* drain the timer channel if it happened to have stopped
		* at the same moment
		 */
		if !timer1.Stop() {
			<-timer1.C
		}
		if !result {
			c.mu.Lock()
			c.reduceTasksToAssign = append(c.reduceTasksToAssign, redIdx)
			c.mu.Unlock()
		}
	case <-timer1.C:
		c.mu.Lock()
		c.reduceTasksToAssign = append(c.reduceTasksToAssign, redIdx)
		c.mu.Unlock()
	}
	c.mu.Lock()
	delete(c.reduceChannels, redIdx)
	close(taskDoneChannel)
	c.mu.Unlock()
}

func (c *Coordinator) WorkerStatusUpdate(args *WorkerStatusArg, _ *struct{}) error {
	/* if status indicates done send a true value over the channel and decrement the number
	 * of tasks remaining. otherwise, send false over channel
	 * the go routine waiting for the timer is reponsible for putting the task back onto
	 * worker remaining stack
	 */
	if args.Status == Done {
		if args.Action == Map {
			c.mu.Lock()
			c.mapChannels[args.ActionId] <- true
			c.mapTasksRemaining--
			c.mu.Unlock()
		} else if args.Action == Reduce {
			c.mu.Lock()
			c.reduceChannels[args.ActionId] <- true
			c.reduceTasksRemaining--
			c.mu.Unlock()
		}
	} else {
		if args.Action == Map {
			c.mu.Lock()
			c.mapChannels[args.ActionId] <- false
			c.mu.Unlock()
		} else if args.Action == Reduce {
			c.mu.Lock()
			c.reduceChannels[args.ActionId] <- false
			c.mu.Unlock()
		}
	}
	return nil
}

func (c *Coordinator) WorkerRequest(_ *struct{}, reply *WorkerRequestReply) error {
	c.mu.Lock()
	if len(c.mapTasksToAssign) > 0 {
		reply.Action = Map
		reply.ActionId = c.mapTasksToAssign[0]
		reply.ActionFilepath = c.files[c.mapTasksToAssign[0]]
		reply.TotalReduce = c.nReduce
		statusChan := make(chan bool)
		c.mapChannels[c.mapTasksToAssign[0]] = statusChan
		go c.WaitForMap(c.mapTasksToAssign[0])
		c.mapTasksToAssign = c.mapTasksToAssign[1:]
	} else if c.mapTasksRemaining != 0 {
		reply.Action = Wait
	} else if len(c.reduceTasksToAssign) > 0 {
		reply.Action = Reduce
		reply.ActionId = c.reduceTasksToAssign[0]
		statusChan := make(chan bool)
		c.reduceChannels[c.reduceTasksToAssign[0]] = statusChan
		go c.WaitForReduce(c.reduceTasksToAssign[0])
		c.reduceTasksToAssign = c.reduceTasksToAssign[1:]
	} else if c.reduceTasksRemaining != 0 {
		/* keep the workers around to make sure everything completes */
		reply.Action = Wait
	} else {
		/* all map & reduce tasks have finished, time to go home */
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

	c.mu.Lock()
	ret = c.reduceTasksRemaining == 0
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.mapTasksRemaining = len(files)
	c.reduceTasksRemaining = nReduce
	for i := range files {
		c.mapTasksToAssign = append(c.mapTasksToAssign, i)
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasksToAssign = append(c.reduceTasksToAssign, i)
	}
	c.mapChannels = make(map[int]chan bool)
	c.reduceChannels = make(map[int]chan bool)

	c.server()
	return &c
}
