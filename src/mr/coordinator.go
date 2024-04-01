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
	 * When a task id is assigned it's popped off the queue. if a task
	 * doesn't complete in time the task id is pushed back onto the queue
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

/* WaitForMap and WaitForReduce are kicked off each time a map & reduce task is assigned
 * in WorkerRequest
 * WorkerRequest will create a channel to communicate done status
 * WaitForMap/WaitForReduce will start a 10second timer at the beginning
 * Three possible outcomes:
 *	- timer completes first, in which case the task is added back to the queue
 *	- done channel first first (in which case clear out the timer channel)
 *		- done is false, in which case add the task back to the queue
 *		- done is true - do nothing (WorkerStatusUpdate decrements the map/reduceTasksRemaining count)
 * Regardless of the three outcomes, always delete the channel from the map &
 * close the channel
 */
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

/* WorkerStatusUpdate is an RPC entry point for the workers to report back status
 * This method will communicate over the existing taskId channel to give either True
 * for done or false for error to the waiting WaitForMap/Reduce method
 * If status is Done decremement the appropriate map/reduceTasksRemaining count
 */
func (c *Coordinator) WorkerStatusUpdate(args *WorkerStatusArg, _ *struct{}) error {
	/* if status indicates done send a true value over the channel and decrement the number
	 * of tasks remaining. otherwise, send false over channel
	 * the go routine waiting for the timer is reponsible for putting the task back onto
	 * worker remaining queue
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

/* Main entry point for Worker threads. A single WorkerRequestReply struct is used for
 * all possible tasks
 * Assign tasks as appropriate, working our way through:
 * 	1. map tasks remaining to be assigned
 *	2. map tasks remaining to complete
 *	3. reduce tasks remaining to be assigned
 *	4. reduce tasks remaining to complete
 */
func (c *Coordinator) WorkerRequest(_ *struct{}, reply *WorkerRequestReply) error {
	c.mu.Lock()
	if len(c.mapTasksToAssign) > 0 {
		/* if we have map tasks, ie input files, left to process give the worker
		 * one of those
		 */
		reply.Action = Map
		reply.ActionId = c.mapTasksToAssign[0]
		reply.ActionFilepath = c.files[c.mapTasksToAssign[0]]
		reply.TotalReduce = c.nReduce
		statusChan := make(chan bool)
		c.mapChannels[c.mapTasksToAssign[0]] = statusChan
		go c.WaitForMap(c.mapTasksToAssign[0])
		c.mapTasksToAssign = c.mapTasksToAssign[1:]
	} else if c.mapTasksRemaining != 0 {
		/* if we have no more map tasks to assign but there's still map tasks in flight
		 * tell the worker to chill
		 */
		reply.Action = Wait
	} else if len(c.reduceTasksToAssign) > 0 {
		/* once all map tasks have complete start assigning reduce tasks */
		reply.Action = Reduce
		reply.ActionId = c.reduceTasksToAssign[0]
		statusChan := make(chan bool)
		c.reduceChannels[c.reduceTasksToAssign[0]] = statusChan
		go c.WaitForReduce(c.reduceTasksToAssign[0])
		c.reduceTasksToAssign = c.reduceTasksToAssign[1:]
	} else if c.reduceTasksRemaining != 0 {
		/* if no more reduce tasks to assign keep the worker around in case a task in
		 * flight errors or times out. that worker that timed out maybe crashed so we
		 * need valid workers around still
		 */
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
/* Simply checks if any reduce tasks are reminaing to complete */
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
