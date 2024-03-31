package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	Map       int = 0
	Reduce    int = 1
	Wait      int = 2
	Terminate int = 3
)

type WorkerRequestReply struct {
	Action         int
	ActionFilepath string
	ActionId       int
	TotalReduce    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
