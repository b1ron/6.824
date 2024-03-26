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
	Map    = 0
	Reduce = 1
)

// Add your RPC definitions here.
type Args struct {
	PID   int
	Phase int
}

type Reply struct {
	Phase    int
	ID       int
	PID      int
	Filename string
	NReduce  int // The number of partitions denoted as R in the paper
	NMap     int
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
