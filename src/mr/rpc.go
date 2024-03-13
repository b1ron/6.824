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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MapArgs struct{ Done bool }

type MapReply struct {
	ID       int
	Filename string
	NReduce  int // The number of partitions denoted as R in the paper
}

// TODO: reduce RPC definitions
type ReduceArgs struct{ Done bool }

type ReduceReply struct {
	NMap int
	ID   int
	Done chan bool // indicates when all map tasks are done
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
