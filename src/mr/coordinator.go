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
	// Your definitions here.
	mu          sync.Mutex // guards the fields below
	mapTasks    []task
	reduceTasks []task

	nReduce int
	done    chan bool
}

type task struct {
	id    int
	state int // 0: not started, 1: in progress, 2: done
	file  string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Map(args *MapArgs, reply *MapReply) error {
	if args.Done {
		c.mapTasks[reply.ID].state = 2 // another crappy lock?
		return nil
	}

	// farmout tasks to workers
	for i, t := range c.mapTasks {
		if t.state > 0 {
			continue
		}

		c.mu.Lock()
		reply.ID = i
		reply.File = t.file
		reply.NReduce = c.nReduce

		t.state = 1
		c.mu.Unlock()
		return nil
	}
	return nil
}

func (c *Coordinator) Reduce() error {
	// TODO: implement
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// TODO: finish this
	for _, t := range c.mapTasks {
		if t.state != 2 {
			continue
		}
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// the coordinator will keep track of the state of each task
	// the coordinator can re-assign tasks to workers if they fail to complete them within a certain time frame (e.g. 10 seconds)
	c.mapTasks = []task{}
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, task{i, 0, file})
	}
	c.nReduce = nReduce

	c.server()
	return &c
}
