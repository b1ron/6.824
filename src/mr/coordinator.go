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
	mapTasks    []*task
	reduceTasks []*task

	nReduce int
	nMap    int
}

type task struct {
	id    int
	state int // 0: not started, 1: in progress, 2: done
	file  string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Map(args *MapArgs, reply *MapReply) error {
	// farmout tasks to workers
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, t := range c.mapTasks {
		if t.state > 0 {
			continue
		}

		reply.ID = i
		reply.Filename = t.file
		reply.NReduce = c.nReduce

		t.state = 1
		return nil
	}
	return nil
}

func (c *Coordinator) Reduce(args *ReduceArgs, reply *ReduceReply) error {
	// or
	if c.nMap == 0 {
		c.mu.Lock()
		defer c.mu.Unlock()
		for i, t := range c.reduceTasks {
			if t.state > 0 {
				continue
			}
			reply.ID = i
			c.reduceTasks[i].state = 1
		}
		return nil
	}

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
	c.mapTasks = []*task{}
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, &task{i, 0, file})
		c.nMap++
	}
	c.nReduce = nReduce
	c.reduceTasks = make([]*task, nReduce)

	c.server()
	return &c
}
