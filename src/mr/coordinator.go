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
	tasks   map[int]task
	nReduce int

	mu    sync.Mutex // guards files and nMap
	files []string
	nMap  int
}

type task struct {
	id   int
	file string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Map(args *MapArgs, reply *MapReply) error {
	if _, ok := c.tasks[args.Id]; !ok {
		c.mu.Lock()
		c.tasks[args.Id] = task{id: c.nMap, file: c.files[c.nMap]}
		c.nMap++
		c.mu.Unlock()

		reply.File = c.tasks[args.Id].file
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
	c.tasks = make(map[int]task)
	c.nReduce = nReduce
	c.files = files
	c.nMap = 0

	c.server()
	return &c
}
