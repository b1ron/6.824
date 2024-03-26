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
	// Your definitions here.
	mu          sync.Mutex // guards the fields below
	mapTasks    []*task
	reduceTasks []*task
	workers     map[int]*worker

	nReduce int
	nMap    int
}

type worker struct {
	phase int
	pid   int
	t     time.Time
	task  *task
}

type task struct {
	id    int
	state int // 0: not started, 1: in progress, 2: done
	file  string
}

func (c *Coordinator) register(pid int, task *task, phase int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.workers[pid] = &worker{
		phase,
		pid,
		time.Now(),
		task,
	}
}

// XXX it's up to the caller to lock before calling this
func (c *Coordinator) unregister(w *worker) {
	switch w.phase {
	case Map:
		c.mapTasks[w.task.id].state = 0
	case Reduce:
		c.reduceTasks[w.task.id].state = 0
	}
	delete(c.workers, w.pid)
}

func (c *Coordinator) workerFailed() (bool, *worker) {
	for _, w := range c.workers {
		if time.Since(w.t) > 10*time.Second {
			return true, w
		}
	}
	return false, nil
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Request(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if failed, w := c.workerFailed(); failed {
		c.unregister(w)
	}

	if c.nMap > 0 {
		// farmout map tasks
		for i, t := range c.mapTasks {
			if t.state > 0 {
				continue
			}

			reply.Phase = Map

			c.register(args.PID, t, Map)

			reply.NMap = c.nMap
			reply.ID = i
			reply.Filename = t.file
			reply.NReduce = c.nReduce

			t.state = 1
			return nil
		}
	}

	// reduce tasks
	reply.Phase = Reduce
	for i, t := range c.reduceTasks {
		if t.state > 0 {
			continue
		}

		c.register(args.PID, t, Reduce)

		reply.ID = i
		c.reduceTasks[i].state = 1
	}
	return nil
}

func (c *Coordinator) Complete(args *Args, reply *Reply) error {
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
		t := &task{
			id:    i,
			state: 0,
			file:  file,
		}
		c.mapTasks = append(c.mapTasks, t)
		c.nMap++
	}
	c.nReduce = nReduce
	c.reduceTasks = make([]*task, nReduce)
	c.workers = make(map[int]*worker)

	c.server()
	return &c
}
