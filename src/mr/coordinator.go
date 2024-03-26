package mr

import (
	"fmt"
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

			fmt.Printf("assigning map task %d to worker %d\n", i, args.PID)

			c.register(args.PID, t, Map)

			reply.Phase = Map
			reply.ID = i
			reply.PID = args.PID
			reply.Filename = t.file
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap

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

		fmt.Printf("assigning reduce task %d to worker %d\n", i, args.PID)

		c.register(args.PID, t, Reduce)

		reply.ID = i
		t.state = 1
	}
	return nil
}

func (c *Coordinator) Complete(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	w := c.workers[args.PID]
	switch w.phase {
	case Map:
		c.mapTasks[w.task.id].state = 2
		c.nMap--
	case Reduce:
		c.reduceTasks[w.task.id].state = 2
	}
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
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &task{state: 0}
	}
	c.workers = make(map[int]*worker)

	c.server()
	return &c
}
