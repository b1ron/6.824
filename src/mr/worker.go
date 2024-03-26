package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var nReduce int = 10 // TODO: set this global variable properly via an RPC call

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		reply, ok := request()
		if !ok {
			break
		}

		switch reply.Phase {
		case Map:
			err := doMap(reply.Filename, reply.NReduce, mapf)
			if err != nil {
				log.Fatalf("doMap failed") // TODO: handle errors properly
			}
		case Reduce:
		}
	}

}

func request() (*Reply, bool) {
	args := &Args{PID: os.Getpid()} // for registering the worker
	reply := &Reply{}
	ok := call("Coordinator.Request", args, reply)
	if ok {
		return reply, true
	}
	return nil, false
}

// tell the coordinator that the task is done and the worker is ready for another task
func complete(phase int, pid int) {
	args := &Args{
		PID:   pid,
		Phase: phase,
	}
	reply := &Reply{}
	call("Coordinator.Complete", args, reply) // TODO
}

func doMap(filename string, nReduce int, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file.Name())
	}
	file.Close()

	kva := mapf(file.Name(), string(content))
	sort.Sort(ByKey(kva))

	// intermediate key-value pairs partitioned into R buckets
	buckets := make(map[int][]KeyValue)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	mapTask := 0
	for reduceTask, kv := range buckets {
		oname := fmt.Sprintf("mr-%v-%v", mapTask, reduceTask)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		mapTask++

		enc := json.NewEncoder(ofile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}

		ofile.Close()
	}
	return nil
}

func doReduce(reduceTask int, reducef func(string, []string) string) error {
	var intermediate = []KeyValue{}

	for mapTask := 0; mapTask < nReduce; mapTask++ {
		filename := fmt.Sprintf("mr-%v-%v", mapTask, reduceTask)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		mapTask++

		var kv []KeyValue
		dec := json.NewDecoder(file)
		for {
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv...)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reduceTask)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", ofile)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
