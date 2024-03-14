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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		mapReply, ok := CallMap()
		if !ok {
			log.Fatalf("call failed!\n")
		}

		if mapReply.Filename == "" {
			break
		}

		doMap(mapReply.Filename, mapReply.NReduce, mapf)
	}

	fmt.Println("map phase done")
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
	intermediateBuckets := make(map[int][]KeyValue)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		intermediateBuckets[bucket] = append(intermediateBuckets[bucket], kv)
	}

	mapTaskN := 0
	for reduceTaskN, kv := range intermediateBuckets {
		oname := fmt.Sprintf("mr-%v-%v", mapTaskN, reduceTaskN)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		mapTaskN++

		enc := json.NewEncoder(ofile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}

		ofile.Close()
	}
	return nil
}

func CallMap() (*MapReply, bool) {
	args := &MapArgs{}
	reply := &MapReply{}

	ok := call("Coordinator.Map", args, reply)
	if ok {
		return reply, true
	}
	return nil, false
}

// TODO: implement
func CallReduce() (*ReduceReply, bool) {
	args := &ReduceArgs{}
	reply := &ReduceReply{}

	ok := call("Coordinator.Reduce", args, reply)
	if ok {
		return reply, true
	}
	return nil, false
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
