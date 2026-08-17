package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		request := AssignRequest{WorkerID: os.Getpid()}
		response := AssignResponse{}
		ok := call("Coordinator.RequestTask", &request, &response)
		if !ok {
			log.Printf("Cannot connect to coordinator")
			return
		}

		switch response.TasksStatus {
		case AllDone:
			log.Printf("All tasks done, exiting")
			return
		case TaskWait:
			log.Printf("No task available, waiting...")
			time.Sleep(1 * time.Second)
			continue
		}

		var err error
		if response.TaskPhase == MapPhase {
			err = Map(mapf, response.Task, response.NReduce)
		} else {
			err = Reduce(reducef, response.Task, response.NMap)
		}
		if err != nil {
			log.Printf("Cannot run task: %v", err)
			continue
		}

		report := TaskReport{
			TaskID: response.TaskID,
			Phase:  response.TaskPhase,
		}
		ack := TaskReportAck{}
		if ok := call("Coordinator.ReportTask", &report, &ack); !ok {
			log.Printf("Cannot connect to coordinator")
			return
		}
	}
}

func Map(mapf func(string, string) []KeyValue, task Task, nReduce int) error {
	filename := task.File
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Cannot open file: %v", err)
		return err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("Cannot read file: %v", err)
		return err
	}
	kva := mapf(filename, string(content))
	buckets := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}
	for i, b := range buckets {
		tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Printf("Cannot create temporary file: %v", err)
			return err
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range b {
			if err := enc.Encode(&kv); err != nil {
				tmpFile.Close()
				return err
			}
		}
		tmpFile.Close()

		finalName := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		if err := os.Rename(tmpFile.Name(), finalName); err != nil {
			log.Printf("Cannot rename temporary file: %v", err)
			return err
		}
	}
	return nil
}

func Reduce(reducef func(string, []string) string, task Task, nMap int) error {
	taskID := task.TaskID
	var kva []KeyValue
	for i := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", i, taskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Cannot open file: %v", err)
			return err
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	ofile, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		log.Printf("Cannot create temporary file: %v", err)
		return err
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	oname := fmt.Sprintf("mr-out-%d", taskID)
	if err := os.Rename(ofile.Name(), oname); err != nil {
		log.Printf("Cannot rename temporary file: %v", err)
		return err
	}
	return nil

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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
