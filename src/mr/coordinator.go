package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const TIMEOUT = 20 * time.Second

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	phase   Phase
	tasks   []Task
	nReduce int
	nMap    int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(request *AssignRequest, response *AssignResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerID := request.WorkerID

	for i := range c.tasks {
		t := &c.tasks[i]
		isIdle := t.Phase == c.phase && t.Status == Idle
		isTimeout := t.Phase == c.phase && t.Status == InProgress && time.Since(t.startTime) > TIMEOUT
		if isIdle || isTimeout {
			t.Status = InProgress
			t.startTime = time.Now()
			response.WorkerID = workerID
			response.TaskID = t.TaskID
			response.Task = *t
			response.TaskPhase = t.Phase
			response.NReduce = c.nReduce
			response.TasksStatus = TaskAssigned
			response.NMap = c.nMap
			return nil
		}
	}

	if !c.allTasksDone() {
		response.TasksStatus = TaskWait
		return nil
	}

	if c.phase == MapPhase {
		// map phase just finished; reduce tasks are now available
		c.phase = ReducePhase
		response.TasksStatus = TaskWait
	} else {
		// reduce phase finished too; the whole job is done
		response.TasksStatus = AllDone
	}

	return nil
}

func (c *Coordinator) ReportTask(request *TaskReport, response *TaskReportAck) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.tasks {
		t := &c.tasks[i]
		if t.Phase == request.Phase && t.TaskID == request.TaskID {
			t.Status = Done
			break
		}
	}
	response.TaskID = request.TaskID
	response.Phase = request.Phase
	response.Acknowledged = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == ReducePhase && c.allTasksDone()
}

func (c *Coordinator) allTasksDone() bool {
	for _, t := range c.tasks {
		if t.Phase == c.phase && t.Status != Done {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	// Your code here.
	c := Coordinator{
		phase:   MapPhase,
		nReduce: nReduce,
		nMap:    len(files),
	}
	// map tasks
	numTasks := len(files)
	c.tasks = make([]Task, numTasks)
	for i := range numTasks {
		c.tasks[i] = Task{
			Phase:  MapPhase,
			Status: Idle,
			TaskID: i,
			File:   files[i],
		}
	}

	// reduce tasks
	for i := range nReduce {
		c.tasks = append(c.tasks, Task{
			Phase:  ReducePhase,
			Status: Idle,
			TaskID: i,
		})
	}
	c.server(sockname)
	return &c
}
