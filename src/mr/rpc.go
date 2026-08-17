package mr

import "time"

//
// RPC definitions.
//
// remember to capitalize all names.
//

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
type Phase int
type Status int
type TaskSignal int

const (
	MapPhase Phase = iota
	ReducePhase
)
const (
	Idle Status = iota
	InProgress
	Done
)
const (
	TaskAssigned TaskSignal = iota
	TaskWait
	AllDone
)

type Task struct {
	Phase     Phase
	Status    Status
	TaskID    int
	File      string
	startTime time.Time
}

type AssignRequest struct {
	WorkerID int
}

type AssignResponse struct {
	WorkerID    int
	TaskPhase   Phase
	TaskID      int
	Task        Task
	NReduce     int
	TasksStatus TaskSignal
	NMap        int
}

type TaskReport struct {
	Phase  Phase
	TaskID int
}

type TaskReportAck struct {
	Phase        Phase
	TaskID       int
	Acknowledged bool
}
