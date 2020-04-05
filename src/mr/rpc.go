package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type NewTaskArgs struct {
	WorkerId int
}

type NewTaskReply struct {
	NewTask bool
	TaskType int //0: map, 1: reduce, 2:wait
	FileName string
	TaskNumber int
	ReduceCount int
	MapCount int
}

type TaskDoneArgs struct {
	TaskType int
	TaskNumber int
}

type TaskDoneReply struct {
	Err int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
