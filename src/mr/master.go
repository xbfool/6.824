package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "time"

type Task struct {
	Status int //0. not start, 1. started, 2.succeed, 3. failed
	FileName string
	StartTime time.Time
	WorkerId int
	TaskNumber int
}

type Master struct {
	// Your definitions here.
	MapTasks [] Task
	ReduceTasks []Task
	ReduceCount int
	MapDone bool
	ReduceDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	if args.TaskType == 0 {
		m.MapTasks[args.TaskNumber].Status = 2
	}else {
		m.ReduceTasks[args.TaskNumber].Status = 2
	}
	reply.Err = 0
	return nil
}

func (m *Master) NewTask(args *NewTaskArgs, reply *NewTaskReply) error {
	fmt.Printf("GetTaskId %v\n", args.WorkerId)
	if !m.MapDone {
		for i , val := range m.MapTasks {
			if val.Status == 0 || val.Status == 3 {
				val.WorkerId = args.WorkerId
				val.Status = 1
				val.StartTime = time.Now()
				val.TaskNumber = i
				reply.FileName = val.FileName
				reply.NewTask = true
				reply.TaskNumber = i
				reply.ReduceCount = m.ReduceCount
				reply.TaskType = 0
				return nil
			} else if val.Status == 1 {
				if time.Now().Sub(val.StartTime)>= 10000000 {
					val.WorkerId = args.WorkerId
					val.Status = 1
					val.StartTime = time.Now()
					reply.FileName = val.FileName
					reply.NewTask = true
					reply.TaskNumber = i
					reply.ReduceCount = m.ReduceCount
					reply.TaskType = 0
					return nil
				}
			}
		  }
		m.MapDone = true
	}else if !m.ReduceDone {
		for i , val := range m.ReduceTasks {
			if val.Status == 0 || val.Status == 3 {
				val.WorkerId = args.WorkerId
				val.Status = 1
				val.StartTime = time.Now()
				val.TaskNumber = i
				reply.TaskType = 1
				reply.NewTask = true
				reply.TaskNumber = i
				reply.ReduceCount = m.ReduceCount
				return nil
			} else if val.Status == 1 {
				if time.Now().Sub(val.StartTime)>= 10000000 {
					val.WorkerId = args.WorkerId
					val.Status = 1
					val.StartTime = time.Now()
					reply.TaskType = 1
					reply.NewTask = true
					reply.TaskNumber = i
					reply.ReduceCount = m.ReduceCount
					return nil
				}
			}
		  }
		  m.ReduceDone = true
	}

	reply.NewTask = false
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.ReduceDone

	// Your code here.

	
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.ReduceCount = nReduce
	// Your code here.
	var mapTasks [] Task
	for i , val := range files {
		var task Task
		task.Status = 0
		task.FileName = val
		task.TaskNumber = i
		mapTasks = append(mapTasks, task)
	  }
	  var reduceTasks [] Task
	for i := 0; i < nReduce; i++ {
		var task Task
		task.Status = 0
		task.FileName = ""
		task.TaskNumber = i
		reduceTasks = append(reduceTasks, task)
	}
	m.MapTasks = mapTasks
	m.ReduceTasks = reduceTasks
	m.MapDone = false
	m.ReduceDone = false
	m.server()
	return &m
}
