package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	ret := NewTask(mapf, reducef)
	for ret {
		ret =  NewTask(mapf, reducef)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func NewTask(mapf func(string, string) []KeyValue,
reducef func(string, []string) string) bool {
	args := NewTaskArgs{}
	args.WorkerId = os.Getpid()
	reply := NewTaskReply{}
	call("Master.NewTask", &args, &reply)
	if !reply.NewTask {
		return false
	}

	if reply.TaskType == 0 {
		intermediate := []KeyValue{}
		filename := reply.FileName
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	
		s := make([]ByKey, reply.ReduceCount)
	
		for _, kv := range intermediate {
			hashKey := ihash(kv.Key) % reply.ReduceCount
			s[hashKey] = append(s[hashKey], kv)
		}
	
		for i, ss := range s {
			filename =  fmt.Sprintf("mr-out-%d-%d.json", reply.TaskNumber, i)
			file, _ = os.Create(filename)
			enc := json.NewEncoder(file)
			for _, kv := range ss {
				_ = enc.Encode(&kv)
			}
		}

		doneArgs := TaskDoneArgs{}
		doneArgs.TaskType = 0
		doneArgs.TaskNumber = reply.TaskNumber
		doneReply := TaskDoneReply{}
		call("Master.TaskDone", &doneArgs, &doneReply)
	}else if reply.TaskType == 1 {

	}
	return true
}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
