package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	keepFlag := true
	for keepFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				//fmt.Printf("Get map task:%+v", task)
				DoMapTask(mapf, &task)
				callDone(task.TaskId)
			}
		case WaittingTask:
			{
				//fmt.Println("waitting for task")
				time.Sleep(time.Second)
			}
		case ReduceTask:
			{
				//fmt.Printf("Get reduce task:%+v", task)
				DoReducerTask(reducef, &task)
				callDone(task.TaskId)
			}
		case ExitTask:
			{
				//fmt.Printf("exit task")
				keepFlag = false
			}
		default:
			panic("unhandled default case")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func GetTask() Task {
	args := TaskArgs{}
	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("%+v \n", reply)
	} else {
		//fmt.Printf("call failed!\n")
	}
	return reply
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	// reducer的数量
	rn := task.ReducerNum

	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	// 写入文件
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(kv)
			if err != nil {
				return
			}
		}
		err := ofile.Close()
		if err != nil {
			return
		}
	}
}

func DoReducerTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	//reduceFileNum := response.TaskId - response.MapNum + 1
	intermediate := shuffle(response.FileNames)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
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
		_, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return
		}
		i = j
	}
	err = tempFile.Close()
	if err != nil {
		return
	}
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	err = os.Rename(tempFile.Name(), fn)
	if err != nil {
		return
	}
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 洗牌方法，得到一组排序好的kv数组
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err := file.Close()
		if err != nil {
			return nil
		}
	}
	sort.Sort(ByKey(kva))
	return kva
}

func callDone(id int) Task {
	args := Task{TaskId: id}
	reply := Task{TaskId: id}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		//fmt.Println(reply)
	} else {
		//fmt.Printf("call failed!\n")
	}
	return reply
}

// CallExample
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
		//fmt.Printf("%+v\n", reply.Y)
	} else {
		//fmt.Printf("call failed!\n")
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
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {

		}
	}(c)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}
