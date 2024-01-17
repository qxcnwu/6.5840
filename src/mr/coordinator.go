package mr

import (
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var Mu = sync.Mutex{}
var Checker = true

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	TaskChannelReduce chan *Task     // 使用chan保证并发安全
	TaskChannelMap    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组
	taskChecker       TaskChecker    // 任务检查
}

// TaskChecker 内部保存的是id编号的任务的起始时间
type TaskChecker struct {
	MetaMap map[int]*Task
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state   State // 任务的状态
	TaskAdr *Task // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// Your code here -- RPC handlers for the worker to call.

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	go func() {
		err := http.Serve(l, nil)
		if err != nil {

		}
	}()
	// 额外启动一个协程进行超时任务的回收
	go func() {
		// 进行循环等待
		for {
			Mu.Lock()
			if !Checker {
				break
			}
			//time.Sleep(1 * time.Second)
			del := []int{}
			// 遍历全部对象
			for k, v := range c.taskChecker.MetaMap {
				now := time.Now().UnixNano() / 1e9
				if now-v.StartTime >= 9 {
					del = append(del, k)
					// 重新载入
					if c.DistPhase == MapPhase {
						//fmt.Printf("Map任务超时，重新注入:%+v\n", v)
						// add meta data
						c.taskMetaHolder.MetaMap[v.TaskId].state = Waiting
						// 添加到管道
						c.TaskChannelMap <- v
					} else if c.DistPhase == ReducePhase {
						//fmt.Printf("Reduce任务超时，重新注入:%+v", v)
						// add meta data
						c.taskMetaHolder.MetaMap[v.TaskId].state = Waiting
						// 添加到管道
						c.TaskChannelReduce <- v
					}
					// 需要删除当前这个任务
				}
			}
			for _, k := range del {
				// 删除当前超时的任务
				//fmt.Println("删除超时任务-", k)
				delete(c.taskChecker.MetaMap, k)
			}
			Mu.Unlock()
		}
		//fmt.Println("检查协程-结束等待!!")
	}()
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	Mu.Lock()
	defer Mu.Unlock()
	if c.DistPhase == AllDone {
		//fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		Checker = false
		return true
	} else {
		return false
	}
}

// init map
func (c *Coordinator) makeMapTasks() {
	for _, v := range c.files {
		// make task
		task := Task{
			TaskType:   MapTask,
			TaskId:     c.TaskId,
			FileName:   v,
			ReducerNum: c.ReducerNum,
		}
		// init task state
		taskMetaInfo := TaskMetaInfo{
			TaskAdr: &task,
			// wait for doing
			state: Waiting,
		}
		// add meta data
		c.taskMetaHolder.MetaMap[c.TaskId] = &taskMetaInfo
		// id increase
		c.TaskId++
		// 添加到管道
		c.TaskChannelMap <- &task
	}
}

// 获取reduce处理的文件
func getFiles(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

// 制作reducer的任务
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		// make task
		task := Task{
			TaskType:   ReduceTask,
			TaskId:     c.TaskId,
			FileNames:  getFiles(i),
			ReducerNum: c.ReducerNum,
			MapNum:     len(c.files),
		}
		// init task state
		taskMetaInfo := TaskMetaInfo{
			TaskAdr: &task,
			// wait for doing
			state: Waiting,
		}
		// add meta data
		c.taskMetaHolder.MetaMap[c.TaskId] = &taskMetaInfo
		// id increase
		c.TaskId++
		// 添加到管道
		c.TaskChannelReduce <- &task
	}
}

// 更改下一阶段的状态
func (c *Coordinator) changeTaskState() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// PollTask pull task
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// 上锁
	Mu.Lock()
	// 结束后解锁
	defer Mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			// 从这里开始拉取任务
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				// 需要记录任务的起始时间
				(*reply).StartTime = time.Now().UnixNano() / 1e9
				// 需要记录任务的起始时间
				c.taskChecker.MetaMap[reply.TaskId] = reply
				if c.taskMetaHolder.judgeState(reply.TaskId) {
					//fmt.Printf("map拉取任务:%+v\n", reply)
				}
			} else {
				reply.TaskType = WaittingTask
				// 查询是否进行任务转换
				if c.taskMetaHolder.checkAllTask() {
					// 更改当前的状态
					c.changeTaskState()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			// 从这里开始拉取任务
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				// 需要记录任务的起始时间
				(*reply).StartTime = time.Now().UnixNano() / 1e9
				// 需要记录任务的起始时间
				c.taskChecker.MetaMap[reply.TaskId] = reply
				if c.taskMetaHolder.judgeState(reply.TaskId) {
					//fmt.Printf("reduce拉取任务:%+v\n", reply)
				}
			} else {
				reply.TaskType = WaittingTask
				// 查询是否进行任务转换
				if c.taskMetaHolder.checkAllTask() {
					// 更改当前的状态
					c.changeTaskState()
				}
				return nil
			}
		}
	case AllDone:
		{
			// 默认值
			reply.TaskType = ExitTask
		}
	default:
		panic("Phase undefined")
	}
	return nil
}

// 检查是否map或者reduce完成
func (t *TaskMetaHolder) checkAllTask() bool {
	mapdone := 0
	mapundone := 0
	reducedone := 0
	reduceundone := 0
	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapdone++
			} else {
				//fmt.Printf("mapundone task:%+v\n", v.TaskAdr.TaskId)
				mapundone++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reducedone++
			} else {
				//fmt.Printf("reduceundone task:%+v\n", v.TaskAdr.TaskId)
				reduceundone++
			}
		}
	}
	//fmt.Printf("mapdone:%d \t mapundone:%d \t reducedone:%d \t reduceundone:%d\n", mapdone, mapundone, reducedone, reduceundone)
	// map全部完成
	if mapdone != 0 && mapundone == 0 && reduceundone == 0 && reducedone == 0 {
		return true
	}
	// reduce全部完成
	if reducedone != 0 && reduceundone == 0 {
		return true
	}
	return false
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	return true
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReducerNum:        nReduce,
		files:             files,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo), // 任务的总数应该是files + Reducer的数量
		},
		taskChecker: TaskChecker{MetaMap: make(map[int]*Task)},
	}

	// Your code here.
	// init all tasks
	c.makeMapTasks()

	c.server()
	return &c
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	Mu.Lock()
	defer Mu.Unlock()
	switch args.TaskType {
	case MapTask:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
			// 任务完成需要删除对应的起始时间
			delete(c.taskChecker.MetaMap, args.TaskId)
			//prevent a duplicated work which returned from another worker
			if ok && meta.state == Working {
				meta.state = Done
				//fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
			} else {
				//fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
			}
			break
		}
	default:
		{
			panic("The task type undefined ! ! !")
		}
	}
	return nil
}
