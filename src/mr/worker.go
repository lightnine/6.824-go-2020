package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/rpc"
	"os"
	"sort"
	"time"

	"6.824-go-2020/tool/mrtool"
	"6.824-go-2020/tool/zlog"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int { return len(a) }

func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var logger = zlog.GetLogger()

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	logger.Infof("start worker")
	// Your worker implementation here.

	for {
		taskResp, err := CallMasterForTask()
		if err != nil {
			logger.Errorf("call master for task failed, err: %+v", err)
			time.Sleep(time.Second)
			continue
		}
		logger.Infof("the taskResp info: %+v", taskResp)
		if taskResp.Done {
			logger.Infof("all task done, so exit worker")
			return
		}
		switch taskResp.TaskType {
		case TaskTypeMap:
			intermediate := ExecuteMapTask(taskResp.Filename, mapf)
			for _, item := range intermediate {
				hash := ihash(item.Key) % taskResp.ReduceCount
				filename := mrtool.GetIntermediateFile(hash)
				WriterData(filename, item)
			}
			// 当前文件处理完成，发送消息
			taskResultReq := &TaskResultReq{
				Filename:  taskResp.Filename,
				TaskType:  TaskTypeMap,
				TaskState: TaskStateSuccess,
			}
			// FIXME 这里上报任务状态，如果失败怎么处理
			if _, err := CallWithTaskInfo(taskResultReq); err != nil {
				logger.Errorf("send map result failed, taskResultReq: %+v, err: %+v", taskResultReq, err)
				return
			}

		case TaskTypeReduce:
			// 读取中间文件，然后调用reduce函数，输出到最终结果中
			kva := ReadIntermediateFile(taskResp.Filename)
			reduceResult := ComputeKeyTimes(kva, reducef)
			outputName := mrtool.GetOutputFile(taskResp.Filename)

			WriterReduceOutput(outputName, reduceResult)

			// 上报任务消息
			taskResultReq := &TaskResultReq{
				Filename:  taskResp.Filename,
				TaskType:  TaskTypeReduce,
				TaskState: TaskStateSuccess,
			}
			// FIXME 这里上报任务状态，如果失败，当前还没有进行处理
			if _, err := CallWithTaskInfo(taskResultReq); err != nil {
				logger.Errorf("send reduce result failed, taskResultReq: %+v, err: %+v", taskResultReq, err)
				return
			}
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func WriterReduceOutput(filename string, kva []KeyValue) {
	ofile, _ := os.Create(filename)
	defer ofile.Close()
	for _, item := range kva {
		fmt.Fprintf(ofile, "%v %v\n", item.Key, item.Value)
	}
}

// ComputeKeyTimes 计算key的次数
func ComputeKeyTimes(intermediate []KeyValue, reducef func(string, []string) string) []KeyValue {
	kva := []KeyValue{}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		kva = append(kva, KeyValue{Key: intermediate[i].Key, Value: output})

		i = j
	}
	return kva
}

func WriterData(filename string, item KeyValue) {
	// TODO 这里需要优化，每次写入一个k-v对，都要打开一下文件
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer file.Close()
	if err != nil {
		logger.Errorf("open file failed, filename: %s, err: %+v", filename, err)
		return
	}
	enc := json.NewEncoder(file)
	if err := enc.Encode(&item); err != nil {
		logger.Errorf("write data to file failed, filename: %s, err: %+v", filename, err)
		return
	}
}

// ReadIntermediateFile 读取中间文件内容
func ReadIntermediateFile(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		logger.Error("open file failed, filename: %s, err: %+v", filename, err)
		os.Exit(1)
	}
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			logger.Errorf("json decode data failed, err: %+v", err)
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// ExecuteMapTask 获取map结果
func ExecuteMapTask(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	// 这里如果遇到错误，当前处理较为简单，直接退出当前进程
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		logger.Errorf("open file failed, filename: %s, err: +v", filename, err)
		os.Exit(1)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		logger.Errorf("cannot read file, filename :%s, err: %+v", filename, err)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	return intermediate
}

// CallMasterForTask 调用master获取任务信息
func CallMasterForTask() (*TaskResp, error) {
	req := &TaskReq{}
	reply := &TaskResp{}

	if callBool := call("Master.FetchTask", req, reply); !callBool {
		logger.Errorf("fetch task from master failed")
		return nil, errors.New("fetch task from master failed")
	}
	return reply, nil
}

// CallWithInfo 通知master 当前任务处理完成
func CallWithTaskInfo(taskReq *TaskResultReq) (*TaskResultResp, error) {
	reply := &TaskResultResp{}
	if ok := call("Master.ReportResult", taskReq, reply); !ok {
		logger.Errorf("report task result to master failed")
		return nil, errors.New("report task result to master failed")
	}
	return reply, nil
}

// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		logger.Fatalf("dialing failed, err: %+v", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	logger.Fatalf("call master failed, err: %+v", err)
	return false
}
