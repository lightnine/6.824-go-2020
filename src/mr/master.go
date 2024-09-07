package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"

	"6.824-go-2020/tool/mrtool"
	"6.824-go-2020/tool/zlog"
)

const (
	MapStage    = "map"
	ReduceStage = "reduce"

	FileStateInit    = "init"
	FileStateRunning = "running"
	FileStateDone    = "done"
)

var log = zlog.GetLogger()

type Master struct {
	// Your definitions here.
	Lock *sync.Mutex

	// 需要处理的文件列表

	MapFiles []MapFileInfo

	// NReduce reduce任务数量
	NReduce int

	// 中间文件列表
	ReduceFiles []ReduceFileInfo
}

// MapFileInfo 需要进行map操作的文件信息
type MapFileInfo struct {
	Filename string

	State string
}

// ReduceFileInfo 需要进行reduce操作的文件信息
type ReduceFileInfo struct {
	Filename string
	State    string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// FetchTask 获取任务信息
func (m *Master) FetchTask(args *TaskReq, reply *TaskResp) error {
	log.Infof("get req from worker(fetch task), req: %+v", args)

	m.Lock.Lock()
	defer m.Lock.Unlock()

	mapIndex := GetMapFileIndex(m.MapFiles)
	if mapIndex != -1 {
		m.MapFiles[mapIndex].State = FileStateRunning
		reply.Filename = m.MapFiles[mapIndex].Filename
		reply.TaskType = TaskTypeMap
		reply.Done = false
		reply.ReduceCount = m.NReduce
		return nil
	}

	reduceIndex := GetReduceFileIndex(m.ReduceFiles)
	if reduceIndex != -1 {
		m.ReduceFiles[reduceIndex].State = FileStateRunning

		reply.Filename = m.ReduceFiles[reduceIndex].Filename
		reply.TaskType = TaskTypeReduce
		reply.Done = false
		return nil
	}
	reply.Done = true
	return nil
}

func GetMapFileIndex(files []MapFileInfo) int {
	for index, file := range files {
		switch file.State {
		case FileStateInit:
			return index
		case FileStateRunning, FileStateDone:
			continue
		}
	}
	return -1
}

func GetReduceFileIndex(files []ReduceFileInfo) int {
	for index, file := range files {
		switch file.State {
		case FileStateInit:
			return index
		case FileStateRunning, FileStateDone:
			continue
		}
	}
	return -1
}

// ReportResult 上报任务结果
func (m *Master) ReportResult(args *TaskResultReq, reply *TaskResultResp) error {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	stage := args.TaskType
	switch stage {
	case TaskTypeMap:
		m.UpdateFileState(args.Filename, m.MapFiles)
	case TaskTypeReduce:
		m.UpdateFileState(args.Filename, m.ReduceFiles)
	}
	return nil
}

func (m *Master) UpdateFileState(filename string, fileInfo any) {
	switch fileInfo.(type) {
	case []MapFileInfo:
		mfiles, _ := fileInfo.([]MapFileInfo)
		for i, file := range mfiles {
			if file.Filename == filename {
				mfiles[i].State = FileStateDone
			}
		}
	case []ReduceFileInfo:
		rfiles, _ := fileInfo.([]ReduceFileInfo)
		for i, file := range rfiles {
			if file.Filename == filename {
				rfiles[i].State = FileStateDone
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	count := 0
	for _, file := range m.ReduceFiles {
		if file.State == FileStateDone {
			count++
			continue
		}
		return false
	}
	if count == len(m.ReduceFiles) {
		ret = true
	}

	return ret
}

func DeleteLastFiles() {
	for i := range 10 {
		os.Remove("mr-tmp/mr-inter-" + strconv.Itoa(i))
		os.Remove("mr-tmp/mr-output-" + strconv.Itoa(i))
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	log.Infof("start master, files: %s, nReduce: %d", strings.Join(files, ", "), nReduce)

	// delete last result files
	DeleteLastFiles()

	m := Master{}
	m.NReduce = nReduce
	m.Lock = new(sync.Mutex)

	// Your code here.
	mfiles := []MapFileInfo{}
	for _, file := range files {
		mfiles = append(mfiles, MapFileInfo{
			Filename: file,
			State:    FileStateInit,
		})
	}
	m.MapFiles = mfiles

	rfiles := []ReduceFileInfo{}
	for i := 0; i < nReduce; i++ {
		rfiles = append(rfiles, ReduceFileInfo{
			Filename: mrtool.GetIntermediateFile(i),
			State:    FileStateInit,
		})
	}
	m.ReduceFiles = rfiles

	m.server()
	return &m
}
