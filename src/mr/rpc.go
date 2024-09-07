package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	TaskTypeMap    = "map"
	TaskTypeReduce = "reduce"

	TaskStateSuccess = "success"
	TaskStateFailure = "failure"
	TaskStateRunning = "running"
)

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskReq struct {
}

type TaskResp struct {
	Filename string
	TaskType string

	// Done 表示任务是否结束
	Done bool

	// ReduceCount 表示 reduce 任务的数量
	ReduceCount int
}

// TaskResultReq 用于worker向master汇报任务执行结果
type TaskResultReq struct {
	// Filename 表示当前任务处理的文件名称
	Filename string

	// TaskType map or reduce 表示当前任务类型
	TaskType string

	// TaskState 表示任务状态
	TaskState string
}

type TaskResultResp struct {
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
