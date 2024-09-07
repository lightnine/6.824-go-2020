package mrtool

// map reduce 工具包

import (
	"fmt"
	"strings"
)

const (
	dir = "mr-tmp"
)

// GetIntermediateFile 获取中间文件名称
func GetIntermediateFile(index int) string {
	return fmt.Sprintf("%s/mr-inter-%v", dir, index)
}

// GetOutputFile 获取输出文件名称
func GetOutputFile(filename string) string {
	return strings.Replace(filename, "inter", "output", 1)
}
