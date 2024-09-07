package zlog_test

import (
	"testing"

	"6.824-go-2020/tool/zlog"
)

func TestLog(t *testing.T) {
	log := zlog.GetLogger()
	log.Infof("info, %s", "info")
	log.Warnf("warn, %s", "warn")
	log.Error("error, %s", "error")
	log.Fatalf("fatalf, %s", "fatals")
}
