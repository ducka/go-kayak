package observe

import (
	"time"
)

type MetricsMonitor interface {
	Incr(name string, value float64, tags ...string)
	Timing(name string, value time.Duration, tags ...string)
}

type NilMetricsMonitor struct{}

func (*NilMetricsMonitor) Incr(name string, value float64, tags ...string)         {}
func (*NilMetricsMonitor) Timing(name string, value time.Duration, tags ...string) {}

// TODO: Pare back this interface as required.
type Logger interface {
	Debug(string)
	Error(string)
	Fatal(string)
	Info(string)
	Panic(string)
	Warn(string)
}

type NilLogger struct{}

func (*NilLogger) Debug(string) {}
func (*NilLogger) Error(string) {}
func (*NilLogger) Fatal(string) {}
func (*NilLogger) Info(string)  {}
func (*NilLogger) Panic(string) {}
func (*NilLogger) Warn(string)  {}
