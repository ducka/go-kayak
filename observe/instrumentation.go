package observe

import (
	"time"
)

type Measurer interface {
	Incr(activity string, name string, value float64, tags ...string)
	Timing(activity string, name string, value time.Duration, tags ...string)
}

type NilMeasurer struct{}

func (*NilMeasurer) Incr(activity string, name string, value float64, tags ...string)         {}
func (*NilMeasurer) Timing(activity string, name string, value time.Duration, tags ...string) {}

type Logger interface {
	Debug(activity string, message string)
	Error(activity string, message string)
	Fatal(activity string, message string)
	Info(activity string, message string)
	Panic(activity string, message string)
	Warn(activity string, message string)
}

type NilLogger struct{}

func (*NilLogger) Debug(string, string) {}
func (*NilLogger) Error(string, string) {}
func (*NilLogger) Fatal(string, string) {}
func (*NilLogger) Info(string, string)  {}
func (*NilLogger) Panic(string, string) {}
func (*NilLogger) Warn(string, string)  {}

var (
	measurer Measurer
	logger   Logger
)

func SetMetricsProvider(provider Measurer) {
	if provider == nil {
		panic("Metrics provider must be specified")
	}

	measurer = provider
}

func SetLoggingProvider(provider Logger) {
	if provider == nil {
		panic("Logging provider must be specified")
	}

	logger = provider
}
