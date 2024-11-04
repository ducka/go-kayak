package instrumentation

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
