package instrumentation

var (
	measurer Measurer
	logger   Logger
)

func init() {
	SetMeasurer(&NilMeasurer{})
	SetLogger(&NilLogger{})
}

func SetMeasurer(provider Measurer) {
	if provider == nil {
		panic("Metrics provider must be specified")
	}

	measurer = provider
}

func SetLogger(provider Logger) {
	if provider == nil {
		panic("Logging provider must be specified")
	}

	logger = provider
}

func Metrics() Measurer {
	return measurer
}

func Logging() Logger {
	return logger
}
