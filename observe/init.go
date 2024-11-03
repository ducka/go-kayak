package observe

func init() {
	SetMetricsProvider(&NilMeasurer{})
	SetLoggingProvider(&NilLogger{})
}
