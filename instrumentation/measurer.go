package instrumentation

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
