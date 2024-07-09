package operator

import (
	"time"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/streams"
)

// Throttle throttles the rate of Items emitted by the observable to the specified flow rate.
func Throttle[T any](flowRate int64, perDuration time.Duration, opts ...observe.ObservableOption) observe.OperatorFunc[T, T] {
	if flowRate < 1 {
		flowRate = 1
	}
	opts = defaultActivityName("Throttle", opts)
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(ctx observe.Context, upstream streams.Reader[T], downstream streams.Writer[T]) {
				for i := range upstream.Read() {
					downstream.Send(i)
					time.Sleep(time.Duration(int64(perDuration) / flowRate))
				}
			},
			opts...,
		)
	}
}
