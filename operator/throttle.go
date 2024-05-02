package operator

import (
	"time"

	"github.com/ducka/go-kayak/observe"
)

// Throttle throttles the rate of items emitted by the observable to the specified flow rate.
func Throttle[T any](flowRate int64, perDuration time.Duration, opts ...observe.Option) OperatorFunc[T, T] {
	if flowRate < 1 {
		flowRate = 1
	}
	opts = defaultActivityName("Throttle", opts)
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(ctx observe.Context, upstream observe.StreamReader[T], downstream observe.StreamWriter[T]) {
				for i := range upstream.Read() {
					downstream.Send(i)
					time.Sleep(time.Duration(int64(perDuration) / flowRate))
				}
			},
			opts...,
		)
	}
}
