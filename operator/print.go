package operator

import (
	"fmt"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/stream"
)

// Print logs the emitted item out via console
func Print[T any](label string, opts ...observe.ObservableOption) observe.OperatorFunc[T, T] {
	opts = defaultActivityName("Print", opts)
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(ctx observe.Context, upstream stream.Reader[T], downstream stream.Writer[T]) {
				for i := range upstream.Read() {
					fmt.Println(label, i)
					downstream.Send(i)
				}
			},
			opts...,
		)
	}
}
