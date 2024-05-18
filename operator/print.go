package operator

import (
	"fmt"

	"github.com/ducka/go-kayak/observe"
)

// Print logs the emitted item out via console
func Print[T any](label string, opts ...observe.ObservableOption) OperatorFunc[T, T] {
	opts = defaultActivityName("Print", opts)
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(ctx observe.Context, upstream observe.StreamReader[T], downstream observe.StreamWriter[T]) {
				for i := range upstream.Read() {
					fmt.Println(label, i)
					downstream.Send(i)
				}
			},
			opts...,
		)
	}
}
