package operator

import (
	"github.com/ducka/go-kayak/observe"
)

// Passthrough is an operator that passes all Items through without modification.
func Passthrough[T any](opts ...observe.ObservableOption) OperatorFunc[T, T] {
	opts = defaultActivityName("Passthrough", opts)
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(ctx observe.Context, upstream observe.StreamReader[T], downstream observe.StreamWriter[T]) {
				for i := range upstream.Read() {
					downstream.Send(i)
				}
			},
			opts...,
		)
	}
}
