package operator

import (
	"github.com/ducka/go-kayak/observe"
)

// Passthrough is an operator that passes all items through without modification.
func Passthrough[T any](opts ...observe.Option) OperatorFunc[T, T] {
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(upstream observe.StreamReader[T], downstream observe.StreamWriter[T]) {
				for i := range upstream.Read() {
					downstream.Send(i)
				}
			},
			opts...,
		)
	}
}
