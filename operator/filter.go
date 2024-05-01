package operator

import (
	"github.com/ducka/go-kayak/observe"
)

type (
	PredicateFunc[T any] func(item T) bool
)

func Filter[T any](predicate PredicateFunc[T], opts ...observe.Option) OperatorFunc[T, T] {
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(upstream observe.StreamReader[T], downstream observe.StreamWriter[T]) {
				for i := range upstream.Read() {
					// If the element has a value and satisfies the predicate, emit it; otherwise if the
					// element is an error, emit it. All others must be filtered out.
					if (i.HasValue() && predicate(i.Value())) || i.HasError() {
						downstream.Send(i)
					}
				}
			},
			opts...,
		)
	}
}
