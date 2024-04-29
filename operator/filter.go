package operator

import (
	"github.com/ducka/go-kayak/observe"
)

type (
	PredicateFunc[T any] func(item observe.Notification[T]) bool
)

func Filter[T any](predicate PredicateFunc[T], opts ...observe.Option) observe.OperatorFunc[T, T] {
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(upstream observe.StreamReader[T], downstream observe.StreamWriter[T]) {
				for i := range upstream.Read() {
					if predicate(i) {
						downstream.Send(i)
					}
				}
			},
			opts...,
		)
	}
}
