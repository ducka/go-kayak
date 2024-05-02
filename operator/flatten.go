package operator

import (
	"github.com/ducka/go-kayak/observe"
)

// Flatten flattens a stream of slices or batches into a flat stream of items
func Flatten[T any](opts ...observe.Option) OperatorFunc[[]T, T] {
	opts = defaultActivityName("Flatten", opts)
	return func(source *observe.Observable[[]T]) *observe.Observable[T] {
		return observe.Operation[[]T, T](
			source,
			func(ctx observe.Context, upstream observe.StreamReader[[]T], downstream observe.StreamWriter[T]) {
				for i := range upstream.Read() {
					if i.HasError() {
						downstream.Error(i.Error())
						continue
					}

					for _, item := range i.Value() {
						downstream.Write(item)
					}
				}
			},
			opts...,
		)
	}
}
