package operator

import (
	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/stream"
)

// Flatten flattens a stream of slices or batches into a flat stream of Items
func Flatten[T any](opts ...observe.ObservableOption) observe.OperatorFunc[[]T, T] {
	opts = defaultActivityName("Flatten", opts)
	return func(source *observe.Observable[[]T]) *observe.Observable[T] {
		return observe.Operation[[]T, T](
			source,
			func(ctx observe.Context, upstream stream.Reader[[]T], downstream stream.Writer[T]) {
				for i := range upstream.Read() {
					if i.IsError() {
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
