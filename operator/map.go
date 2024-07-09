package operator

import (
	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/streams"
)

type (
	MapFunc[TIn, TOut any] func(item TIn, index int) (TOut, error)
)

// Map transforms the Items emitted by an Observable by applying a function to each item.
func Map[TIn, TOut any](mapper MapFunc[TIn, TOut], opts ...observe.ObservableOption) observe.OperatorFunc[TIn, TOut] {
	if mapper == nil {
		panic(`"Map" expected mapper func`)
	}
	opts = defaultActivityName("Map", opts)
	return func(source *observe.Observable[TIn]) *observe.Observable[TOut] {
		var index int

		return observe.Operation[TIn, TOut](
			source,
			func(ctx observe.Context, upstream streams.Reader[TIn], downstream streams.Writer[TOut]) {
				for item := range upstream.Read() {
					switch item.Kind() {
					case streams.NextKind:
						output, err := mapper(item.Value(), index)
						index++

						if err != nil {
							downstream.Error(err)
							continue
						}

						downstream.Write(output)
					case streams.ErrorKind:
						downstream.Error(item.Error())
					}
				}
			},
			opts...,
		)
	}
}
