package operator

import (
	"github.com/ducka/go-kayak/observe"
)

type (
	MapFunc[TIn, TOut any] func(item TIn, index int) (TOut, error)
)

// Map transforms the Items emitted by an Observable by applying a function to each item.
func Map[TIn any, TOut any](mapper MapFunc[TIn, TOut], opts ...observe.ObservableOption) OperatorFunc[TIn, TOut] {
	if mapper == nil {
		panic(`"Map" expected mapper func`)
	}
	opts = defaultActivityName("Map", opts)
	return func(source *observe.Observable[TIn]) *observe.Observable[TOut] {
		var index int

		return observe.Operation[TIn, TOut](
			source,
			func(ctx observe.Context, upstream observe.StreamReader[TIn], downstream observe.StreamWriter[TOut]) {
				for item := range upstream.Read() {
					switch item.Kind() {
					case observe.NextKind:
						output, err := mapper(item.Value(), index)
						index++

						if err != nil {
							downstream.Error(err)
							continue
						}

						downstream.Write(output)
					case observe.ErrorKind:
						downstream.Error(item.Error())
					}
				}
			},
			opts...,
		)
	}
}
