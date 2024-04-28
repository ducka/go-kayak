package operator

import (
	"github.com/ducka/go-kayak/observe"
)

type (
	MapFunc[TIn, TOut any] func(TIn, int) (TOut, error)
)

// Map transforms the items emitted by an Observable by applying a function to each item.
func Map[TIn any, TOut any](mapper MapFunc[TIn, TOut], options ...observe.ObservableOption) observe.OperatorFunc[TIn, TOut] {
	if mapper == nil {
		panic(`"Map" expected mapper func`)
	}
	return func(source *observe.Observable[TIn]) *observe.Observable[TOut] {
		var index int

		return observe.ObserveOperation[TIn, TOut](
			source,
			func(upstream observe.StreamReader[TIn], downstream observe.StreamWriter[TOut]) {
				for item := range upstream.Read() {
					switch item.Kind() {
					case observe.NextKind:
						output, err := mapper(item.Value(), index)
						index++

						if err != nil {
							downstream.Error(err, output)
							continue
						}

						downstream.Write(output)
					case observe.ErrorKind:
						downstream.Error(item.Err())
					}
				}
			},
			options...,
		)
	}
}
