package operator

import (
	"github.com/ducka/go-kayak/observe"
)

type (
	ProcessorFunc[TIn, TOut any] func(ctx observe.Context, upstream observe.StreamReader[TIn], downstream observe.StreamWriter[TOut])
)

// Process is an operator that gives direct access to the upstream and downstream streams.
func Process[TIn, TOut any](processor ProcessorFunc[TIn, TOut], opts ...observe.ObservableOption) observe.OperatorFunc[TIn, TOut] {
	if processor == nil {
		panic(`"Process" expected processor func`)
	}
	opts = defaultActivityName("Process", opts)
	return func(source *observe.Observable[TIn]) *observe.Observable[TOut] {

		return observe.Operation[TIn, TOut](
			source,
			func(ctx observe.Context, upstream observe.StreamReader[TIn], downstream observe.StreamWriter[TOut]) {
				processor(ctx, upstream, downstream)
			},
			opts...,
		)
	}
}
