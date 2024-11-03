package observe

import (
	"github.com/ducka/go-kayak/streams"
)

type OperationOption[TIn, TOut any] func(builder *OperationOptionsBuilder[TIn, TOut])

type operationOptions[TIn, TOut any] struct {
	poolingStrategy PoolingStrategy[TIn, TOut]
}

type OperationOptionsBuilder[TIn, TOut any] struct {
	ObservableOptionsBuilder[TOut]
	operationOptions[TIn, TOut]
}

func (b *OperationOptionsBuilder[TIn, TOut]) apply(options ...OperationOption[TIn, TOut]) {
	for _, opt := range options {
		opt(b)
	}
}

func (b *OperationOptionsBuilder[TIn, TOut]) toObservableOptions() []ObservableOption[TOut] {
	return []ObservableOption[TOut]{
		func(builder *ObservableOptionsBuilder[TOut]) {
			builder.observableOptions = b.observableOptions
		},
	}
}

func (b *OperationOptionsBuilder[TIn, TOut]) WithPool(poolSize int) *OperationOptionsBuilder[TIn, TOut] {
	b.WithPoolStrategy(NewRoundRobinPoolingStrategy[TIn, TOut](poolSize))
	return b
}

func (b *OperationOptionsBuilder[TIn, TOut]) WithPartitionedPool(poolSize int, keySelector PartitionKeySelector[TIn]) *OperationOptionsBuilder[TIn, TOut] {
	b.WithPoolStrategy(NewPartitionedPoolingStrategy[TIn, TOut](poolSize, keySelector, DefaultHashFunc))
	return b
}

func (b *OperationOptionsBuilder[TIn, TOut]) WithPoolStrategy(strategy PoolingStrategy[TIn, TOut]) *OperationOptionsBuilder[TIn, TOut] {
	b.operationOptions.poolingStrategy = strategy
	return b
}

// Operation observes items produce by an streams processing operation. This observable provides an operation callback that
// provides the opportunity to manipulate data in the stream before sending it to a downstream observer. This function allows
// you to change the type of an Observable from one type to another.
func Operation[TIn any, TOut any](
	source *Observable[TIn],
	operation OperationFunc[TIn, TOut],
	opts ...OperationOption[TIn, TOut],
) *Observable[TOut] {
	operationOptions := OperationOptionsBuilder[TIn, TOut]{}
	operationOptions.apply(opts...)

	observable := newObservable[TOut](
		func(downstream streams.Writer[TOut], observableOptions observableOptions) {
			upstream := source.ToStream()
			usePool := operationOptions.poolingStrategy != nil

			ctx := Context{
				Context:  observableOptions.ctx,
				Activity: observableOptions.activity,
			}

			if !usePool {
				operation(ctx, upstream, downstream)
				return
			}

			operationOptions.poolingStrategy.Execute(ctx, operation, upstream, downstream)
		},
		mapToParentObservable(source),
		operationOptions.toObservableOptions()...,
	)

	return observable
}
