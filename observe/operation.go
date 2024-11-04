package observe

import (
	"github.com/ducka/go-kayak/streams"
)

type OperationOption[TIn, TOut any] func(builder *OperationOptions[TIn, TOut])

type OperationOptions[TIn, TOut any] struct {
	observableOptions *ObservableOptions
	poolingStrategy   PoolingStrategy[TIn, TOut]
}

func NewOperationOptionsBuilder[TIn, TOut any]() *OperationOptions[TIn, TOut] {
	return &OperationOptions[TIn, TOut]{
		observableOptions: NewObservableOptionsBuilder(),
		poolingStrategy:   nil,
	}
}

func (b *OperationOptions[TIn, TOut]) apply(options ...OperationOption[TIn, TOut]) {
	for _, opt := range options {
		opt(b)
	}
}

func (b *OperationOptions[TIn, TOut]) toObservableOptions() []ObservableOption {
	return []ObservableOption{
		func(options *ObservableOptions) {
			b.observableOptions.copyTo(options)
		},
	}
}

func (b *OperationOptions[TIn, TOut]) WithPool(poolSize int) *OperationOptions[TIn, TOut] {
	b.WithPoolStrategy(NewRoundRobinPoolingStrategy[TIn, TOut](poolSize))
	return b
}

func (b *OperationOptions[TIn, TOut]) WithPartitionedPool(keySelector PartitionKeySelector[TIn], settings ...ParitionedPoolSettings) *OperationOptions[TIn, TOut] {
	b.WithPoolStrategy(NewPartitionedPoolingStrategy[TIn, TOut](keySelector, settings...))
	return b
}

func (b *OperationOptions[TIn, TOut]) WithPoolStrategy(strategy PoolingStrategy[TIn, TOut]) *OperationOptions[TIn, TOut] {
	b.poolingStrategy = strategy
	return b
}

// Operation observes items produce by an streams processing operation. This observable provides an operation callback that
// provides the opportunity to manipulate data in the stream before sending it to a downstream observer. This function allows
// you to change the type of an Observable from one type to another.
func Operation[TIn any, TOut any](
	source *Observable[TIn],
	operation OperationFunc[TIn, TOut],
	options ...OperationOption[TIn, TOut],
) *Observable[TOut] {
	o := NewOperationOptionsBuilder[TIn, TOut]()
	o.apply(options...)

	observable := newObservable[TOut](
		func(downstream streams.Writer[TOut], observableOptions ObservableOptions) {
			upstream := source.ToStream()
			usePool := o.poolingStrategy != nil

			ctx := Context{
				Context:  observableOptions.ctx,
				Activity: observableOptions.activity,
			}

			if !usePool {
				operation(ctx, upstream, downstream)
				return
			}

			o.poolingStrategy.Execute(ctx, operation, upstream, downstream)
		},
		mapToParentObservable(source),
		o.toObservableOptions()...,
	)

	return observable
}
