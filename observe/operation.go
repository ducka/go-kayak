package observe

import (
	"github.com/ducka/go-kayak/streams"
)

type OperationOption[TIn, TOut any] func(builder *OperationOptions[TIn, TOut])

type OperationOptions[TIn, TOut any] struct {
	*ObservableSettings
	poolingStrategy PoolingStrategy[TIn, TOut]
}

func NewOperationSettings[TIn, TOut any]() *OperationOptions[TIn, TOut] {
	return &OperationOptions[TIn, TOut]{
		ObservableSettings: NewObservableSettings(),
		poolingStrategy:    nil,
	}
}

func (b *OperationOptions[TIn, TOut]) apply(options ...OperationOption[TIn, TOut]) {
	for _, opt := range options {
		opt(b)
	}
}

func (b *OperationOptions[TIn, TOut]) toOptions() []ObservableOption {
	return []ObservableOption{
		func(options *ObservableSettings) {
			b.ObservableSettings.copyTo(options)
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
	settings := NewOperationSettings[TIn, TOut]()
	settings.apply(options...)

	observable := newObservable[TOut](
		func(ctx Context, downstream streams.Writer[TOut]) {
			upstream := source.ToStream()
			usePool := settings.poolingStrategy != nil

			if !usePool {
				operation(ctx, upstream, downstream)
				return
			}

			settings.poolingStrategy.Execute(ctx, operation, upstream, downstream)
		},
		mapToParentObservable(source),
		settings.toOptions()...,
	)

	return observable
}
