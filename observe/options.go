package observe

import (
	"context"
)

type ObservableOption[TIn any] func(builder *ObservableOptionsBuilder[TIn])

type ObservableOptionsBuilder[TIn any] struct {
	observableOptions
}

type observableOptions struct {
	ctx                  context.Context
	activity             string
	backpressureStrategy BackpressureStrategy
	errorStrategy        ErrorStrategy
	buffer               uint64
	publishStrategy      PublishStrategy
}

func NewObservableOptionsBuilder[TIn any]() *ObservableOptionsBuilder[TIn] {
	return &ObservableOptionsBuilder[TIn]{
		observableOptions: observableOptions{
			ctx:                  context.Background(),
			backpressureStrategy: defaultBackpressureStrategy,
			errorStrategy:        defaultErrorStrategy,
			publishStrategy:      defaultPublishStrategy,
		},
	}
}

func (b *ObservableOptionsBuilder[TIn]) apply(options ...ObservableOption[TIn]) {
	for _, opt := range options {
		opt(b)
	}
}

func (b *ObservableOptionsBuilder[TIn]) WithContext(ctx context.Context) *ObservableOptionsBuilder[TIn] {
	b.observableOptions.ctx = ctx
	return b
}

func (b *ObservableOptionsBuilder[TIn]) WithPublishStrategy(publishStrategy PublishStrategy) *ObservableOptionsBuilder[TIn] {
	b.observableOptions.publishStrategy = publishStrategy
	return b
}

func (b *ObservableOptionsBuilder[TIn]) WithErrorStrategy(errorStrategy ErrorStrategy) *ObservableOptionsBuilder[TIn] {
	b.observableOptions.errorStrategy = errorStrategy
	return b
}

func (b *ObservableOptionsBuilder[TIn]) WithBackpressureStrategy(backpressureStrategy BackpressureStrategy) *ObservableOptionsBuilder[TIn] {
	b.observableOptions.backpressureStrategy = backpressureStrategy
	return b
}

func (b *ObservableOptionsBuilder[TIn]) WithActivityName(activity string) *ObservableOptionsBuilder[TIn] {
	b.observableOptions.activity = activity
	return b
}

func (b *ObservableOptionsBuilder[TIn]) WithBuffer(bufferSize uint64) *ObservableOptionsBuilder[TIn] {
	b.observableOptions.buffer = bufferSize
	return b
}

type subscribeOptions struct {
	onError          OnErrorFunc
	onComplete       OnCompleteFunc
	waitTillComplete bool
}

type SubscribeOption func(options *subscribeOptions)

func WithOnError(onError OnErrorFunc) SubscribeOption {
	return func(options *subscribeOptions) {
		options.onError = onError
	}
}

func WithOnComplete(onComplete OnCompleteFunc) SubscribeOption {
	return func(options *subscribeOptions) {
		options.onComplete = onComplete
	}
}

func WithWaitTillComplete() SubscribeOption {
	return func(options *subscribeOptions) {
		options.waitTillComplete = true
	}
}
