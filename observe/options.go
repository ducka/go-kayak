package observe

import (
	"context"
)

type ObservableOption func(options *ObservableOptions)

type ObservableOptions struct {
	ctx                  context.Context
	activity             string
	backpressureStrategy BackpressureStrategy
	errorStrategy        ErrorStrategy
	buffer               uint64
	publishStrategy      PublishStrategy
}

func NewObservableOptionsBuilder() *ObservableOptions {
	return &ObservableOptions{
		ctx:                  context.Background(),
		backpressureStrategy: defaultBackpressureStrategy,
		errorStrategy:        defaultErrorStrategy,
		publishStrategy:      defaultPublishStrategy,
	}
}

func (b *ObservableOptions) apply(options ...ObservableOption) {
	for _, opt := range options {
		opt(b)
	}
}

func (b *ObservableOptions) copyTo(o *ObservableOptions) {
	o.ctx = b.ctx
	o.activity = b.activity
	o.backpressureStrategy = b.backpressureStrategy
	o.errorStrategy = b.errorStrategy
	o.buffer = b.buffer
	o.publishStrategy = b.publishStrategy
}

func (b *ObservableOptions) WithContext(ctx context.Context) *ObservableOptions {
	b.ctx = ctx
	return b
}

func (b *ObservableOptions) WithPublishStrategy(publishStrategy PublishStrategy) *ObservableOptions {
	b.publishStrategy = publishStrategy
	return b
}

func (b *ObservableOptions) WithErrorStrategy(errorStrategy ErrorStrategy) *ObservableOptions {
	b.errorStrategy = errorStrategy
	return b
}

func (b *ObservableOptions) WithBackpressureStrategy(backpressureStrategy BackpressureStrategy) *ObservableOptions {
	b.backpressureStrategy = backpressureStrategy
	return b
}

func (b *ObservableOptions) WithActivityName(activity string) *ObservableOptions {
	b.activity = activity
	return b
}

func (b *ObservableOptions) WithBuffer(bufferSize uint64) *ObservableOptions {
	b.buffer = bufferSize
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
