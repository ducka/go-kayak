package observe

import (
	"context"
	"runtime"
)

type options struct {
	ctx                  context.Context
	activity             string
	backpressureStrategy BackpressureStrategy
	errorStrategy        ErrorStrategy
	poolSize             int
	buffer               uint64
	publishStrategy      PublishStrategy
}

func newOptions() options {
	return options{
		ctx:                  context.Background(),
		backpressureStrategy: Block,
		errorStrategy:        StopOnError,
		buffer:               0,
		publishStrategy:      Immediately,
	}
}

type Option func(options *options)

// WithPublishStrategy instructs the observable when to start observing items
func WithPublishStrategy(strategy PublishStrategy) Option {
	return func(options *options) {
		options.publishStrategy = strategy
	}
}

func WithContext(ctx context.Context) Option {
	return func(options *options) {
		options.ctx = ctx
	}
}

func WithErrorStrategy(strategy ErrorStrategy) Option {
	return func(options *options) {
		options.errorStrategy = strategy
	}
}

func WithBackpressureStrategy(strategy BackpressureStrategy) Option {
	return func(options *options) {
		options.backpressureStrategy = strategy
	}
}

func WithActivityName(activityName string) Option {
	return func(options *options) {
		options.activity = activityName
	}
}

func WithBuffer(buffer uint64) func(options *options) {
	return func(options *options) {
		options.buffer = buffer
	}
}

// WithCPUPool sets the number of goroutines to use for currently processing items to the number of CPU cores on the host machine
func WithCPUPool() Option {
	return WithPool(runtime.NumCPU())
}

// WithPool sets the number of goroutines to use for concurrently processing items
func WithPool(poolSize int) Option {
	return func(options *options) {
		if poolSize < 1 {
			options.poolSize = 1
			return
		}

		options.poolSize = poolSize
	}
}

type subscribeOptions struct {
	onError          OnErrorFunc
	onComplete       OnCompleteFunc
	waitTillFinished bool
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
