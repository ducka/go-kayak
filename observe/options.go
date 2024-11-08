package observe

import (
	"context"
	"runtime"
)

type observableOptions struct {
	ctx                  context.Context
	activity             string
	backpressureStrategy BackpressureStrategy
	errorStrategy        ErrorStrategy
	poolSize             int
	buffer               uint64
	publishStrategy      PublishStrategy
}

func newOptions() observableOptions {
	return observableOptions{
		ctx:                  context.Background(),
		backpressureStrategy: Block,
		errorStrategy:        StopOnError,
		buffer:               0,
		publishStrategy:      Immediately,
	}
}

type ObservableOption func(options *observableOptions)

// WithPublishStrategy instructs the observable when to start observing items
func WithPublishStrategy(strategy PublishStrategy) ObservableOption {
	return func(options *observableOptions) {
		options.publishStrategy = strategy
	}
}

func WithContext(ctx context.Context) ObservableOption {
	return func(options *observableOptions) {
		options.ctx = ctx
	}
}

func WithErrorStrategy(strategy ErrorStrategy) ObservableOption {
	return func(options *observableOptions) {
		options.errorStrategy = strategy
	}
}

func WithBackpressureStrategy(strategy BackpressureStrategy) ObservableOption {
	return func(options *observableOptions) {
		options.backpressureStrategy = strategy
	}
}

func WithActivityName(activityName string) ObservableOption {
	return func(options *observableOptions) {
		options.activity = activityName
	}
}

func WithBuffer(buffer uint64) func(options *observableOptions) {
	return func(options *observableOptions) {
		options.buffer = buffer
	}
}

// WithCPUPool sets the number of goroutines to use for currently processing items to the number of CPU cores on the host machine
func WithCPUPool() ObservableOption {
	return WithPool(runtime.NumCPU())
}

// WithPool sets the number of goroutines to use for concurrently processing items
func WithPool(poolSize int) ObservableOption {
	return func(options *observableOptions) {
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
