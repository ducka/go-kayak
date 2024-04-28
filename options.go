package kayak

import (
	"context"
	"runtime"
)

type subscribeOptions struct {
	onError    OnErrorFunc
	onComplete OnCompleteFunc
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

type observableOptions struct {
	ctx                  context.Context
	activity             string
	backpressureStrategy BackpressureStrategy
	errorStrategy        ErrorStrategy
	poolSize             int
}

func newObservableOptions() observableOptions {
	return observableOptions{
		ctx:                  context.Background(),
		backpressureStrategy: Block,
		errorStrategy:        StopOnError,
	}
}

func (o observableOptions) Clone() observableOptions {
	return observableOptions{
		ctx: o.ctx,
	}
}

type ObservableOption func(options *observableOptions)

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
