package observe

import (
	"context"
)

type ObservableOption func(settings *ObservableSettings)

type ObservableSettings struct {
	ctx                  *context.Context
	activity             *string
	backpressureStrategy *BackpressureStrategy
	errorStrategy        *ErrorStrategy
	buffer               *uint64
	publishStrategy      *PublishStrategy
}

func NewObservableSettings() *ObservableSettings {
	return &ObservableSettings{}
}

func (b *ObservableSettings) apply(options ...ObservableOption) {
	for _, opt := range options {
		opt(b)
	}
}

// copyTo copies settings to the supplied ObservableSettings object
func (b *ObservableSettings) copyTo(dest *ObservableSettings) {
	if b.ctx != nil {
		dest.ctx = b.ctx
	}
	if b.activity != nil {
		dest.activity = b.activity
	}
	if b.backpressureStrategy != nil {
		dest.backpressureStrategy = b.backpressureStrategy
	}
	if b.errorStrategy != nil {
		dest.errorStrategy = b.errorStrategy
	}
	if b.buffer != nil {
		dest.buffer = b.buffer
	}
	if b.publishStrategy != nil {
		dest.publishStrategy = b.publishStrategy
	}
}

func (b *ObservableSettings) WithContext(ctx context.Context) *ObservableSettings {
	b.ctx = &ctx
	return b
}

func (b *ObservableSettings) WithPublishStrategy(publishStrategy PublishStrategy) *ObservableSettings {
	b.publishStrategy = &publishStrategy
	return b
}

func (b *ObservableSettings) WithErrorStrategy(errorStrategy ErrorStrategy) *ObservableSettings {
	b.errorStrategy = &errorStrategy
	return b
}

func (b *ObservableSettings) WithBackpressureStrategy(backpressureStrategy BackpressureStrategy) *ObservableSettings {
	b.backpressureStrategy = &backpressureStrategy
	return b
}

func (b *ObservableSettings) WithActivityName(activity string) *ObservableSettings {
	b.activity = &activity
	return b
}

func (b *ObservableSettings) WithBuffer(bufferSize uint64) *ObservableSettings {
	b.buffer = &bufferSize
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
