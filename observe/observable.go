package observe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ducka/go-kayak/instrumentation"
	"github.com/ducka/go-kayak/streams"
	"github.com/ducka/go-kayak/utils"
	"github.com/robfig/cron/v3"
)

type (
	OnErrorFunc                      func(error)
	OnCompleteFunc                   func(reason CompleteReason, err error)
	OnNextFunc[T any]                func(v T)
	ProducerFunc[T any]              func(ctx Context, streamWriter streams.Writer[T])
	OperationFunc[TIn any, TOut any] func(ctx Context, streamReader streams.Reader[TIn], streamWriter streams.Writer[TOut])
	StopFunc                         func()

	ErrorStrategy        string
	BackpressureStrategy string
	CompleteReason       string
	PublishStrategy      string
)

const (
	// ContinueOnError instructs the observable to continue observing items when an error is encountered
	ContinueOnError ErrorStrategy = "continue"
	// StopOnError instructs the observable to stop observing items when an error is encountered
	StopOnError ErrorStrategy = "stop"

	// Drop instructs the observable to drop items when the downstream is not ready to receive them
	Drop BackpressureStrategy = "drop"
	// Block instructs the observable to block on sending items until the downstream is ready to receive them
	Block BackpressureStrategy = "block"

	// Failed indicates that the observable has encountered an error whilst observing the upstream sequence
	Failed CompleteReason = "failure"
	// Completed indicates that the observable has completed observing the upstream sequence
	Completed CompleteReason = "completed"

	// Immediately instructs the observable to start observing as soon as the observable is instantiated
	Immediately PublishStrategy = "immediate"
	// OnConnect instructs the observable to start observing items when the Connect function is called
	OnConnect PublishStrategy = "connect"
)

// Producer observes items produced by a callback function
func Producer[T any](producer ProducerFunc[T], opts ...ObservableOption) *Observable[T] {
	return newObservable[T](
		func(ctx Context, streamWriter streams.Writer[T]) {
			producer(ctx, streamWriter)
		},
		nil,
		opts...,
	)
}

// Empty is an observable that emits nothing. This observable completes immediately.
func Empty[T any](opts ...ObservableOption) *Observable[T] {
	return newObservable[T](func(ctx Context, streamWriter streams.Writer[T]) {
		streamWriter.Close()
	}, nil, opts...)
}

// Array is an observable that emits items from an array
func Array[T any](items []T, opts ...ObservableOption) *Observable[T] {
	return newObservable[T](func(ctx Context, streamWriter streams.Writer[T]) {
		for _, item := range items {
			streamWriter.Write(item)
		}
		streamWriter.Close()
	}, nil, opts...)
}

// Value is an observable that emits a single item
func Value[T any](value T, opts ...ObservableOption) *Observable[T] {
	return newObservable[T](func(ctx Context, streamWriter streams.Writer[T]) {
		streamWriter.Write(value)
	}, nil, opts...)
}

// Cron is an observable that emits items on a specified cron schedule
func Cron(cronPattern string, opts ...ObservableOption) (*Observable[time.Time], StopFunc) {
	parser := cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)

	schedule, err := parser.Parse(cronPattern)
	if err != nil {
		panic(fmt.Errorf("failed to parse cron pattern: %v", err))
	}

	stopCh := make(chan struct{})
	stopper := func() {
		close(stopCh)
	}

	return newObservable[time.Time](
		func(ctx Context, streamWriter streams.Writer[time.Time]) {
			for {
				next := schedule.Next(time.Now())
				select {
				case <-time.After(time.Until(next)):
					streamWriter.Write(next)
				case <-stopCh:
					return
				}
			}
		},
		nil,
		opts...,
	), stopper
}

// Timer is an observable that emits items on a specified interval
func Timer(interval time.Duration, opts ...ObservableOption) (*Observable[time.Time], StopFunc) {
	ticker := time.NewTicker(interval)
	done := make(chan interface{})
	stopper := func() {
		defer close(done)
		ticker.Stop()
	}
	return newObservable[time.Time](
		func(ctx Context, streamWriter streams.Writer[time.Time]) {
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case t := <-ticker.C:
					streamWriter.Write(t)
				}
			}
		},
		nil,
		opts...,
	), stopper
}

// Range observes a range of generated integers
func Range(start, count int, opts ...ObservableOption) *Observable[int] {
	return newObservable[int](func(ctx Context, streamWriter streams.Writer[int]) {
		for i := start; i < start+count; i++ {
			streamWriter.Write(i)
		}
	}, nil, opts...)
}

// Stream observes items that are written to the Writer produced by this function
func Stream[T any](opts ...ObservableOption) (streams.Writer[T], *Observable[T]) {
	return newStreamObservable[T](nil, opts...)
}

// Sequence observes an array of values
func Sequence[T any](sequence []T, opts ...ObservableOption) *Observable[T] {
	return newObservable[T](func(ctx Context, streamWriter streams.Writer[T]) {
		for _, item := range sequence {
			streamWriter.Write(item)
		}
	}, nil, opts...)
}

/* TODO: Potential changes to Observable
1) You could create an observable that accepts a Reader as a producer of items. This could be an
efficient way to pump items into the observable; however "buffer" will no longer work, as buffer is defined
in the upstream Stream inside of the Observable.
1.1) I think for the public interface it's important to maintain "buffer" functionality of an observable. This
more efficient direct from stream observable should probably be some sort of specialist observable constructor.
2) The producer driven version of the observable is still important. So is the version of the observable that produces
a stream writer.
*/

func newStreamObservable[T any](parents []upstreamObservable, opts ...ObservableOption) (streams.Writer[T], *Observable[T]) {
	source := streams.NewStream[T]()

	return source,
		newObservable[T](
			func(ctx Context, streamWriter streams.Writer[T]) {
				for item := range source.Read() {
					streamWriter.Send(item)
				}
			},
			parents,
			opts...,
		)
}

func newObservable[T any](producer func(ctx Context, streamWriter streams.Writer[T]), parents []upstreamObservable, options ...ObservableOption) *Observable[T] {
	settings := NewObservableSettings()

	// Propagate observableOptions down the observable chain
	if len(parents) > 0 {
		propagateSettings(settings, parents)
	}

	settings.apply(options...)

	obs := &Observable[T]{
		mu:       new(sync.Mutex),
		settings: settings,
		ctx: NewContext(
			utils.ValueOrFallback(settings.ctx, context.Background()),
			utils.ValueOrFallback(settings.activity, ""),
		),
		publishStrategy:      utils.ValueOrFallback(settings.publishStrategy, Immediately),
		errorStrategy:        utils.ValueOrFallback(settings.errorStrategy, StopOnError),
		buffer:               utils.ValueOrFallback(settings.buffer, uint64(0)),
		backpressureStrategy: utils.ValueOrFallback(settings.backpressureStrategy, Block),
		producer:             producer,
		downstream:           streams.NewStream[T](),
		parents:              parents,
		subs:                 newSubscribers[T](),
		measurer:             instrumentation.Metrics(),
		logger:               instrumentation.Logging(),
	}

	if obs.publishStrategy == Immediately {
		obs.Connect()
	}

	return obs
}

type upstreamSettings struct {
	ctx             context.Context
	errorStrategy   *ErrorStrategy
	publishStrategy *PublishStrategy
}

type upstreamObservable interface {
	Connect()
	cloneSettings() upstreamSettings
}

type Observable[T any] struct {
	mu                   *sync.Mutex
	settings             *ObservableSettings
	ctx                  Context
	backpressureStrategy BackpressureStrategy
	errorStrategy        ErrorStrategy
	buffer               uint64
	publishStrategy      PublishStrategy
	// producer is a function that produces the upstream stream of items to be observed
	producer func(ctx Context, writer streams.Writer[T])
	// parent is the observable that has produced the upstream downstream of items
	parents []upstreamObservable
	// downstream is the stream that will Send items to the observer
	downstream *streams.Stream[T]
	// connected is a flag that indicates whether the observable has begun observing items from the upstream
	connected bool
	subs      *subscribers[T]
	measurer  instrumentation.Measurer
	logger    instrumentation.Logger
}

// Connect starts the observation of the upstream stream
func (o *Observable[T]) Connect() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.connected {
		return
	}

	// Propagate connect up the observable chain
	if len(o.parents) > 0 {
		for _, parent := range o.parents {
			parent.Connect()
		}
	}

	upstream := streams.NewStream[T](o.buffer)

	go func() {
		defer o.downstream.Close()

		for {
			select {
			case <-o.ctx.Done():
				now := time.Now()
				o.downstream.Error(o.ctx.Err())
				o.measurer.Timing(o.ctx.Activity, "item_backpressure", time.Since(now))
				o.measurer.Incr(o.ctx.Activity, "item_emitted", 1)
				o.measurer.Incr(o.ctx.Activity, "error_emitted", 1)
				return
			case item, ok := <-upstream.Read():
				if !ok {
					return
				}

				if item.Error() != nil && o.errorStrategy == StopOnError {
					now := time.Now()
					o.downstream.Send(item)
					o.measurer.Timing(o.ctx.Activity, "item_backpressure", time.Since(now))
					o.measurer.Incr(o.ctx.Activity, "item_emitted", 1)
					o.measurer.Incr(o.ctx.Activity, "error_emitted", 1)
					return
				}

				if o.backpressureStrategy == Drop {
					now := time.Now()
					ok := o.downstream.TrySend(item)
					if ok {
						// Only record backpressure measurer if the item was successfully sent, otherwise you'll
						// be measuring backpressure for dropped items which would bring backpressure down to zero,
						// making the metric useless
						o.measurer.Timing(o.ctx.Activity, "item_backpressure", time.Since(now))
						o.measurer.Incr(o.ctx.Activity, "item_emitted", 1)
						o.measurer.Incr(o.ctx.Activity, "value_emitted", 1)
					} else {
						o.measurer.Incr(o.ctx.Activity, "value_dropped", 1)
					}
				} else {
					now := time.Now()
					o.downstream.Send(item)
					o.measurer.Timing(o.ctx.Activity, "item_backpressure", time.Since(now))
					o.measurer.Incr(o.ctx.Activity, "item_emitted", 1)
					o.measurer.Incr(o.ctx.Activity, "value_emitted", 1)
				}
			}
		}
	}()

	go func() {
		defer upstream.Close()
		// TODO: Consider not exporting the observable settings here.. it's a bit clunky.
		o.producer(o.ctx, upstream)
	}()

	o.connected = true
}

// ToStream returns a downstream Reader so the observable can be observed asynchronously
func (o *Observable[T]) ToStream() streams.Reader[T] {
	return o.downstream
}

// ToResult synchronously observes the observable and returns the emitted items. This function will block until the observable
// completes observing the upstream sequence.
func (o *Observable[T]) ToResult() []streams.Notification[T] {
	notifications := make([]streams.Notification[T], 0)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for item := range o.downstream.Read() {
			notifications = append(notifications, item)
		}
		wg.Done()
	}()

	o.Connect()

	wg.Wait()

	return notifications
}

// Subscribe synchronously observes the observable and invokes its OnNext, OnError and OnComplete callbacks as items are
// emitted from the upstream sequence. This function will block until the observable completes observing the upstream sequence.
func (o *Observable[T]) Subscribe(onNext OnNextFunc[T], options ...SubscribeOption) {
	subscribeOptions := &subscribeOptions{
		onError:    func(err error) {},
		onComplete: func(reason CompleteReason, err error) {},
	}

	for _, opt := range options {
		opt(subscribeOptions)
	}

	o.subs.add(onNext, subscribeOptions.onError, subscribeOptions.onComplete)

	// start listening to the upstream for emitted items as soon as the first subscriber is registered
	if o.subs.len() == 1 {
		go func() {
			var err error

			for item := range o.downstream.Read() {
				if item.Error() != nil {
					err = item.Error()
					o.subs.dispatchError(err)
					continue
				}

				o.subs.dispatchNext(item.Value())
			}

			reason := Completed
			if err != nil {
				reason = Failed
			}

			o.subs.dispatchComplete(reason, err)
		}()
	}

	if subscribeOptions.waitTillComplete {
		o.subs.WaitTillComplete()
	}
}

func (o *Observable[T]) getContext() context.Context {
	return o.ctx
}

func (o *Observable[T]) setErrorStrategy(strategy ErrorStrategy) {
	o.errorStrategy = strategy
}

func (o *Observable[T]) cloneSettings() upstreamSettings {
	return upstreamSettings{
		ctx:             o.ctx,
		errorStrategy:   o.settings.errorStrategy,
		publishStrategy: o.settings.publishStrategy,
	}
}

func propagateSettings(opts *ObservableSettings, parents []upstreamObservable) {
	ctxs := make([]context.Context, 0, len(parents))

	// Certain settings need to be propagated from parent observable observableOptions to their child
	// observable observableOptions
	for _, p := range parents {
		o := p.cloneSettings()
		ctxs = append(ctxs, o.ctx)

		if o.errorStrategy != nil {
			opts.WithErrorStrategy(*o.errorStrategy)
		}
		if o.publishStrategy != nil {
			opts.WithPublishStrategy(*o.publishStrategy)
		}
	}

	opts.ctx = utils.ToPtr(utils.CombinedContexts(ctxs...))
}

func mapToParentObservable[T any](obs ...*Observable[T]) []upstreamObservable {
	parents := make([]upstreamObservable, 0, len(obs))
	for _, o := range obs {
		parents = append(parents, o)
	}
	return parents
}
