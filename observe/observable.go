package observe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ducka/go-kayak/streams"
	"github.com/ducka/go-kayak/utils"
	"github.com/robfig/cron/v3"
)

type (
	OnErrorFunc                      func(error)
	OnCompleteFunc                   func(reason CompleteReason, err error)
	OnNextFunc[T any]                func(v T)
	ProducerFunc[T any]              func(streamWriter streams.Writer[T])
	OperationFunc[TIn any, TOut any] func(Context, streams.Reader[TIn], streams.Writer[TOut])
	StopFunc                         func()

	ErrorStrategy        string
	BackpressureStrategy string
	CompleteReason       string
	PublishStrategy      string
)

type Context struct {
	context.Context
	Activity string
}

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
		func(streamWriter streams.Writer[T], opts observableOptions) {
			producer(streamWriter)
		},
		nil,
		opts...,
	)
}

// Empty is an observable that emits nothing. This observable completes immediately.
func Empty[T any](opts ...ObservableOption) *Observable[T] {
	return newObservable[T](func(streamWriter streams.Writer[T], opts observableOptions) {
		streamWriter.Close()
	}, nil, opts...)
}

// Array is an observable that emits items from an array
func Array[T any](items []T, opts ...ObservableOption) *Observable[T] {
	return newObservable[T](func(streamWriter streams.Writer[T], opts observableOptions) {
		for _, item := range items {
			streamWriter.Write(item)
		}
		streamWriter.Close()
	}, nil, opts...)
}

// Value is an observable that emits a single item
func Value[T any](value T, opts ...ObservableOption) *Observable[T] {
	return newObservable[T](func(streamWriter streams.Writer[T], opts observableOptions) {
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
		func(streamWriter streams.Writer[time.Time], opts observableOptions) {

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
		func(streamWriter streams.Writer[time.Time], opts observableOptions) {
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
	return newObservable[int](func(streamWriter streams.Writer[int], opts observableOptions) {
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
	return newObservable[T](func(streamWriter streams.Writer[T], opts observableOptions) {
		for _, item := range sequence {
			streamWriter.Write(item)
		}
	}, nil, opts...)
}

// Operation observes items produce by an streams processing operation. This observable provides an operation callback that
// provides the opportunity to manipulate data in the stream before sending it to a downstream observer. This function allows
// you to change the type of an Observable from one type to another.
func Operation[TIn any, TOut any](
	source *Observable[TIn],
	operation OperationFunc[TIn, TOut],
	opts ...ObservableOption,
) *Observable[TOut] {
	observable := newObservable[TOut](
		func(downstream streams.Writer[TOut], opts observableOptions) {
			opWg := &sync.WaitGroup{}
			poolWg := &sync.WaitGroup{}
			upstream := source.ToStream()
			usePool := opts.poolSize > 1
			ctx := Context{
				Context: opts.ctx,
			}

			if !usePool {
				operation(ctx, upstream, downstream)
				return
			}

			// Initialise the pool of operations to currently process the upstream
			pool := make(chan *streams.Stream[TIn], opts.poolSize)
			poolStreamsToClose := make([]*streams.Stream[TIn], opts.poolSize)
			for i := 0; i < opts.poolSize; i++ {
				poolStream := streams.NewStream[TIn]()
				pool <- poolStream
				poolStreamsToClose[i] = poolStream

				opWg.Add(1)
				go func(poolStream streams.Reader[TIn], downstream streams.Writer[TOut]) {
					defer opWg.Done()
					now := time.Now()
					operation(ctx, poolStream, downstream)
					opts.metrics.Timing(opts.activity, "operation_duration", time.Since(now))
				}(poolStream, downstream)
			}

			// Send items to the next available stream in the pool
			for item := range upstream.Read() {
				select {
				case <-opts.ctx.Done():
					return
				case nextStream := <-pool:
					poolWg.Add(1)

					go func(item streams.Notification[TIn], nextStream *streams.Stream[TIn], pool chan *streams.Stream[TIn]) {
						defer poolWg.Done()

						nextStream.Send(item)
						// return the stream to the pool
						pool <- nextStream

					}(item, nextStream, pool)
				}
			}

			// Wait until the concurrently running poolStream streams have finished draining
			poolWg.Wait()

			// Once drained, close the poolStream streams
			for _, poolStream := range poolStreamsToClose {
				poolStream.Close()
			}

			// And close the pool
			close(pool)

			// Wait until the concurrently executing operations have finished writing to the downstream
			opWg.Wait()
		},
		mapToParentObservable(source),
		opts...,
	)

	return observable
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
			func(streamWriter streams.Writer[T], options observableOptions) {
				for item := range source.Read() {
					streamWriter.Send(item)
				}
			},
			parents,
			opts...,
		)
}

func newObservable[T any](producer func(streamWriter streams.Writer[T], options observableOptions), parents []upstreamObservable, options ...ObservableOption) *Observable[T] {
	opts := newOptions()

	// Propagate observableOptions down the observable chain
	if len(parents) > 0 {
		applyOptions(&opts, parents)
	}

	// Apply observableOptions to the current observable
	for _, opt := range options {
		opt(&opts)
	}

	obs := &Observable[T]{
		mu:         new(sync.Mutex),
		opts:       opts,
		producer:   producer,
		downstream: streams.NewStream[T](),
		parents:    parents,
		subs:       newSubscribers[T](),
		metrics:    opts.metrics,
		logger:     opts.logger,
	}

	if opts.publishStrategy == Immediately {
		obs.Connect()
	}

	return obs
}

type upstreamObservable interface {
	Connect()
	cloneOptions() observableOptions
}

type Observable[T any] struct {
	mu   *sync.Mutex
	opts observableOptions
	// producer is a function that produces the upstream stream of items to be observed
	producer func(streams.Writer[T], observableOptions)
	// parent is the observable that has produced the upstream downstream of items
	parents []upstreamObservable
	// downstream is the stream that will Send items to the observer
	downstream *streams.Stream[T]
	// connected is a flag that indicates whether the observable has begun observing items from the upstream
	connected bool
	subs      *subscribers[T]
	metrics   MetricsMonitor
	logger    Logger
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

	upstream := streams.NewStream[T](o.opts.buffer)

	go func() {
		defer o.downstream.Close()

		for {
			select {
			case <-o.opts.ctx.Done():
				now := time.Now()
				o.downstream.Error(o.opts.ctx.Err())
				o.metrics.Timing(o.opts.activity, "item_backpressure", time.Since(now))
				o.metrics.Incr(o.opts.activity, "item_emitted", 1)
				o.metrics.Incr(o.opts.activity, "error_emitted", 1)
				return
			case item, ok := <-upstream.Read():
				if !ok {
					return
				}

				if item.Error() != nil && o.opts.errorStrategy == StopOnError {
					now := time.Now()
					o.downstream.Send(item)
					o.metrics.Timing(o.opts.activity, "item_backpressure", time.Since(now))
					o.metrics.Incr(o.opts.activity, "item_emitted", 1)
					o.metrics.Incr(o.opts.activity, "error_emitted", 1)
					return
				}

				if o.opts.backpressureStrategy == Drop {
					now := time.Now()
					ok := o.downstream.TrySend(item)
					o.metrics.Timing(o.opts.activity, "item_backpressure", time.Since(now))
					if ok {
						o.metrics.Incr(o.opts.activity, "item_emitted", 1)
						o.metrics.Incr(o.opts.activity, "value_emitted", 1)
					} else {
						o.metrics.Incr(o.opts.activity, "value_dropped", 1)
					}
				} else {
					now := time.Now()
					o.downstream.Send(item)
					o.metrics.Timing(o.opts.activity, "item_backpressure", time.Since(now))
					o.metrics.Incr(o.opts.activity, "item_emitted", 1)
					o.metrics.Incr(o.opts.activity, "value_emitted", 1)
				}
			}
		}
	}()

	go func() {
		defer upstream.Close()
		o.producer(upstream, o.opts)
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
	return o.opts.ctx
}

func (o *Observable[T]) setErrorStrategy(strategy ErrorStrategy) {
	o.opts.errorStrategy = strategy
}

func (o *Observable[T]) cloneOptions() observableOptions {
	return observableOptions{
		ctx:             o.opts.ctx,
		errorStrategy:   o.opts.errorStrategy,
		publishStrategy: o.opts.publishStrategy,
		// TODO: This is a temporary requirement until observables support global middlewares
		metrics: o.opts.metrics,
		// TODO: This is a temporary requirement until observables support global middlewares
		logger: o.opts.logger,
	}
}

func applyOptions(opts *observableOptions, parents []upstreamObservable) {
	ctxs := make([]context.Context, 0, len(parents))
	defaults := newOptions()
	// Certain settings need to be propagated from parent observable observableOptions to their child
	// observable observableOptions
	for _, p := range parents {
		o := p.cloneOptions()
		ctxs = append(ctxs, o.ctx)

		// Only set the following observableOptions if they deviate away from the defaults
		if o.errorStrategy != defaults.errorStrategy {
			opts.errorStrategy = o.errorStrategy
		}
		if o.publishStrategy == defaults.publishStrategy {
			opts.publishStrategy = o.publishStrategy
		}
	}

	opts.ctx = utils.CombinedContexts(ctxs...)
}

func mapToParentObservable[T any](obs ...*Observable[T]) []upstreamObservable {
	parents := make([]upstreamObservable, 0, len(obs))
	for _, o := range obs {
		parents = append(parents, o)
	}
	return parents
}
