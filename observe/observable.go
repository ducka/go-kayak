package observe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type (
	OnErrorFunc                      func(error)
	OnCompleteFunc                   func(reason CompleteReason, err error)
	OnNextFunc[T any]                func(v T)
	ProducerFunc[T any]              func(streamWriter StreamWriter[T])
	OperationFunc[TIn any, TOut any] func(Context, StreamReader[TIn], StreamWriter[TOut])
	StopFunc                         func()

	ErrorStrategy        string
	BackpressureStrategy string
	CompleteReason       string
)

type Context struct {
	context.Context
	Activity string
}

const (
	ContinueOnError ErrorStrategy = "continue"
	StopOnError     ErrorStrategy = "stop"

	Drop  BackpressureStrategy = "drop"
	Block BackpressureStrategy = "block"

	Failed    CompleteReason = "failure"
	Completed CompleteReason = "completed"
)

// Producer observes items produced by a callback function
func Producer[T any](producer ProducerFunc[T], opts ...Option) *Observable[T] {
	return newObservable[T](
		func(streamWriter StreamWriter[T], opts options) {
			producer(streamWriter)
		},
		opts...,
	)
}

func Cron(cronPattern string) (*Observable[time.Time], StopFunc) {
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

	return Producer[time.Time](func(streamWriter StreamWriter[time.Time]) {

		for {
			next := schedule.Next(time.Now())
			select {
			case <-time.After(time.Until(next)):
				streamWriter.Write(next)
			case <-stopCh:
				return
			}
		}
	}), stopper
}

// Timer is an observable that emits items on a specified interval
func Timer(interval time.Duration) (*Observable[time.Time], StopFunc) {
	ticker := time.NewTicker(interval)
	done := make(chan interface{})
	stopper := func() {
		defer close(done)
		ticker.Stop()
	}
	return Producer[time.Time](func(streamWriter StreamWriter[time.Time]) {
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case t := <-ticker.C:
				streamWriter.Write(t)
			}
		}
	}), stopper
}

func Range(start, count int, opts ...Option) *Observable[int] {
	return Producer[int](func(streamWriter StreamWriter[int]) {
		for i := start; i < start+count; i++ {
			streamWriter.Write(i)
		}
	}, opts...)
}

func Stream[T any](opts ...Option) (StreamWriter[T], *Observable[T]) {
	source := newStream[T]()

	return source,
		newObservable[T](
			func(streamWriter StreamWriter[T], options options) {
				for item := range source.Read() {
					streamWriter.Send(item)
				}
			},
			opts...,
		)
}

// Sequence observes an array of values
func Sequence[T any](sequence []T, opts ...Option) *Observable[T] {
	return Producer[T](func(streamWriter StreamWriter[T]) {
		for _, item := range sequence {
			streamWriter.Write(item)
		}
	}, opts...)
}

// Operation observes items produce by an streams processing operation. This observable provides an operation callback that
// provides the opportunity to manipulate data in the stream before sending it to a downstream observer. This function allows
// you to change the type of an Observable from one type to another.
func Operation[TIn any, TOut any](
	source *Observable[TIn],
	operation OperationFunc[TIn, TOut],
	opts ...Option,
) *Observable[TOut] {
	observable := newObservableWithParent[TOut](
		func(downstream StreamWriter[TOut], opts options) {
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
			pool := make(chan *stream[TIn], opts.poolSize)
			poolStreamsToClose := make([]*stream[TIn], opts.poolSize)
			for i := 0; i < opts.poolSize; i++ {
				poolStream := newStream[TIn]()
				pool <- poolStream
				poolStreamsToClose[i] = poolStream

				opWg.Add(1)
				go func(poolStream StreamReader[TIn], downstream StreamWriter[TOut]) {
					defer opWg.Done()
					operation(ctx, poolStream, downstream)
				}(poolStream, downstream)
			}

			// Send items to the next available stream in the pool
			for item := range upstream.Read() {
				select {
				case <-opts.ctx.Done():
					return
				case nextStream := <-pool:
					poolWg.Add(1)

					go func(item Notification[TIn], nextStream *stream[TIn], pool chan *stream[TIn]) {
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
		convertObservable(source),
		opts...,
	)

	return observable
}

func newObservable[T any](upstreamCallback func(streamWriter StreamWriter[T], options options), options ...Option) *Observable[T] {
	return newObservableWithParent[T](upstreamCallback, nil, options...)
}

func applyOptions(opts *options, parents []parentObservable) {
	ctxs := make([]context.Context, 0, len(parents))
	// Certain settings need to be propagated from parent observable options to their child
	// observable options
	for _, p := range parents {
		o := p.cloneOptions()
		ctxs = append(ctxs, o.ctx)
		if o.errorStrategy == ContinueOnError {
			opts.errorStrategy = ContinueOnError
		}
	}

	opts.ctx = combinedContexts(ctxs...)
}

func combinedContexts(ctxs ...context.Context) context.Context {
	combinedCtx, cancel := context.WithCancel(context.Background())

	for _, ctx := range ctxs {
		go func(ctx context.Context) {
			defer cancel()

			<-ctx.Done()
		}(ctx)
	}

	return combinedCtx
}

func convertObservable[T any](obs ...*Observable[T]) []parentObservable {
	parents := make([]parentObservable, 0, len(obs))
	for _, o := range obs {
		parents = append(parents, o)
	}
	return parents
}

func newObservableWithParent[T any](source func(streamWriter StreamWriter[T], options options), parents []parentObservable, options ...Option) *Observable[T] {
	opts := newOptions()

	// Propagate options down the observable chain
	if len(parents) > 0 {
		applyOptions(&opts, parents)
	}

	// Apply options to the current observable
	for _, opt := range options {
		opt(&opts)
	}

	return &Observable[T]{
		mu:         new(sync.Mutex),
		opts:       opts,
		source:     source,
		downstream: newStream[T](),
		parents:    parents,
	}
}

type parentObservable interface {
	Observe()
	cloneOptions() options
}

type Observable[T any] struct {
	mu   *sync.Mutex
	opts options
	// source is a function that produces the upstream stream of items to be observed
	source func(StreamWriter[T], options)
	// parent is the observable that has produced the upstream downstream of items
	parents []parentObservable
	// downstream is the stream that will Send items to the observer
	downstream *stream[T]
	// connected is a flag that indicates whether the observable has begun observing items from the upstream
	connected         bool
	onNextSubscribers []OnNextFunc[T]
}

// Observe starts the observation of the upstream stream
func (o *Observable[T]) Observe() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.connected {
		return
	}

	// Propagate connect up the observable chain
	if len(o.parents) > 0 {
		for _, parent := range o.parents {
			parent.Observe()
		}
	}

	upstream := newStream[T](o.opts.buffer)

	go func() {
		defer o.downstream.Close()

		for {
			select {
			case <-o.opts.ctx.Done():
				o.downstream.Error(o.opts.ctx.Err())
				return
			case item, ok := <-upstream.Read():
				if !ok {
					return
				}

				if item.Error() != nil && o.opts.errorStrategy == StopOnError {
					o.downstream.Send(item)
					return
				}

				if o.opts.backpressureStrategy == Drop {
					o.downstream.TrySend(item)
				} else {
					o.downstream.Send(item)
				}
			}
		}
	}()

	go func() {
		defer upstream.Close()
		o.source(upstream, o.opts)
	}()

	o.connected = true
}

// ToStream returns a downstream StreamReader so the observable can be observed asynchronously
func (o *Observable[T]) ToStream() StreamReader[T] {
	return o.downstream
}

// ToResult synchronously observes the observable and returns the emitted items. This function will block until the observable
// completes observing the upstream sequence.
func (o *Observable[T]) ToResult() []Notification[T] {
	notifications := make([]Notification[T], 0)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for item := range o.downstream.Read() {
			notifications = append(notifications, item)
		}
		wg.Done()
	}()

	o.Observe()

	wg.Wait()

	return notifications
}

// TODO: Support multiple subscriber callbacks, not just one.
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

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		var err error

		for item := range o.downstream.Read() {
			if item.Error() != nil {
				err = item.Error()
				subscribeOptions.onError(err)
				continue
			}

			onNext(item.Value())
		}

		reason := Completed
		if err != nil {
			reason = Failed
		}

		subscribeOptions.onComplete(reason, err)
	}()

	// Trigger the observation process of the upstream sequence
	o.Observe()

	wg.Wait()
}

func (o *Observable[T]) getContext() context.Context {
	return o.opts.ctx
}

func (o *Observable[T]) setErrorStrategy(strategy ErrorStrategy) {
	o.opts.errorStrategy = strategy
}

func (o *Observable[T]) cloneOptions() options {
	return options{
		ctx:           o.opts.ctx,
		errorStrategy: o.opts.errorStrategy,
	}
}
