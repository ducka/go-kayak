package observe

import (
	"context"
	"sync"
)

type (
	OnErrorFunc                      func(error)
	OnCompleteFunc                   func(reason CompleteReason, err error)
	OnNextFunc[T any]                func(v T)
	ProducerFunc[T any]              func(streamWriter StreamWriter[T])
	OperationFunc[TIn any, TOut any] func(StreamReader[TIn], StreamWriter[TOut])

	ErrorStrategy        string
	BackpressureStrategy string
	CompleteReason       string
)

const (
	ContinueOnError ErrorStrategy = "continue"
	StopOnError     ErrorStrategy = "stop"

	Drop  BackpressureStrategy = "drop"
	Block BackpressureStrategy = "block"

	Failed    CompleteReason = "failure"
	Completed CompleteReason = "completed"
)

// Sequence observes an array of values
func Sequence[T any](sequence []T, opts ...Option) *Observable[T] {
	return Producer[T](func(streamWriter StreamWriter[T]) {
		for _, item := range sequence {
			streamWriter.Write(item)
		}
	}, opts...)
}

// Producer observes items produced by a callback function
func Producer[T any](producer ProducerFunc[T], opts ...Option) *Observable[T] {
	return newObservable[T](
		func(streamWriter StreamWriter[T], opts options) {
			producer(streamWriter)
		},
		opts...,
	)
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

			if !usePool {
				operation(upstream, downstream)
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
					operation(poolStream, downstream)
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
		source,
		opts...,
	)

	return observable
}

func newObservable[T any](upstreamCallback func(streamWriter StreamWriter[T], options options), options ...Option) *Observable[T] {
	return newObservableWithParent[T](upstreamCallback, nil, options...)
}

func newObservableWithParent[T any](source func(streamWriter StreamWriter[T], options options), parent parentObservable, options ...Option) *Observable[T] {
	opts := newOptions()

	// Propagate options down the observable chain
	if parent != nil {
		opts = parent.cloneOptions()
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
		parent:     parent,
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
	parent parentObservable
	// downstream is the stream that will Send items to the observer
	downstream *stream[T]
	// connected is a flag that indicates whether the observable has begun observing items from the upstream
	connected bool
}

// Observe starts the observation of the upstream stream
func (o *Observable[T]) Observe() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.connected {
		return
	}

	// Propagate connect up the observable chain
	if o.parent != nil {
		o.parent.Observe()
	}

	upstream := newStream[T]()

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

				if item.Err() != nil && o.opts.errorStrategy == StopOnError {
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
			if item.Err() != nil {
				err = item.Err()
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
