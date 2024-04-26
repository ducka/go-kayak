/*
TODO
- An observable produces items to be observed
- A pipeline is essentially a chain of observables, starting from the observerable producing the original
  data, and subsequent observables passing through the data.
- Operations are essentially observables internally that make some sort of modification or perform some sort
  operation on the data as it passes through the observable.
- You should be able to implement a "connect" capability by having a connect function on each observable. Internally
  the connect function would call the connection function of the parent observable first, and then it would execute the
  go routine that would produce items for itself. In the case of an observable that sits in the middle of a pipeline, its
  upstreamCallback function would be consuming items upstream, executing an operation, and sending them on downstream.
- You can use subjects to implement fork and merge patterns.
*/

package streamsv2

import (
	"context"
	"sync"
)

type (
	OnNextFunc[T any] func(T)

	// OnErrorFunc defines a function that computes a value from an error.
	OnErrorFunc                   func(error)
	OnCompleteFunc                func()
	FinalizerFunc                 func()
	ErrorFunc                     func() error
	OperatorFunc[I any, O any]    func(source *Observable[I]) *Observable[O]
	DurationFunc[T any, R any]    func(value T) Observable[R]
	PredicateFunc[T any]          func(value T, index uint) bool
	ProjectionFunc[T any, R any]  func(value T, index uint) Observable[R]
	ComparerFunc[A any, B any]    func(prev A, curr B) int8
	ComparatorFunc[A any, B any]  func(prev A, curr B) bool
	AccumulatorFunc[A any, V any] func(acc A, value V, index uint) (A, error)
	// ProducerFunc is a function that produces elements for an observable
	ProducerFunc[T any] func(streamWriter StreamWriter[T])
)

//type Subscription interface {
//	// allow user to unsubscribe the downstream manually
//	Unsubscribe()
//}

//type Observer[T any] interface {
//	Next(T)
//	Error(error)
//	Complete()
//}

type Subscriber[T any] interface {
	Stop()
	Send() chan<- Notification[T]
	ForEach() <-chan Notification[T]
	Closed() <-chan struct{}
	// Unsubscribe()
	// Observer[T]
}

type ObservableConnector interface {
	Connect()
}

//type Subject[T any] interface {
//	Subscriber[T]
//	Subscription
//}

type observableOptions struct {
	ctx context.Context
}

type ObservableOption func(options observableOptions)

func newObservable[T any](upstreamCallback func() StreamReader[T], options ...ObservableOption) *Observable[T] {
	return newObservableWithParent[T](upstreamCallback, nil, options...)
}

func newObservableWithParent[T any](upstreamCallback func() StreamReader[T], parentObservable ObservableConnector, options ...ObservableOption) *Observable[T] {
	opts := observableOptions{
		ctx: context.Background(),
	}

	for _, opt := range options {
		opt(opts)
	}

	return &Observable[T]{
		mu:               new(sync.Mutex),
		opts:             opts,
		upstreamCallback: upstreamCallback,
		downstream:       newStream[T](),
		parent:           parentObservable,
	}
}

// connect starts the observation of the upstream stream
func (o *Observable[T]) Connect() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.connected {
		return
	}

	// Propagate connect up the observable chain
	if o.parent != nil {
		o.parent.Connect()
	}

	go func() {
		defer o.downstream.close()

		upstream := o.upstreamCallback()

		for {
			select {
			case <-o.opts.ctx.Done():
				return
			case item, ok := <-upstream.Read():
				if !ok {
					return
				}

				//if item.Kind() == CompleteKind {
				//	upstream.close()
				//}

				closed, _ := o.downstream.send(item)

				if closed {

					// TODO: What do you do here when the downstream downstream is closed? Propagate it back up?
				}
			}
		}
	}()

	o.connected = true
}

// ObserveStream observes items from a downstream
func ObserveStream[T any](source StreamReader[T], options ...ObservableOption) *Observable[T] {
	return newObservable(
		func() StreamReader[T] {
			return source
		},
		options...,
	)
}

// ObserveProducer observes items produced by a callback function
func ObserveProducer[T any](producer ProducerFunc[T], options ...ObservableOption) *Observable[T] {
	return newObservable[T](
		func() StreamReader[T] {
			stream := newStream[T]()

			go producer(stream)

			return stream
		},
		options...,
	)
}

func ObserveOperation[TIn any, TOut any](
	source *Observable[TIn],
	producer func(StreamReader[TIn], StreamWriter[TOut]),
	options ...ObservableOption,
) *Observable[TOut] {
	observable := newObservableWithParent[TOut](
		func() StreamReader[TOut] {
			downstream := newStream[TOut]()

			producer(source.Observe(), downstream)

			return downstream
		},
		source,
		options...,
	)

	return observable
}

type Observable[T any] struct {
	mu   *sync.Mutex
	opts observableOptions
	// upstreamCallback is a function that produces the upstream downstream of items
	upstreamCallback func() StreamReader[T]
	// parent is the observable that has produced the upstream downstream of items
	parent ObservableConnector
	// downstream is the stream that will send items to the observer
	downstream *stream[T]
	// connected is a flag that indicates whether the observable has begun observing items from the upstream
	connected bool
}

// Observe returns a downstream reader so the caller can observe items produced by the observable
func (o *Observable[T]) Observe() StreamReader[T] {
	return o.downstream
}

// Subscribe starts the synchronous observation of the observable
func (o *Observable[T]) Subscribe(onNext func(v T), onError func(err error), onComplete func()) {
	defer onComplete()
	o.Connect()

	for item := range o.downstream.Read() {
		if item.Err() != nil {
			onError(item.Err())
			return
		}

		if item.Done() {
			return
		}

		onNext(item.Value())
	}
}

/*


// NB: Passes a subscriber (essentially a channel) to the source (the upstreamCallback of elements) to start producing.
func (o *Observable[T]) SubscribeWith(subscriber Subscriber[T]) {
	o.source(subscriber)
}

// NB: The cb is a callback that lets the caller know when the subscription to the source has been completed.
//
//	SubscribeOn essentially creates a channel (subscriber) and passes it to the source (upstreamCallback of elements) to start producing. The channel / subscriber is returned.
func (o *Observable[T]) SubscribeOn(cb ...func()) Subscriber[T] {
	var subscriber Subject[T]
	if o.connect != nil {
		subscriber = o.connect()
	} else {
		subscriber = NewSubscriber[T]()
	}
	finalizer := func() {}
	if len(cb) > 0 {
		finalizer = cb[0]
	}
	go func() {
		defer subscriber.Unsubscribe()
		defer finalizer()
		o.source(subscriber)
	}()
	return subscriber
}

func (o *Observable[T]) Subscribe(onNext func(T), onError func(error), onComplete func()) {
	subscriber := NewSafeSubscriber(onNext, onError, onComplete)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		o.source(subscriber)
	}()
	go func() {
		defer wg.Done()
		consumeStreamUntil(o.ctx, subscriber, func() {})
	}()
	wg.Wait()
}

func consumeStreamUntil[T any](ctx context.Context, sub *safeSubscriber[T], finalizer FinalizerFunc) {
	defer sub.Unsubscribe()
	defer finalizer()

observe:
	for {
		select {
		// If context cancelled, shut down everything
		case <-ctx.Done():
			sub.dst.Error(ctx.Err())

		case <-sub.Closed():
			break observe

		case item, ok := <-sub.ForEach():
			if !ok {
				break observe
			}

			// handle `Error` notification
			if err := item.Err(); err != nil {
				sub.dst.Error(err)
				break observe
			}

			// handle `Complete` notification
			if item.Done() {
				sub.dst.Complete()
				break observe
			}

			sub.dst.Next(item.Value())
		}
	}
}
*/
