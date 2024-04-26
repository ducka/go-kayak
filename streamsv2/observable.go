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
	// OnErrorFunc defines a function that computes a value from an error.
	OnErrorFunc                   func(error)
	OnCompleteFunc                func()
	OnNextFunc[T any]             func(v T)
	ErrorFunc                     func() error
	OperatorFunc[I any, O any]    func(source *Observable[I]) *Observable[O]
	DurationFunc[T any, R any]    func(value T) Observable[R]
	PredicateFunc[T any]          func(value T, index uint) bool
	ProjectionFunc[T any, R any]  func(value T, index uint) Observable[R]
	ComparerFunc[A any, B any]    func(prev A, curr B) int8
	ComparatorFunc[A any, B any]  func(prev A, curr B) bool
	AccumulatorFunc[A any, V any] func(acc A, value V, index uint) (A, error)
	// ProducerFunc is a function that produces elements for an observable
	ProducerFunc[T any]              func(streamWriter StreamWriter[T])
	OperationFunc[TIn any, TOut any] func(StreamReader[TIn], StreamWriter[TOut])

	ErrorStrategy        string
	BackpressureStrategy string
)

const (
	ContinueOnErrorStrategy ErrorStrategy = "continue"
	StopOnErrorStrategy     ErrorStrategy = "stop"

	DropBackpressureStrategy  BackpressureStrategy = "drop"
	BlockBackpressureStrategy BackpressureStrategy = "block"
)

type Subscriber[T any] interface {
	Stop()
	Send() chan<- Notification[T]
	ForEach() <-chan Notification[T]
	Closed() <-chan struct{}
}

type parentObservable interface {
	Connect()
	cloneOptions() observableOptions
}
type observableOptions struct {
	ctx                  context.Context
	activity             string
	backpressureStrategy BackpressureStrategy
	errorStrategy        ErrorStrategy
}

func newObservableOptions() observableOptions {
	return observableOptions{
		ctx:                  context.Background(),
		backpressureStrategy: BlockBackpressureStrategy,
		errorStrategy:        StopOnErrorStrategy,
	}
}

func (o observableOptions) Clone() observableOptions {
	return observableOptions{
		ctx:                  o.ctx,
		backpressureStrategy: o.backpressureStrategy,
		errorStrategy:        o.errorStrategy,
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

func newObservable[T any](upstreamCallback func(streamWriter StreamWriter[T], options observableOptions), options ...ObservableOption) *Observable[T] {
	return newObservableWithParent[T](upstreamCallback, nil, options...)
}

func newObservableWithParent[T any](upstreamCallback func(streamWriter StreamWriter[T], options observableOptions), parent parentObservable, options ...ObservableOption) *Observable[T] {
	opts := newObservableOptions()

	if parent != nil {
		opts = parent.cloneOptions()
	}

	for _, opt := range options {
		opt(&opts)
	}

	return &Observable[T]{
		mu:               new(sync.Mutex),
		opts:             opts,
		upstreamCallback: upstreamCallback,
		downstream:       newStream[T](),
		parent:           parent,
	}
}

// Connect starts the observation of the upstream stream
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

	upstream := newStream[T]()

	go func() {
		defer upstream.Close()
		defer o.downstream.Close()

		for {
			select {
			case <-o.opts.ctx.Done():
				return
			case item, ok := <-upstream.Read():
				if !ok {
					return
				}

				if o.opts.backpressureStrategy == DropBackpressureStrategy {
					select {
					case o.downstream.Send() <- item:
					default:
						continue
					}
				} else {
					o.downstream.Send() <- item
				}

				if item.Err() != nil && o.opts.errorStrategy == StopOnErrorStrategy {
					return
				}

				if item.Kind() == CompleteKind {
					return
				}
			}
		}
	}()

	go o.upstreamCallback(upstream, o.opts)

	o.connected = true

}

// ObserveStream observes items from a downstream
//func ObserveStream[T any](source StreamReader[T], cloneOptions ...ObservableOption) *Observable[T] {
//	return newObservable(
//		func(streamWriter StreamWriter[T], opts observableOptions) {
//			producer(streamWriter)
//		},
//		cloneOptions...,
//	)
//}

// ObserveProducer observes items produced by a callback function
func ObserveProducer[T any](producer ProducerFunc[T], options ...ObservableOption) *Observable[T] {
	return newObservable[T](
		func(streamWriter StreamWriter[T], opts observableOptions) {
			producer(streamWriter)
		},
		options...,
	)
}

func ObserveOperation[TIn any, TOut any](
	source *Observable[TIn],
	operation OperationFunc[TIn, TOut],
	options ...ObservableOption,
) *Observable[TOut] {
	observable := newObservableWithParent[TOut](
		func(streamWriter StreamWriter[TOut], opts observableOptions) {
			operation(source.Observe(), streamWriter)
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
	upstreamCallback func(StreamWriter[T], observableOptions)
	// parent is the observable that has produced the upstream downstream of items
	parent parentObservable
	// downstream is the stream that will Send items to the observer
	downstream *stream[T]
	// connected is a flag that indicates whether the observable has begun observing items from the upstream
	connected bool
}

// Observe returns a downstream reader so the caller can observe items produced by the observable
func (o *Observable[T]) Observe() StreamReader[T] {
	return o.downstream
}

// Subscribe starts the synchronous observation of the observable
func (o *Observable[T]) Subscribe(onNext OnNextFunc[T], options ...SubscribeOption) {
	subscribeOptions := &subscribeOptions{
		onError:    func(err error) {},
		onComplete: func() {},
	}

	for _, opt := range options {
		opt(subscribeOptions)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer subscribeOptions.onComplete()
		defer wg.Done()

		for item := range o.downstream.Read() {
			if item.Err() != nil {
				subscribeOptions.onError(item.Err())
				continue
			}

			if item.Done() {
				return
			}

			onNext(item.Value())
		}
	}()

	// Send a connect signal up the observable chain to start observing
	o.Connect()

	wg.Wait()
}

func (o *Observable[T]) cloneOptions() observableOptions {
	return o.opts.Clone()
}

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
