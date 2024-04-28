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
  source function would be consuming items upstream, executing an operation, and sending them on downstream.
- You can use subjects to implement fork and merge patterns.
*/

package observe

import (
	"sync"
)

type (
	OnErrorFunc                      func(error)
	OnCompleteFunc                   func()
	OnNextFunc[T any]                func(v T)
	ProducerFunc[T any]              func(streamWriter StreamWriter[T])
	OperationFunc[TIn any, TOut any] func(StreamReader[TIn], StreamWriter[TOut])

	ErrorStrategy        string
	BackpressureStrategy string
)

const (
	ContinueOnError ErrorStrategy = "continue"
	StopOnError     ErrorStrategy = "stop"

	Drop  BackpressureStrategy = "drop"
	Block BackpressureStrategy = "block"
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

func newObservable[T any](upstreamCallback func(streamWriter StreamWriter[T], options observableOptions), options ...ObservableOption) *Observable[T] {
	return newObservableWithParent[T](upstreamCallback, nil, options...)
}

func newObservableWithParent[T any](source func(streamWriter StreamWriter[T], options observableOptions), parent parentObservable, options ...ObservableOption) *Observable[T] {
	opts := newObservableOptions()

	if parent != nil {
		opts = parent.cloneOptions()
	}

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
		defer o.downstream.Close()

		for {
			select {
			case <-o.opts.ctx.Done():
				return
			case item, ok := <-upstream.Read():
				if !ok {
					return
				}

				if item.Err() != nil && o.opts.errorStrategy == StopOnError {
					o.downstream.Error(item.Err())
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

func Sequence[T any](sequence []T, options ...ObservableOption) *Observable[T] {
	return ObserveProducer[T](func(streamWriter StreamWriter[T]) {
		for _, item := range sequence {
			streamWriter.Write(item)
		}
	}, options...)
}

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
		func(downstream StreamWriter[TOut], opts observableOptions) {
			upstream := source.Observe()
			usePool := opts.poolSize > 1

			if !usePool {
				operation(upstream, downstream)
				return
			}

			// Initialise the pool of operations
			pool := make(chan *stream[TIn], opts.poolSize)
			for i := 0; i < int(opts.poolSize); i++ {
				poolStream := newStream[TIn]()
				pool <- poolStream

				go operation(poolStream, downstream)
			}

			// Send items to the next available stream in the pool
			for item := range upstream.Read() {
				select {
				case <-opts.ctx.Done():
					return
				case nextChan := <-pool:
					go func(item Notification[TIn], nextStream *stream[TIn], pool chan *stream[TIn]) {
						nextStream.Send(item)

						// return the stream to the pool
						pool <- nextStream
					}(item, nextChan, pool)
				}
			}
		},
		source,
		options...,
	)

	return observable
}

type Observable[T any] struct {
	mu   *sync.Mutex
	opts observableOptions
	// source is a function that produces the upstream stream of items to be observed
	source func(StreamWriter[T], observableOptions)
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

	o.Connect()

	wg.Wait()

	return notifications
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
