package streamsv2

import (
	"sync"
)

type StreamWriter[T any] interface {
	Write(value T) (bool, error)
	Error(err error)
	Complete()
}

type StreamReader[T any] interface {
	Read() <-chan Notification[T]
}
type stream[T any] struct {
	// to prevent DATA RACE
	mu *sync.RWMutex

	// channel to transfer data
	ch chan Notification[T]

	// determine the channel was closed
	closed bool

	//
	//stopping bool
	//
	//stopCh chan struct{}
}

//type subscriber[T any] struct {
//	// to prevent DATA RACE
//	mu *sync.RWMutex
//
//	// channel to transfer data
//	ch chan Notification[T]
//
//	// channel to indentify it has stopped
//	stop chan struct{}
//
//	isStopped bool
//
//	// determine the channel was closed
//	closed bool
//}

func newStream[T any](bufferCount ...uint) *stream[T] {
	ch := make(chan Notification[T])
	if len(bufferCount) > 0 {
		ch = make(chan Notification[T], bufferCount[0])
	}
	return &stream[T]{
		mu: new(sync.RWMutex),
		ch: ch,
	}
}

//func (s *subscriber[T]) Stop() {
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	if s.isStopped {
//		return
//	}
//	s.isStopped = true
//	close(s.stop)
//}
//
//func (s *subscriber[T]) Closed() <-chan struct{} {
//	s.mu.RLock()
//	defer s.mu.RUnlock()
//	return s.stop
//}

//func (s *downstream[T]) Stop() {
//	s.mu.RLock()
//	defer s.mu.RUnlock()
//	s.stopped = true
//	close(s.stopCh)
//}

// Next will return the channel to receive the downstream data
func (s *stream[T]) Read() <-chan Notification[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ch
}

func (s *stream[T]) Write(value T) (bool, error) {
	return s.send(Next[T](value))
}

func (s *stream[T]) Error(err error) {
	s.send(Error[T](err))
}

func (s *stream[T]) Complete() {
	s.send(Complete[T]())
}

// Send will send the notification to the stream
func (s *stream[T]) send(notification Notification[T]) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, &StreamClosedError{
			Message: "downstream is closed",
		}
	}

	s.ch <- notification
	return true, nil
}

func (s *stream[T]) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

type StreamClosedError struct {
	Message string
	Code    int
}

func (e *StreamClosedError) Error() string {
	return e.Message
}

// this will close the downstream and stop the emission of the downstream data
//func (s *subscriber[T]) Unsubscribe() {
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	if s.closed {
//		return
//	}
//	s.closeChannel()
//}
//
//func (s *subscriber[T]) closeChannel() {
//	s.closed = true
//	close(s.ch)
//}
//
//type safeSubscriber[T any] struct {
//	*subscriber[T]
//
//	dst Observer[T]
//}
//
//func NewSafeSubscriber[T any](onNext OnNextFunc[T], onError OnErrorFunc, onComplete OnCompleteFunc) *safeSubscriber[T] {
//	sub := &safeSubscriber[T]{
//		subscriber: NewSubscriber[T](),
//		dst:        NewObserver(onNext, onError, onComplete),
//	}
//	return sub
//}

//func NewObserver[T any](onNext OnNextFunc[T], onError OnErrorFunc, onComplete OnCompleteFunc) Observer[T] {
//	if onNext == nil {
//		onNext = func(T) {}
//	}
//	if onError == nil {
//		onError = func(error) {}
//	}
//	if onComplete == nil {
//		onComplete = func() {}
//	}
//	return &consumerObserver[T]{
//		onNext:     onNext,
//		onError:    onError,
//		onComplete: onComplete,
//	}
//}
//
//type consumerObserver[T any] struct {
//	onNext     func(T)
//	onError    func(error)
//	onComplete func()
//}
//
//func (o *consumerObserver[T]) Next(v T) {
//	o.onNext(v)
//}
//
//func (o *consumerObserver[T]) Error(err error) {
//	o.onError(err)
//}
//
//func (o *consumerObserver[T]) Complete() {
//	o.onComplete()
//}
