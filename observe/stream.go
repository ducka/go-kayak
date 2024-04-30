package observe

import (
	"sync"
)

type StreamWriter[T any] interface {
	Write(value T)
	Error(err error, value ...T)
	Send(notification Notification[T])
	TrySend(notification Notification[T]) bool
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
}

func newStream[T any](bufferCount ...uint64) *stream[T] {
	ch := make(chan Notification[T])
	if len(bufferCount) > 0 {
		ch = make(chan Notification[T], bufferCount[0])
	}
	return &stream[T]{
		mu: new(sync.RWMutex),
		ch: ch,
	}
}

// Next will return the channel to receive the downstream data
func (s *stream[T]) Read() <-chan Notification[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ch
}

func (s *stream[T]) Write(value T) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.send() <- Next[T](value)
}

func (s *stream[T]) Error(err error, value ...T) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(value) > 0 {
		s.send() <- Error[T](err, value[0])
		return
	}

	s.send() <- Error[T](err)
}

func (s *stream[T]) Send(notification Notification[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.ch <- notification
}

func (s *stream[T]) TrySend(notification Notification[T]) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	select {
	case s.ch <- notification:
		return true
	default:
		return false
	}
}

func (s *stream[T]) send() chan<- Notification[T] {
	return s.ch
}

func (s *stream[T]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}
