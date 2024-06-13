package stream

import (
	"sync"
)

type Writer[T any] interface {
	Write(value T)
	Error(err error)
	Send(notification Notification[T])
	TrySend(notification Notification[T]) bool
	Close()
}

type Reader[T any] interface {
	Read() <-chan Notification[T]
}
type Stream[T any] struct {
	// to prevent DATA RACE
	mu *sync.RWMutex

	// channel to transfer data
	ch chan Notification[T]

	// determine the channel was closed
	closed bool
}

func NewStream[T any](bufferCount ...uint64) *Stream[T] {
	ch := make(chan Notification[T])
	if len(bufferCount) > 0 {
		ch = make(chan Notification[T], bufferCount[0])
	}
	return &Stream[T]{
		mu: new(sync.RWMutex),
		ch: ch,
	}
}

// Next will return the channel to receive the downstream data
func (s *Stream[T]) Read() <-chan Notification[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ch
}

func (s *Stream[T]) Write(value T) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.send() <- Next[T](value)
}

func (s *Stream[T]) Error(err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.send() <- Error[T](err)
}

func (s *Stream[T]) Send(notification Notification[T]) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.ch <- notification
}

func (s *Stream[T]) TrySend(notification Notification[T]) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	select {
	case s.ch <- notification:
		return true
	default:
		return false
	}
}

func (s *Stream[T]) send() chan<- Notification[T] {
	return s.ch
}

func (s *Stream[T]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}
