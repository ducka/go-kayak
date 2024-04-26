package streamsv2

import (
	"sync"
)

type StreamWriter[T any] interface {
	Write(value T)
	Error(err error)
	Send() chan<- Notification[T]
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
}

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

func (s *stream[T]) Error(err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.send() <- Error[T](err)
}

func (s *stream[T]) Complete() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.send() <- Complete[T]()
}

// Send will Send the notification to the stream
func (s *stream[T]) Send() chan<- Notification[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.send()
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

type StreamClosedError struct {
	Message string
	Code    int
}

func (e *StreamClosedError) Error() string {
	return e.Message
}
