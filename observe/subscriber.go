package observe

import (
	"sync"
)

type subscriber[T any] struct {
	next     OnNextFunc[T]
	complete OnCompleteFunc
	err      OnErrorFunc
}

type subscribers[T any] struct {
	mu *sync.RWMutex
	s  []subscriber[T]
	wg *sync.WaitGroup
}

func newSubscribers[T any]() *subscribers[T] {
	return &subscribers[T]{
		mu: new(sync.RWMutex),
		s:  make([]subscriber[T], 0),
		wg: new(sync.WaitGroup),
	}
}

func (s *subscribers[T]) len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.s)
}

func (s *subscribers[T]) hasSubscribers() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.s) > 0
}

func (s *subscribers[T]) add(onNext OnNextFunc[T], errorFunc OnErrorFunc, completeFunc OnCompleteFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wg.Add(1)
	s.s = append(s.s, subscriber[T]{
		next:     onNext,
		err:      errorFunc,
		complete: completeFunc,
	})
}

func (s *subscribers[T]) dispatchNext(v T) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sub := range s.s {
		sub.next(v)
	}
}

func (s *subscribers[T]) dispatchError(err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sub := range s.s {
		sub.err(err)
	}
}

func (s *subscribers[T]) dispatchComplete(reason CompleteReason, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sub := range s.s {
		sub.complete(reason, err)
		s.wg.Done()
	}
}

func (s *subscribers[T]) WaitTillComplete() {
	s.wg.Wait()
}
