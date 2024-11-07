package testutils

import (
	"sync"
)

// ConcurrencySync synchronises a specified number of concurrently running processes at a checkpoint.
type ConcurrencySync struct {
	muCk sync.RWMutex
	muWg sync.RWMutex
	wg   sync.WaitGroup

	reset bool
	iter  int
	chk   int
	limit int
}

func NewConcurrencySync(concurrencyLimit int) *ConcurrencySync {
	if concurrencyLimit < 1 {
		panic("concurrency limit must be greater than 0")
	}

	i := &ConcurrencySync{
		limit: concurrencyLimit,
	}

	return i
}

// Checkpoint blocks the calling process until a requisite number of processes have reached the checkpoint.
func (s *ConcurrencySync) Checkpoint() {
	s.muCk.Lock()
	s.iter++

	if s.limit == 1 {
		s.chk++
		s.muCk.Unlock()
		return
	}

	if s.iter%s.limit == 0 {
		s.wg.Done()
		s.reset = false
		s.chk++
		s.muCk.Unlock()
	} else {
		if !s.reset {
			s.muWg.Lock()
			s.wg.Add(1)
			s.reset = true
			s.muWg.Unlock()
		}
		s.muCk.Unlock()
		s.muWg.RLock()
		s.wg.Wait()
		s.muWg.RUnlock()
	}
}

// ReleaseCount returns the number of times the checkpoint has been released.
func (s *ConcurrencySync) ReleaseCount() int {
	s.muCk.RLock()
	defer s.muCk.RUnlock()
	return s.chk
}

// HitCount returns the number of times the checkpoint has been hit.
func (s *ConcurrencySync) HitCount() int {
	s.muCk.RLock()
	defer s.muCk.RUnlock()
	return s.iter
}
