package testutils

import (
	"sync"
)

// CheckpointSync synchronises a specified number of concurrently running processes at a checkpoint.
type CheckpointSync struct {
	muCk sync.RWMutex
	muWg sync.RWMutex
	wg   sync.WaitGroup

	reset bool
	iter  int
	chk   int
	limit int
}

func NewCheckpointSync(concurrencyThreshold int) *CheckpointSync {
	if concurrencyThreshold < 1 {
		panic("concurrency threshold must be greater than 0")
	}

	i := &CheckpointSync{
		limit: concurrencyThreshold,
	}

	return i
}

// Checkpoint blocks the calling process until it is called by other concurrent processes enough times to release the lock.
// This concurrency threshold is defined by the concurrencyThreshold parameter in the NewCheckpointSync function.
//
// The point of this function is to synchronise multiple concurrent processes at a checkpoint. If the checkpoint is not
// hit the required number of times, then the block will not be lifted.
func (s *CheckpointSync) Checkpoint() {
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
func (s *CheckpointSync) ReleaseCount() int {
	s.muCk.RLock()
	defer s.muCk.RUnlock()
	return s.chk
}

// HitCount returns the number of times the checkpoint has been hit.
func (s *CheckpointSync) HitCount() int {
	s.muCk.RLock()
	defer s.muCk.RUnlock()
	return s.iter
}
