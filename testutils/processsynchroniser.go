package testutils

import (
	"sync"
)

// ProcessSynchroniser synchronises multiple concurrent processes at a checkpoint. If the checkpoint is not
// hit the required number of times, then the block will not be lifted.
type ProcessSynchroniser struct {
	muCk sync.RWMutex
	muWg sync.RWMutex
	wg   sync.WaitGroup

	reset     bool
	iter      int
	chk       int
	threshold int
}

func NewProcessSynchroniser(checkpointThreshold int) *ProcessSynchroniser {
	if checkpointThreshold < 1 {
		checkpointThreshold = 1
	}

	i := &ProcessSynchroniser{
		threshold: checkpointThreshold,
	}

	return i
}

// Checkpoint blocks the calling process until it is called by other concurrent processes enough times to release the lock.
// This concurrency threshold is defined by the checkpointThreshold parameter in the NewProcessSynchroniser function.
//
// The point of this function is to synchronise multiple concurrent processes at a checkpoint. If the checkpoint is not
// hit the required number of times, then the block will not be lifted.
func (s *ProcessSynchroniser) Checkpoint() {
	s.muCk.Lock()
	s.iter++

	if s.threshold == 1 {
		s.chk++
		s.muCk.Unlock()
		return
	}

	if s.iter%s.threshold == 0 {
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

// ReleaseCount returns the number of times the checkpoint synchronisation gate has been released.
func (s *ProcessSynchroniser) ReleaseCount() int {
	s.muCk.RLock()
	defer s.muCk.RUnlock()
	return s.chk
}

// HitCount returns the number of times the checkpoint has been hit.
func (s *ProcessSynchroniser) HitCount() int {
	s.muCk.RLock()
	defer s.muCk.RUnlock()
	return s.iter
}
