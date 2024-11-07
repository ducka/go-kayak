package utils

import (
	"sync"
	"time"
)

// WaitFor waits for the WaitGroup to be done or a timeout elapses
func WaitFor(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()
	select {
	case <-c:
		return true
	case <-time.After(timeout):
		return false
	}
}
