package store

import (
	"context"
	"time"
)

type StateEntry[TState any] struct {
	Key       string
	State     *TState
	Timestamp *int64
	Expiry    *time.Duration
}

type StateStore[TState any] interface {
	Get(ctx context.Context, keys ...string) ([]StateEntry[TState], error)
	Set(ctx context.Context, entries ...StateEntry[TState]) error
}

type StateStoreConflict struct {
	conflicts []string
}

func (s *StateStoreConflict) Error() string {
	return "State entry was modified concurrently"
}

func (s *StateStoreConflict) GetConflicts() []string {
	return s.conflicts
}
