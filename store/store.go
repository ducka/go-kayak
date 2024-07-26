package store

import (
	"context"
	"time"
)

type StateEntry[TState any] struct {
	Key       string
	State     *TState
	Timestamp *int64
}

type StateStore[TState any] interface {
	Get(ctx context.Context, keys []string) ([]StateEntry[TState], error)
	Set(ctx context.Context, entries []StateEntry[TState], options ...StoreOption) error
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

type storeOptions struct {
	Expiry *time.Duration
}

type StoreOption func(*storeOptions)

func WithExpiry(expiry time.Duration) StoreOption {
	return func(o *storeOptions) {
		o.Expiry = &expiry
	}
}

func applyOptions(defaults storeOptions, options []StoreOption) storeOptions {
	for _, opt := range options {
		opt(&defaults)
	}
	return defaults
}
