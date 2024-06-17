package store

import (
	"context"
	"sync"
	"time"

	"github.com/ducka/go-kayak/utils"
)

type InMemoryStore[T any] struct {
	store map[string]StateEntry[T]
	mu    *sync.RWMutex
}

func NewInMemoryStore[T any]() *InMemoryStore[T] {
	return &InMemoryStore[T]{
		store: make(map[string]StateEntry[T]),
		mu:    new(sync.RWMutex),
	}
}

func (i *InMemoryStore[T]) Get(ctx context.Context, keys ...string) (map[string]StateEntry[T], error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	result := make(map[string]StateEntry[T])
	for _, key := range keys {
		if entry, ok := i.store[key]; ok {
			result[key] = entry
		}
	}
	return result, nil
}

func (i *InMemoryStore[T]) Set(ctx context.Context, entries map[string]StateEntry[T]) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	conflicts := make([]string, 0)

	for key, entry := range entries {
		if stored, ok := i.store[key]; ok {
			if stored.Timestamp != entry.Timestamp {
				conflicts = append(conflicts, key)
				continue
			}
		}

		entry.Timestamp = utils.ToPtr(time.Now().Unix())
		i.store[key] = entry
	}

	if len(conflicts) > 0 {
		return &StateStoreConflict{conflicts: conflicts}
	}

	return nil
}
