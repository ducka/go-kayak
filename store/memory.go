package store

import (
	"context"
	"sync"
	"time"

	"github.com/ducka/go-kayak/utils"
)

type InMemoryStore[T any] struct {
	store map[string]inMemoryStateEntryWrapper[T]
	mu    *sync.RWMutex
	done  chan struct{}
}

func NewInMemoryStore[T any]() *InMemoryStore[T] {
	i := &InMemoryStore[T]{
		store: make(map[string]inMemoryStateEntryWrapper[T]),
		mu:    new(sync.RWMutex),
	}

	// Periodically purge the store of expired entries
	go func() {
		for {
			select {
			case <-i.done:
				return
			case now := <-time.After(10 * time.Second):
				i.mu.Lock()
				for key, entry := range i.store {
					if entry.ExpireOn != nil && entry.ExpireOn.Before(now) {
						delete(i.store, key)
					}
				}
				i.mu.Unlock()
			}
		}
	}()

	return i
}

func (i *InMemoryStore[T]) Get(ctx context.Context, keys []string) ([]StateEntry[T], error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	result := make([]StateEntry[T], 0, len(keys))
	for _, key := range keys {
		if entry, ok := i.store[key]; ok {

			// Don't return expired entries
			if entry.ExpireOn != nil && entry.ExpireOn.Before(time.Now()) {
				continue
			}

			result = append(result, entry.StateEntry)
		}
	}
	return result, nil
}

func (i *InMemoryStore[T]) Set(ctx context.Context, entries []StateEntry[T], options ...StoreOption) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	opts := applyOptions(storeOptions{}, options)

	conflicts := make([]string, 0)

	for _, entry := range entries {
		if stored, ok := i.store[entry.Key]; ok {
			if stored.Timestamp != entry.Timestamp {
				conflicts = append(conflicts, entry.Key)
				continue
			}

			if entry.State == nil {
				delete(i.store, entry.Key)
				continue
			}
		}

		// Write the entry to the stores
		entry.Timestamp = utils.ToPtr(time.Now().Unix())
		wrapper := inMemoryStateEntryWrapper[T]{
			StateEntry: entry,
		}

		if opts.Expiry != nil {
			wrapper.ExpireOn = utils.ToPtr(time.Now().Add(*opts.Expiry))
		}

		i.store[entry.Key] = wrapper
	}

	if len(conflicts) > 0 {
		return &StateStoreConflict{conflicts: conflicts}
	}

	return nil
}

type inMemoryStateEntryWrapper[T any] struct {
	StateEntry[T]
	ExpireOn *time.Time
}
