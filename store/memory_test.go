package store

import (
	"context"
	"testing"
	"time"

	"github.com/ducka/go-kayak/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type StoreTestSuite[TStoreState any] struct {
	suite.Suite
	ctx           context.Context
	existingKey   string
	existingEntry StateEntry[TStoreState]
	newEntry      StateEntry[TStoreState]
	createSUT     func() StateStore[TStoreState]
}

func NewStoreTestSuite[TStoreState any](storeFactory func() StateStore[TStoreState]) *StoreTestSuite[TStoreState] {
	return &StoreTestSuite[TStoreState]{
		createSUT: storeFactory,
	}
}

func (t *StoreTestSuite[TStoreState]) SetupTest() {
	t.ctx = context.Background()
	t.existingKey = "existing-key"
	t.existingEntry = StateEntry[string]{
		Key:       t.existingKey,
		State:     utils.ToPtr("existing-value"),
		Timestamp: utils.ToPtr(time.Now().Unix()),
		Expiry:    nil,
	}
	t.newEntry = StateEntry[string]{
		Key:       "new-key",
		State:     utils.ToPtr("new-value"),
		Timestamp: utils.ToPtr(time.Now().Unix()),
		Expiry:    nil,
	}
}

func (t *StoreTestSuite[TStoreState]) TestInMemoryStore() {
	t.Run("Given a state store", func() {
		sut := t.createSUT()

		t.Run("When getting a non-existent key from the store", func() {
			result, _ := sut.Get(t.ctx, "non-existent")

			t.Run("Then the result is empty", func() {
				assert.Empty(t.T(), result)
			})
		})

		t.Run("When setting a new key to the store", func() {
			newEntry := StateEntry[TStoreState]{
				Key:   uuid.NewString(),
				State: new(TStoreState),
			}

			err := sut.Set(t.ctx, newEntry)

			t.Run("Then no error should occur", func() {
				assert.Empty(t.T(), err)
			})

			t.Run("Then when getting an existing key from the store", func() {
				existingEntry, _ := sut.Get(t.ctx, newEntry.Key)

				t.Run("Then the result is the existing entry", func() {
					assert.Equal(t.T(), newEntry, existingEntry)
				})
			})
		})
	})

	t.Run("Given an in-memory store", func() {
		sut := t.createSUT()

		t.Run("When setting an entry to the store", func() {
			err := sut.Set(t.ctx, map[string]StateEntry[string]{
				t.newEntry.Key: t.newEntry,
			})

			t.Run("Then no error should occur", func() {
				assert.Empty(t.T(), err)
			})

			t.Run("Then the entry should be set in the store", func() {
				assert.Equal(t.T(), t.newEntry, sut.store[t.newEntry.Key])
			})
		})
	})

	t.Run("Given an in-memory store", func() {
		sut := t.createSUT()

		t.Run("When updating a pre-existing entry", func() {
			updatedEntry := StateEntry[string]{
				Key:       t.existingKey,
				State:     utils.ToPtr("updated-value"),
				Timestamp: t.existingEntry.Timestamp,
			}

			err := sut.Set(t.ctx, map[string]StateEntry[string]{
				t.existingKey: updatedEntry,
			})

			t.Run("Then no error should occur", func() {
				assert.Empty(t.T(), err)
			})

			t.Run("Then the entry should be set in the store", func() {
				assert.Equal(t.T(), updatedEntry.Key, sut.store[t.existingKey].Key)
				assert.Equal(t.T(), updatedEntry.State, sut.store[t.existingKey].State)
			})
		})
	})

	t.Run("Given an in-memory store", func() {
		sut := t.createSUT()

		t.Run("When updating a pre-existing entry that has been written to by another process", func() {
			updatedEntry := StateEntry[string]{
				Key:       t.existingKey,
				State:     utils.ToPtr("updated-value"),
				Timestamp: t.existingEntry.Timestamp,
			}

			// Simulate a concurrently updated state entry
			existingEntry := sut.store[t.existingKey]
			existingEntry.Timestamp = utils.ToPtr(int64(1234))
			sut.store[t.existingKey] = existingEntry

			// Perform the set operation
			err := sut.Set(t.ctx, map[string]StateEntry[string]{
				t.existingKey: updatedEntry,
			})

			t.Run("Then a concurrency error should occur", func() {
				assert.Error(t.T(), err)
				assert.IsType(t.T(), &StateStoreConflict{}, err)

				if conflictErr, ok := err.(*StateStoreConflict); ok {
					assert.Contains(t.T(), conflictErr.GetConflicts(), t.existingKey)
				}
			})

			t.Run("Then the existing entry should remain unchanged", func() {
				assert.Equal(t.T(), t.existingEntry.State, sut.store[t.existingKey].State)
			})
		})
	})
}

func TestInMemoryStoreTestSuite(t *testing.T) {
	testSuite := NewStoreTestSuite[string](func() StateStore[string] {
		return NewInMemoryStore[string]()
	})
	suite.Run(t, testSuite)
}
