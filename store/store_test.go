package store

import (
	"context"

	"github.com/ducka/go-kayak/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type StoreTestSuite struct {
	suite.Suite
	ctx       context.Context
	createSUT func() StateStore[string]
}

func NewStoreTestSuite(storeFactory func() StateStore[string]) *StoreTestSuite {
	return &StoreTestSuite{
		createSUT: storeFactory,
		ctx:       context.Background(),
	}
}

func (t *StoreTestSuite) TestGettingAndSetting() {
	sut := t.createSUT()

	// Test getting a non-existent key
	result, err := sut.Get(t.ctx, uuid.NewString())

	assert.Empty(t.T(), result)
	assert.NoError(t.T(), err)

	// Test setting a new key
	newEntry := StateEntry[string]{
		Key:   uuid.NewString(),
		State: utils.ToPtr(uuid.NewString()),
	}

	var gotEntry StateEntry[string]

	err = sut.Set(t.ctx, newEntry)

	assert.Empty(t.T(), err)

	// Test getting the new key
	g, err := sut.Get(t.ctx, newEntry.Key)
	assert.NoError(t.T(), err)
	gotEntry = g[0]
	assert.Equal(t.T(), newEntry.Key, gotEntry.Key)
	assert.Equal(t.T(), newEntry.State, gotEntry.State)
	assert.NotEmpty(t.T(), gotEntry.Timestamp)

	// Test updating an existing key
	updatedEntry := StateEntry[string]{
		Key:       newEntry.Key,
		State:     utils.ToPtr(uuid.NewString()),
		Timestamp: gotEntry.Timestamp,
	}
	err = sut.Set(t.ctx, updatedEntry)

	assert.Empty(t.T(), err)

	// Test getting the updated key
	g, _ = sut.Get(t.ctx, updatedEntry.Key)
	gotEntry = g[0]

	assert.Equal(t.T(), updatedEntry.Key, gotEntry.Key)
	assert.Equal(t.T(), updatedEntry.State, gotEntry.State)
	assert.NotEmpty(t.T(), gotEntry.Timestamp)

	// Test deleting the key
	err = sut.Set(t.ctx, StateEntry[string]{
		Key:       updatedEntry.Key,
		State:     nil,
		Timestamp: gotEntry.Timestamp,
	})
	assert.Empty(t.T(), err)

	// Test getting a non-existent key
	result, err = sut.Get(t.ctx, updatedEntry.Key)

	assert.Empty(t.T(), result)
	assert.NoError(t.T(), err)

}

func (t *StoreTestSuite) TestOptimisticConcurrency() {
	sut := t.createSUT()

	newEntry := StateEntry[string]{
		Key:   uuid.NewString(),
		State: utils.ToPtr(uuid.NewString()),
	}

	err := sut.Set(t.ctx, newEntry)
	assert.NoError(t.T(), err)

	// Retrieve the set key from the store
	retrievedEntries, err := sut.Get(t.ctx, newEntry.Key)
	assert.NotEmpty(t.T(), retrievedEntries)
	assert.NoError(t.T(), err)

	// Set the retrieved entry back to the store. This should succeed.
	err = sut.Set(t.ctx, retrievedEntries...)
	assert.NoError(t.T(), err)

	// Set the retrieved entry back to the store again. This should fail because the timestamp has increased with the previous set.
	err = sut.Set(t.ctx, retrievedEntries...)

	if conflict := err.(*StateStoreConflict); conflict != nil {
		assert.Contains(t.T(), conflict.GetConflicts(), newEntry.Key)
	} else {
		assert.Fail(t.T(), "Expected a StateStoreConflict error")
	}
}

//func (t *StoreTestSuite) TestEntryExpiry() {
//	sut := t.createSUT()
//
//	newEntry := StateEntry[string]{
//		Key:    uuid.NewString(),
//		State:  utils.ToPtr(uuid.NewString()),
//		Expiry: utils.ToPtr(1 * time.Second),
//	}
//
//	err := sut.Set(t.ctx, newEntry)
//	assert.NoError(t.T(), err)
//
//	// Retrieve the set key from the store
//	retrievedEntries, err := sut.Get(t.ctx, newEntry.Key)
//	assert.Len(t.T(), retrievedEntries, 1)
//	assert.NoError(t.T(), err)
//	assert.Equal(t.T(), retrievedEntries[0].Key, newEntry.Key)
//
//	time.Sleep(2 * time.Second)
//
//	retrievedEntries, err = sut.Get(t.ctx, newEntry.Key)
//	assert.Empty(t.T(), retrievedEntries)
//	assert.NoError(t.T(), err)
//}
