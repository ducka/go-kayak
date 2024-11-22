package observe

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ducka/go-kayak/streams"
	"github.com/ducka/go-kayak/testutils"
	"github.com/ducka/go-kayak/utils"
	"github.com/stretchr/testify/assert"
)

type partitionedPoolingStrategyTestCase struct {
	name           string
	ItemsToProcess int
	poolSize       int
	BufferSize     int
	HashFunc       HashFunc
	ShouldTimeOut  bool
}

// 1) Test that a partially filled pool will still process all items.
// 2) Test that a pools partitions can be filled up completely (requires blocking).
// 3) Assert the slot the item is processed in is correct (only for partitioned pooling).
// 4) Assert the next available pool is always used (only for round robin pooling).
// 5) Assert all items are processed. Order is not important.

func Test_PartitionedPoolingStrategy_Execute(t *testing.T) {

	var passThroughHashFunc HashFunc = func(key string) uint16 {
		i, _ := strconv.Atoi(key)
		return uint16(i)
	}

	for _, test := range []partitionedPoolingStrategyTestCase{
		// Testing the boundaries of the pool size
		{
			name:           "check all partitions can be filled with items",
			ItemsToProcess: 5,
			poolSize:       5,
			BufferSize:     0,
			HashFunc:       passThroughHashFunc,
			ShouldTimeOut:  false,
		},
		{
			name:           "check that a single partition can be filled with items",
			ItemsToProcess: 1,
			poolSize:       1,
			BufferSize:     0,
			HashFunc:       passThroughHashFunc,
			ShouldTimeOut:  false,
		},
		{
			name:           "check that a single partition can be filled with multiple items",
			ItemsToProcess: 5,
			poolSize:       1,
			BufferSize:     0,
			HashFunc:       passThroughHashFunc,
			ShouldTimeOut:  false,
		},
		{
			ItemsToProcess: 1000,
			poolSize:       5,
			BufferSize:     0,
			HashFunc:       passThroughHashFunc,
			ShouldTimeOut:  false,
		},
		{
			// This is a sanity check to verify CheckpointSync is working as intended
			name:           "verify that CheckpointSync will block if the number of items to process is not divisible by the pool size",
			ItemsToProcess: 1,
			poolSize:       5,
			BufferSize:     0,
			HashFunc:       passThroughHashFunc,
			ShouldTimeOut:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewContext(context.Background(), "test")
			keySelector := func(i int) string { return strconv.Itoa(i) }

			settings := ParitionedPoolSettings{
				PoolSize:   utils.ToPtr(test.poolSize),
				BufferSize: utils.ToPtr(test.BufferSize),
				HashFunc:   utils.ToPtr(test.HashFunc),
			}

			sut := NewPartitionedPoolingStrategy[int, int](keySelector, settings)

			wg := sync.WaitGroup{}
			wg.Add(2)

			// Producer
			upstream := streams.NewStream[int]()
			go func() {
				for i := 0; i < test.ItemsToProcess; i++ {
					upstream.Write(i)
				}
				defer upstream.Close()
				defer wg.Done()
			}()

			// Processor
			concurrencySync := testutils.NewCheckpointSync(int(test.poolSize))

			downstream := streams.NewStream[int]()
			operation := func(ctx Context, streamReader streams.Reader[int], streamWriter streams.Writer[int]) {
				slot := ctx.Value(slotCtxKey).(int)
				for item := range streamReader.Read() {
					// Ensure the item has been hashed to the correct slot
					assert.Equal(
						t,
						slot,
						int(test.HashFunc(strconv.Itoa(item.Value()))%uint16(test.poolSize)),
						"item %d was found in the incorrect slot %d",
						item.Value(),
						slot,
					)
					concurrencySync.Checkpoint()
					streamWriter.Send(item)
				}
			}
			go func() {
				sut.Execute(ctx, operation, upstream, downstream)
				defer downstream.Close()
			}()

			// Consumer
			go func() {
				collector := 0
				counter := 0
				for item := range downstream.Read() {
					collector = collector + (item.Value() + 1)
					counter++
				}

				assert.Equal(t, arithmeticSum(test.ItemsToProcess), collector, "the sequence from 0 to %d was not complete", test.ItemsToProcess)
				assert.Equal(t, test.ItemsToProcess, counter, "there are items missing from the sequence")
				// intentionally rounded down to the nearest int, as we're not interested in partially filled batches
				numOfPooledBatches := test.ItemsToProcess / int(test.poolSize)
				assert.Equal(t, concurrencySync.ReleaseCount(), numOfPooledBatches, "the number of parallel batches processed should have been %d but was %d instead", concurrencySync.ReleaseCount(), numOfPooledBatches)
				defer wg.Done()
			}()

			ok := utils.WaitFor(&wg, 10*time.Second)

			if test.ShouldTimeOut {
				assert.Equal(t, test.ShouldTimeOut, !ok, "Test did not time out when it should have")
			} else {
				assert.True(t, ok, "Test timed out when it should not have")
			}
		})
	}
}

// arithmeticSum calculates the sum of the first n natural numbers (i.e. 1 + 2 + 3 + ... + n)
func arithmeticSum(n int) int {
	return n * (n + 1) / 2
}

func Test_RoundRobinPoolingStrategy_Execute(t *testing.T) {
	for _, test := range []partitionedPoolingStrategyTestCase{
		// Testing the boundaries of the pool size
		{
			name:           "check all partitions can be filled with items",
			ItemsToProcess: 5,
			poolSize:       5,
			BufferSize:     0,
			ShouldTimeOut:  false,
		},
		{
			ItemsToProcess: 1,
			poolSize:       1,
			BufferSize:     0,
			ShouldTimeOut:  false,
		},
		{
			ItemsToProcess: 5,
			poolSize:       1,
			BufferSize:     0,
			ShouldTimeOut:  false,
		},
		{
			ItemsToProcess: 1000,
			poolSize:       5,
			BufferSize:     0,
			ShouldTimeOut:  false,
		},
		{
			// This is a sanity check to verify CheckpointSync is working as intended
			name:           "verify that CheckpointSync will block if the number of items to process is not divisible by the pool size",
			ItemsToProcess: 1,
			poolSize:       5,
			BufferSize:     0,
			ShouldTimeOut:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewContext(context.Background(), "test")

			sut := NewRoundRobinPoolingStrategy[int, int](test.poolSize)

			wg := sync.WaitGroup{}
			wg.Add(2)

			// Producer
			upstream := streams.NewStream[int]()
			go func() {
				for i := 0; i < test.ItemsToProcess; i++ {
					upstream.Write(i)
				}
				defer upstream.Close()
				defer wg.Done()
			}()

			// Processor
			concurrencySync := testutils.NewCheckpointSync(int(test.poolSize))

			downstream := streams.NewStream[int]()
			operation := func(ctx Context, streamReader streams.Reader[int], streamWriter streams.Writer[int]) {
				for item := range streamReader.Read() {
					concurrencySync.Checkpoint()
					streamWriter.Send(item)
				}
			}
			go func() {
				sut.Execute(ctx, operation, upstream, downstream)
				defer downstream.Close()
			}()

			// Consumer
			go func() {
				collector := 0
				counter := 0
				for item := range downstream.Read() {
					collector = collector + (item.Value() + 1)
					counter++
				}

				assert.Equal(t, arithmeticSum(test.ItemsToProcess), collector, "the sequence from 0 to %d was not complete", test.ItemsToProcess)
				assert.Equal(t, test.ItemsToProcess, counter, "there are items missing from the sequence")
				// intentionally rounded down to the nearest int, as we're not interested in partially filled batches
				numOfPooledBatches := test.ItemsToProcess / int(test.poolSize)
				assert.Equal(t, concurrencySync.ReleaseCount(), numOfPooledBatches, "the number of parallel batches processed should have been %d but was %d instead", concurrencySync.ReleaseCount(), numOfPooledBatches)
				defer wg.Done()
			}()

			ok := utils.WaitFor(&wg, 10*time.Second)

			if test.ShouldTimeOut {
				assert.Equal(t, test.ShouldTimeOut, !ok, "Test did not time out when it should have")
			} else {
				assert.True(t, ok, "Test timed out when it should not have")
			}
		})
	}
}
