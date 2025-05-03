package observe

import (
	"context"
	"fmt"
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

type partitionedPoolingStrategyTestCase2 struct {
	name                         string
	strategies                   []PoolingStrategy[int, int]
	sequenceProducer             func() []int
	assertItem                   func(item int, slot int, strategy PoolingStrategy[int, int])
	assertOutput                 func(producedSequence []int, receivedSequence []int, strategy PoolingStrategy[int, int])
	wg                           sync.WaitGroup
	shouldTimeOut                bool
	expectPoolSize               int
	simulateMinParallelProcesses int
}

// 1) Test that a partially filled pool will still process all items.
// 2) Test that a pools partitions can be filled up completely (requires blocking).
// 3) Assert the slot the item is processed in is correct (only for partitioned pooling).
// 4) Assert the next available pool is always used (only for round robin pooling).
// 5) Assert all items are processed. Order is not important.

func Test_PartitionedPoolingStrategy_Execute(t *testing.T) {

	passThroughHashFunc := utils.ToPtr[HashFunc](func(key string) uint16 {
		i, _ := strconv.Atoi(key)
		return uint16(i)
	})

	passThroughKeySelector := func(i int) string { return strconv.Itoa(i) }

	for _, test := range []partitionedPoolingStrategyTestCase2{
		{
			name: "Ensure all items are processed when the pool isn't fully utilised",
			strategies: []PoolingStrategy[int, int]{
				NewRoundRobinPoolingStrategy[int, int](4),
				NewPartitionedPoolingStrategy[int, int](
					passThroughKeySelector,
					ParitionedPoolSettings{
						PoolSize: utils.ToPtr(4),
						HashFunc: passThroughHashFunc,
					},
				),
			},
			sequenceProducer: func() []int { return []int{1, 2, 3} },
			assertItem: func(item int, slot int, strategy PoolingStrategy[int, int]) {
				switch s := strategy.(type) {
				case *RoundRobinPoolingStrategy[int, int]:
					// For round robin, we can assert that slots are used in order
					assert.Less(t, slot, s.poolSize, "Slot should be less than pool size")
				case *PartitionedPoolingStrategy[int, int]:
					// For partitioned, we can assert that items are hashed to the correct slot
					key := passThroughKeySelector(item)
					hash := (*passThroughHashFunc)(key)
					expectedSlot := int(hash % s.poolSize)
					assert.Equal(t, expectedSlot, slot, "Item should be processed in correct partition")
				}
			},
			assertOutput: func(producedSequence, receivedSequence []int, strategy PoolingStrategy[int, int]) {
				assert.ElementsMatch(t, producedSequence, receivedSequence)
			},
		},
		// {
		// 	name: "Test that a completely filled pool fills all partitions",
		// 	strategies: []PoolingStrategy[int, int]{
		// 		NewRoundRobinPoolingStrategy[int, int](4),
		// 		NewPartitionedPoolingStrategy[int, int](
		// 			passThroughKeySelector,
		// 			ParitionedPoolSettings{
		// 				PoolSize: utils.ToPtr(4),
		// 				HashFunc: passThroughHashFunc,
		// 			},
		// 		),
		// 	},
		// 	sequenceProducer: func() []int { return []int{1, 2, 3} },
		// 	assertItem: func(item int, slot int) {
		// 		// synchroniser.Checkpoint()
		// 	},
		// },
		// {
		// 	name: "Assert the slot the item is processed in is correct",
		// 	strategies: []PoolingStrategy[int, int]{
		// 		NewPartitionedPoolingStrategy[int, int](
		// 			passThroughKeySelector,
		// 			ParitionedPoolSettings{
		// 				PoolSize:   utils.ToPtr(5),
		// 				BufferSize: utils.ToPtr(0),
		// 				HashFunc:   passThroughHashFunc,
		// 			},
		// 		),
		// 	},
		// 	sequenceProducer: func() []int { return []int{1, 2, 3, 4, 5, 6, 7, 8} },
		// },
		// {
		// 	name: "Assert the next available pool is always used",
		// 	strategies: []PoolingStrategy[int, int]{
		// 		NewRoundRobinPoolingStrategy[int, int](4),
		// 	},
		// 	sequenceProducer: func() []int { return []int{1, 2, 3, 4, 5, 6, 7, 8} },
		// },
		// {
		// 	name: "Assert all items are processed. Order is not important",
		// 	strategies: []PoolingStrategy[int, int]{
		// 		NewRoundRobinPoolingStrategy[int, int](4),
		// 		NewPartitionedPoolingStrategy[int, int](
		// 			passThroughKeySelector,
		// 			ParitionedPoolSettings{
		// 				PoolSize:   utils.ToPtr(5),
		// 				BufferSize: utils.ToPtr(0),
		// 				HashFunc:   passThroughHashFunc,
		// 			},
		// 		),
		// 	},
		// 	sequenceProducer: func() []int {
		// 		result := make([]int, 0, 1000)

		// 		for i := 0; i < 1000; i++ {
		// 			result = append(result, i)
		// 		}

		// 		return result
		// 	},
		// },
	} {
		for _, sut := range test.strategies {
			t.Run(fmt.Sprintf("%t: %s", sut, test.name), func(t *testing.T) {
				tctx := NewContext(context.Background(), "test")

				wg := sync.WaitGroup{}
				wg.Add(2)
				producedSequence := make([]int, 0)

				// Producer
				upstream := streams.NewStream[int]()
				go func() {
					for _, item := range test.sequenceProducer() {
						upstream.Write(item)
						producedSequence = append(producedSequence, item)
					}
					defer upstream.Close()
					defer wg.Done()
				}()

				// Processor
				synchroniser := testutils.NewProcessSynchroniser(int(test.expectPoolSize))

				downstream := streams.NewStream[int]()
				assertItem := func(ctx Context, streamReader streams.Reader[int], streamWriter streams.Writer[int]) {
					slot := ctx.Value(slotCtxKey).(int)
					for item := range streamReader.Read() {
						test.assertItem(item.Value(), slot, sut)
						synchroniser.Checkpoint()
						streamWriter.Send(item)
					}
				}
				go func() {
					sut.Execute(tctx, assertItem, upstream, downstream)
					defer downstream.Close()
				}()

				// Consumer
				go func() {
					defer wg.Done()
					receivedSequence := make([]int, 0)

					for item := range downstream.Read() {
						receivedSequence = append(receivedSequence, item.Value())
					}

					test.assertOutput(producedSequence, receivedSequence, sut)

					// assert.Equal(t, arithmeticSum(test.ItemsToProcess), collector, "the sequence from 0 to %d was not complete", test.ItemsToProcess)
					// assert.Equal(t, test.ItemsToProcess, counter, "there are items missing from the sequence")
					// // intentionally rounded down to the nearest int, as we're not interested in partially filled batches
					// numOfPooledBatches := test.ItemsToProcess / int(test.poolSize)
					// assert.Equal(t, synchroniser.ReleaseCount(), numOfPooledBatches, "the number of parallel batches processed should have been %d but was %d instead", synchroniser.ReleaseCount(), numOfPooledBatches)
				}()

				ok := utils.WaitFor(&wg, 10*time.Second)

				if test.shouldTimeOut {
					assert.False(t, ok, "Test did not time out when it should have")
				} else {
					assert.True(t, ok, "Test timed out when it should not have")
				}
			})
		}

	}
}

// // arithmeticSum calculates the sum of the first n natural numbers (i.e. 1 + 2 + 3 + ... + n)
// func arithmeticSum(n int) int {
// 	return n * (n + 1) / 2
// }

// func Test_RoundRobinPoolingStrategy_Execute(t *testing.T) {
// 	for _, test := range []partitionedPoolingStrategyTestCase{
// 		// Testing the boundaries of the pool size
// 		{
// 			name:           "check all partitions can be filled with items",
// 			ItemsToProcess: 5,
// 			poolSize:       5,
// 			BufferSize:     0,
// 			ShouldTimeOut:  false,
// 		},
// 		{
// 			ItemsToProcess: 1,
// 			poolSize:       1,
// 			BufferSize:     0,
// 			ShouldTimeOut:  false,
// 		},
// 		{
// 			ItemsToProcess: 5,
// 			poolSize:       1,
// 			BufferSize:     0,
// 			ShouldTimeOut:  false,
// 		},
// 		{
// 			ItemsToProcess: 1000,
// 			poolSize:       5,
// 			BufferSize:     0,
// 			ShouldTimeOut:  false,
// 		},
// 		{
// 			// This is a sanity check to verify synchroniser is working as intended
// 			name:           "verify that synchroniser will block if the number of items to process is not divisible by the pool size",
// 			ItemsToProcess: 1,
// 			poolSize:       5,
// 			BufferSize:     0,
// 			ShouldTimeOut:  true,
// 		},
// 	} {
// 		t.Run(test.name, func(t *testing.T) {
// 			ctx := NewContext(context.Background(), "test")

// 			sut := NewRoundRobinPoolingStrategy[int, int](test.poolSize)

// 			wg := sync.WaitGroup{}
// 			wg.Add(2)

// 			// Producer
// 			upstream := streams.NewStream[int]()
// 			go func() {
// 				for i := 0; i < test.ItemsToProcess; i++ {
// 					upstream.Write(i)
// 				}
// 				defer upstream.Close()
// 				defer wg.Done()
// 			}()

// 			// Processor
// 			synchroniser := testutils.NewSynchroniser(int(test.poolSize))

// 			downstream := streams.NewStream[int]()
// 			assertItem := func(ctx Context, streamReader streams.Reader[int], streamWriter streams.Writer[int]) {
// 				for item := range streamReader.Read() {
// 					synchroniser.Checkpoint()
// 					streamWriter.Send(item)
// 				}
// 			}
// 			go func() {
// 				sut.Execute(ctx, assertItem, upstream, downstream)
// 				defer downstream.Close()
// 			}()

// 			// Consumer
// 			go func() {
// 				collector := 0
// 				counter := 0
// 				for item := range downstream.Read() {
// 					collector = collector + (item.Value() + 1)
// 					counter++
// 				}

// 				assert.Equal(t, arithmeticSum(test.ItemsToProcess), collector, "the sequence from 0 to %d was not complete", test.ItemsToProcess)
// 				assert.Equal(t, test.ItemsToProcess, counter, "there are items missing from the sequence")
// 				// intentionally rounded down to the nearest int, as we're not interested in partially filled batches
// 				numOfPooledBatches := test.ItemsToProcess / int(test.poolSize)
// 				assert.Equal(t, synchroniser.ReleaseCount(), numOfPooledBatches, "the number of parallel batches processed should have been %d but was %d instead", synchroniser.ReleaseCount(), numOfPooledBatches)
// 				defer wg.Done()
// 			}()

// 			ok := utils.WaitFor(&wg, 10*time.Second)

// 			if test.ShouldTimeOut {
// 				assert.Equal(t, test.ShouldTimeOut, !ok, "Test did not time out when it should have")
// 			} else {
// 				assert.True(t, ok, "Test timed out when it should not have")
// 			}
// 		})
// 	}
// }
