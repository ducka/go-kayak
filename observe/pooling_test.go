package observe

import (
	"context"
	"fmt"
	"regexp"
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
	name                         string
	strategies                   []PoolingStrategy[int, int]
	sequenceProducer             func() []int
	assertItem                   func(item int, slot int, strategy PoolingStrategy[int, int])
	assertOutput                 func(producedSequence []int, receivedSequence []int, strategy PoolingStrategy[int, int])
	numOfPartitionsToSynchronise int
}

func Test_PartitionedPoolingStrategy_Execute(t *testing.T) {

	passThroughHashFunc := utils.ToPtr[HashFunc](func(key string) uint16 {
		i, _ := strconv.Atoi(key)
		return uint16(i)
	})

	passThroughKeySelector := func(i int) string { return strconv.Itoa(i) }

	calculateHashSlot := func(item int, poolSize uint16) int {
		key := passThroughKeySelector(item)
		hash := (*passThroughHashFunc)(key)
		return int(hash % poolSize)
	}

	for _, test := range []partitionedPoolingStrategyTestCase{
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
			assertOutput: func(producedSequence, receivedSequence []int, strategy PoolingStrategy[int, int]) {
				assert.ElementsMatch(t, producedSequence, receivedSequence)
			},
		},
		{
			name: "Test that a completely filled pool fills all partitions",
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
			sequenceProducer: func() []int { return []int{1, 2, 3, 4, 5, 6, 7, 8} },
			assertOutput: func(producedSequence, receivedSequence []int, strategy PoolingStrategy[int, int]) {
				assert.ElementsMatch(t, producedSequence, receivedSequence)
			},
			numOfPartitionsToSynchronise: 4,
		},
		{
			name: "Assert the slot the item is processed in is correct",
			strategies: []PoolingStrategy[int, int]{
				NewPartitionedPoolingStrategy[int, int](
					passThroughKeySelector,
					ParitionedPoolSettings{
						PoolSize:   utils.ToPtr(5),
						BufferSize: utils.ToPtr(0),
						HashFunc:   passThroughHashFunc,
					},
				),
			},
			sequenceProducer: func() []int { return []int{1, 2, 3, 4, 5, 6} },
			assertItem: func(item int, slot int, strategy PoolingStrategy[int, int]) {
				switch s := strategy.(type) {
				case *PartitionedPoolingStrategy[int, int]:
					expectedSlot := calculateHashSlot(item, s.poolSize)
					assert.Equal(t, expectedSlot, slot, "Item should be processed in correct partition")
				}
			},
			assertOutput: func(producedSequence, receivedSequence []int, strategy PoolingStrategy[int, int]) {
				assert.ElementsMatch(t, producedSequence, receivedSequence)
			},
		},
		{
			name: "Assert a large number of items are processed, ensuring slot allocation and output is correct. Order is not important.",
			strategies: []PoolingStrategy[int, int]{
				NewRoundRobinPoolingStrategy[int, int](4),
				NewPartitionedPoolingStrategy[int, int](
					passThroughKeySelector,
					ParitionedPoolSettings{
						PoolSize:   utils.ToPtr(4),
						BufferSize: utils.ToPtr(1000),
						HashFunc:   passThroughHashFunc,
					},
				),
			},
			sequenceProducer: func() []int {
				return GenerateIntSequence(1, 100000)
			},
			assertItem: func(item int, slot int, strategy PoolingStrategy[int, int]) {
				switch s := strategy.(type) {
				case *RoundRobinPoolingStrategy[int, int]:
					// For round robin, slot order is not important
				case *PartitionedPoolingStrategy[int, int]:
					expectedSlot := calculateHashSlot(item, s.poolSize)
					assert.Equal(t, expectedSlot, slot, "Item should be processed in correct partition")
				}
			},
			assertOutput: func(producedSequence, receivedSequence []int, strategy PoolingStrategy[int, int]) {
				assert.ElementsMatch(t, producedSequence, receivedSequence)
			},
		},
	} {
		for _, sut := range test.strategies {
			sutName := regexp.
				MustCompile(`[^\.]+\.([^[]+)`).
				FindStringSubmatch(fmt.Sprintf("%T", sut))[1]

			t.Run(fmt.Sprintf("%s %s", sutName, test.name), func(t *testing.T) {

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
				synchroniser := testutils.NewProcessSynchroniser(int(test.numOfPartitionsToSynchronise))

				downstream := streams.NewStream[int]()
				assertItem := func(ctx Context, streamReader streams.Reader[int], streamWriter streams.Writer[int]) {
					slot := ctx.Value(slotCtxKey).(int)
					for item := range streamReader.Read() {
						if test.assertItem != nil {
							test.assertItem(item.Value(), slot, sut)
						}
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
				}()

				ok := utils.WaitFor(&wg, 10*time.Second)

				assert.True(t, ok, "Test timed out when it should not have")
			})
		}

	}
}
