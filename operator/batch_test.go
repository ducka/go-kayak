package operator

import (
	"math"
	"testing"
	"time"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/streams"
	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	t.Run("When batching up a sequence of 95 integers into 10 batches", func(t *testing.T) {
		batchSize := 10
		numberOfItems := 95
		sequence := observe.GenerateIntSequence(0, numberOfItems)

		ob := observe.Sequence(sequence)

		b := Batch[int](batchSize)(ob)

		actual := b.ToResult()

		t.Run("Then the sequence emitted should be 10 batches of integers", func(t *testing.T) {
			assert.Len(t, actual, int(math.Ceil(float64(numberOfItems)/float64(batchSize))))
		})

		t.Run("Then each batch except for the last batch should have 10 integers", func(t *testing.T) {
			for i := 0; i < len(actual)-1; i++ {
				actualBatch := actual[i]
				expectedBatch := sequence[i*batchSize : (i+1)*batchSize]
				assert.Equal(t, len(actualBatch.Value()), batchSize)
				assert.Equal(t, expectedBatch, actualBatch.Value())
			}
		})

		t.Run("Then the last batch should have 5 Items", func(t *testing.T) {
			lastItem := actual[len(actual)-1]
			assert.Equal(t, len(lastItem.Value()), numberOfItems%batchSize)
		})
	})

	t.Run("When emitting a sequence that makes up an incomplete batch", func(t *testing.T) {
		batchSize := 10
		numberOfItems := 5
		sequence := observe.GenerateIntSequence(0, numberOfItems)

		ob := observe.Sequence(sequence)

		b := Batch[int](batchSize)(ob)

		actual := b.ToResult()

		t.Run("Then the sequence should still be emitted as a batch", func(t *testing.T) {
			assert.Len(t, actual, 1)
			assert.Len(t, actual[0].Value(), numberOfItems)
			assert.Equal(t, sequence, actual[0].Value())
		})
	})

	t.Run("When producing a sequence of 15 elements that emits 5 elements every 100ms", func(t *testing.T) {
		batchSize := 10
		numberOfItems := 15
		timeout := time.Millisecond * 50
		sequence := observe.GenerateIntSequence(0, numberOfItems)

		ob := observe.Producer(func(streamWriter streams.Writer[int]) {
			for i, item := range sequence {
				streamWriter.Write(item)

				if i == 4 || i == 9 {
					time.Sleep(timeout * 2)
				}
			}
		})

		t.Run("When batching the elements with a timeout of 50ms", func(t *testing.T) {
			b := BatchWithTimeout[int](batchSize, timeout)(ob)

			actual := b.ToResult()

			t.Run("Then we should receive 3 incomplete batches of 5 elements", func(t *testing.T) {
				assert.Len(t, actual, 3)
				assert.Len(t, actual[0].Value(), 5)
				assert.Len(t, actual[1].Value(), 5)
				assert.Len(t, actual[2].Value(), 5)
			})
		})
	})
}
