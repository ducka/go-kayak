package operator

import (
	"math"
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	t.Run("", func(t *testing.T) {
		batchSize := 10
		numberOfItems := 95
		sequence := generateIntSequence(numberOfItems)

		ob := observe.Sequence(sequence)

		b := Batch[int](batchSize)(ob)

		actual := b.ToResult()

		assert.Len(t, actual, int(math.Ceil(float64(numberOfItems)/float64(batchSize))))
		lastItem := actual[len(actual)-1]
		assert.Equal(t, len(lastItem.Value()), numberOfItems%batchSize)
	})
}
