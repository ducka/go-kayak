package observe

import (
	"sort"
	"testing"

	"github.com/ducka/go-kayak/stream"
	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	t.Run("When merging two integer sequences", func(t *testing.T) {
		sequence1 := generateRange(200, 100)
		sequence2 := generateRange(0, 100)

		ob1 := Sequence(sequence1)
		ob2 := Sequence(sequence2)

		merged := Merge(ob1, ob2)
		actual := toValues[int](merged.ToResult()...)

		t.Run("Then the merged sequence should contain all elements", func(t *testing.T) {
			expected := append(sequence1, sequence2...)

			sort.Sort(sort.IntSlice(actual))
			sort.Sort(sort.IntSlice(expected))

			assert.Len(t, actual, len(expected))
			assert.Equal(t, expected, actual)
		})
	})
}

func toValues[T any](notifications ...streams.Notification[T]) []T {
	values := make([]T, 0, len(notifications))
	for _, n := range notifications {
		values = append(values, n.Value())
	}
	return values
}

func generateRange(start, sequenceSize int) []int {
	sequence := make([]int, 0, sequenceSize)
	for i := start; i < sequenceSize+start; i++ {
		sequence = append(sequence, i)
	}
	return sequence
}
