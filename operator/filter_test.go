package operator

import (
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	t.Run("When emitting a sequence of 10 elements", func(t *testing.T) {
		sequence := generateIntSequence(10)
		ob := observe.Sequence(sequence)

		t.Run("When filtering the sequence to emit only even numbers", func(t *testing.T) {
			of := Filter[int](func(item observe.Notification[int]) bool {
				return item.Ok() && item.Value()%2 == 0
			})

			t.Run("Then the sequence should contain only even numbers", func(t *testing.T) {
				actual := of(ob).ToResult()
				expected := convertToNotifications([]int{0, 2, 4, 6, 8})
				assert.Len(t, actual, 5)
				assert.Equal(t, expected, actual)
			})
		})
	})
}
