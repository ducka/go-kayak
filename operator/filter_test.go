package operator

import (
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	t.Run("When emitting a sequence of 10 elements", func(t *testing.T) {
		sequence := observe.GenerateIntSequence(0, 10)
		ob := observe.Sequence(sequence)

		t.Run("When filtering the sequence to emit only even numbers", func(t *testing.T) {
			of := Filter[int](func(i int) bool {
				return i%2 == 0
			})(ob)

			t.Run("Then the sequence should contain only even numbers", func(t *testing.T) {
				actual := of.ToResult()
				expected := observe.ConvertToNotifications(0, 2, 4, 6, 8)
				assert.Len(t, actual, 5)
				assert.Equal(t, expected, actual)
			})
		})
	})
}
