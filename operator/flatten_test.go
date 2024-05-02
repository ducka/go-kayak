package operator

import (
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/stretchr/testify/assert"
)

func TestFlatten(t *testing.T) {
	t.Run("When observing an array of slices", func(t *testing.T) {
		ob := observe.Sequence[[]int]([][]int{
			{1, 2, 3},
			{4, 5, 6},
			{7, 8, 9},
		})

		t.Run("When flattening the array of slices", func(t *testing.T) {
			of := Flatten[int]()(ob)

			t.Run("When subscribing to the flattened stream", func(t *testing.T) {
				actual := observe.ConvertToValues[int](of.ToResult()...)
				assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, actual)
			})
		})

	})
}
