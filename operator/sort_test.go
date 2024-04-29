package operator

import (
	"errors"
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/stretchr/testify/assert"
)

func TestSort(t *testing.T) {
	t.Run("When emitting an unordered sequence including errors", func(t *testing.T) {
		ob := observe.Producer(func(downstream observe.StreamWriter[int]) {
			downstream.Write(4)
			downstream.Write(2)
			downstream.Error(errors.New("error with value"), 3)
			downstream.Write(5)
			downstream.Error(errors.New("error without value"))
			downstream.Write(1)
		})

		t.Run("When sorting the sequence in ascending order with a ContinueOnError strategy", func(t *testing.T) {
			os := Sort[int](func(a, b int) bool {
				return a < b
			}, observe.WithErrorStrategy(observe.ContinueOnError))(ob)

			actual := os.ToResult()

			t.Run("Then the sequence should be sorted with errors with no values at the end", func(t *testing.T) {
				assert.Equal(t,
					[]observe.Notification[int]{
						observe.Next(1),
						observe.Next(2),
						observe.Error(errors.New("error with value"), 3),
						observe.Next(4),
						observe.Next(5),
						observe.Error[int](errors.New("error without value")),
					},
					actual,
				)
			})
		})
	})
}
