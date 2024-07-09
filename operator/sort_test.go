package operator

import (
	"errors"
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/streams"
	"github.com/stretchr/testify/assert"
)

func TestSort(t *testing.T) {
	t.Run("When emitting an unordered sequence including errors", func(t *testing.T) {
		ob := observe.Producer(func(downstream streams.Writer[int]) {
			downstream.Write(4)
			downstream.Write(2)
			downstream.Error(errors.New("error1"))
			downstream.Write(5)
			downstream.Error(errors.New("error2"))
			downstream.Write(1)
		}, observe.WithErrorStrategy(observe.ContinueOnError))

		t.Run("When sorting the sequence in ascending order with a ContinueOnError strategy", func(t *testing.T) {
			os := Sort[int](func(a, b int) bool {
				return a < b
			})(ob)

			actual := os.ToResult()

			t.Run("Then the sequence should be sorted with errors with no values at the end", func(t *testing.T) {
				assert.Equal(t,
					[]streams.Notification[int]{
						streams.Next(1),
						streams.Next(2),
						streams.Next(4),
						streams.Next(5),
						streams.Error[int](errors.New("error1")),
						streams.Error[int](errors.New("error2")),
					},
					actual,
				)
			})
		})
	})
}
