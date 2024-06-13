package operator

import (
	"errors"
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/stream"
	"github.com/stretchr/testify/assert"
)

func TestSort(t *testing.T) {
	t.Run("When emitting an unordered sequence including errors", func(t *testing.T) {
		ob := observe.Producer(func(downstream stream.Writer[int]) {
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
					[]stream.Notification[int]{
						stream.Next(1),
						stream.Next(2),
						stream.Next(4),
						stream.Next(5),
						stream.Error[int](errors.New("error1")),
						stream.Error[int](errors.New("error2")),
					},
					actual,
				)
			})
		})
	})
}
