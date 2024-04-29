package operator

import (
	"errors"
	"strconv"
	"testing"

	"github.com/ducka/go-kayak/observe"
	"github.com/stretchr/testify/assert"
)

func TestMap(t *testing.T) {
	t.Run("When observing a sequence of integers", func(t *testing.T) {
		sequence := []int{1, 2, 3}

		ob := observe.Sequence(sequence)

		t.Run("And the integers are mapped to strings", func(t *testing.T) {
			m := Map(func(item int, index int) (string, error) {
				return strconv.Itoa(item), nil
			})(ob)

			t.Run("Then the emitted integers should now be strings", func(t *testing.T) {
				assert.EqualValues(t,
					[]observe.Notification[string]{
						observe.Next("1"),
						observe.Next("2"),
						observe.Next("3"),
					},
					m.ToResult(),
				)
			})
		})
	})

	t.Run("When observing a sequence of integers", func(t *testing.T) {
		sequence := []int{1, 2, 3}

		ob := observe.Sequence(sequence)

		t.Run("And an error occurs midway", func(t *testing.T) {
			err := errors.New("error")
			m := Map(func(item int, index int) (string, error) {
				if item == 2 {
					return "2", err
				}
				return strconv.Itoa(item), nil
			})(ob)

			t.Run("Then the emitted notifications should be a mixture of strings and errors", func(t *testing.T) {
				assert.EqualValues(t,
					[]observe.Notification[string]{
						observe.Next("1"),
						observe.Error(err, "2"),
						observe.Next("3"),
					},
					m.ToResult(),
				)
			})
		})
	})
}