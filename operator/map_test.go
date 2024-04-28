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
				actual := toResultValues(m)

				assert.EqualValues(t, []string{"1", "2", "3"}, actual)
			})
		})
	})

	t.Run("When observing a sequence of integers", func(t *testing.T) {
		sequence := []int{1, 2, 3}

		ob := observe.Sequence(sequence)

		t.Run("And an error occurs midway", func(t *testing.T) {
			m := Map(func(item int, index int) (string, error) {
				if item == 2 {
					return "2", errors.New("error")
				}
				return strconv.Itoa(item), nil
			})(ob)

			t.Run("Then the emitted integers should now be strings", func(t *testing.T) {
				actual := m.ToResult()

				assert.EqualValues(t, []string{"1", "2", "3"}, actual)
			})
		})
	})
}
