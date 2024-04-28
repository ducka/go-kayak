package kayak

import (
	"testing"
)

func TestGoPool(t *testing.T) {

	t.Run("When creating a pool of 5 go routines", func(t *testing.T) {
		pool := newPool(5)

		pool.run(func() {

		})

	})

	pool := newPool(10)
	pool.run(func() {})
}
