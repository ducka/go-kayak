package observe

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testForkResults struct {
	actual1 []Notification[int]
	actual2 []Notification[int]
}

func TestFork(t *testing.T) {
	t.Run("When forking an observed sequence of integers", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		wg.Add(2)
		sequence := generateIntSequence(10)
		expected := convertToNotifications(sequence)

		ob := Sequence[int](sequence)

		forks := Fork(ob, 2)

		results := &testForkResults{}

		t.Run("And observing the emitted items from the forked observables", func(t *testing.T) {
			go func(results *testForkResults) {
				defer wg.Done()
				results.actual1 = forks[0].ToResult()
			}(results)

			go func(results *testForkResults) {
				defer wg.Done()
				results.actual2 = forks[1].ToResult()
			}(results)

			wg.Wait()

			t.Run("Then the emitted items from the forked observables should match the original sequence", func(t *testing.T) {
				assert.Equal(t, expected, results.actual1)
				assert.Equal(t, expected, results.actual2)
			})
		})
	})
}
