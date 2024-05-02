package go_kayak

import (
	"fmt"
	"testing"
	"time"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/operator"
)

func TestDemo(t *testing.T) {

	/* TODOs
	1) Implement Merge and Fork
	*/

	ob := operator.Pipe5(
		observe.Range(0, 100),
		operator.Filter[int](func(item int) bool {
			// only emit even numbers
			return item%2 == 0
		}),
		operator.Sort[int](func(left, right int) bool {
			// sort descending
			return left > right
		}),
		operator.Map[int, string](func(item int, index int) (string, error) {
			// transform ints to strings
			return fmt.Sprintf("item %d", item), nil
		}),
		operator.Throttle[string](1, 100*time.Millisecond),
		// should emit 2 batches concurrently
		operator.Batch[string](5, observe.WithPool(2)),
	)

	//forks := observe.Fork(ob, 2)

	ob.Subscribe(func(item []string) {
		fmt.Println(item)
	})
}
