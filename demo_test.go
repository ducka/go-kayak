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

	sequence := operator.GenerateIntSequence(100)

	ob := observe.Pipe5(
		observe.Sequence(sequence),
		operator.Filter[int](func(item int) bool {
			return item%2 == 0
		}),
		operator.Sort[int](func(left, right int) bool {
			return left > right
		}),
		operator.Map[int, string](func(item int, index int) (string, error) {
			return fmt.Sprintf("item %d", item), nil
		}),
		operator.Throttle[string](1, 100*time.Millisecond),
		operator.Batch[string](5, observe.WithPool(2)),
	)

	ob.Subscribe(func(item []string) {
		fmt.Println(item)
	})
}
