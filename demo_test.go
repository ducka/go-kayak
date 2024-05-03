package go_kayak

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/operator"
)

func TestDemo(t *testing.T) {

	/* TODOs
	1) Implement Merge and Fork
	*/

	wg := &sync.WaitGroup{}
	wg.Add(1)

	ob := operator.Pipe7(
		observe.Range(0, 100, observe.WithActivityName("Observing kafka")),
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
		operator.Print[[]string]("batches:"),
		operator.Flatten[string](),
	)

	forked := observe.Fork(ob, 2)

	pipe1 := operator.Pipe1(
		forked[0],
		operator.Print[string]("fork1:"),
	)

	pipe2 := operator.Pipe1(
		forked[1],
		operator.Print[string]("fork2:"),
	)

	merged := observe.Merge(pipe1, pipe2)

	merged.Subscribe(func(item string) {
		fmt.Println(item)
	},
		observe.WithOnComplete(func(reason observe.CompleteReason, err error) {
			wg.Done()
		}))

	wg.Wait()
}
