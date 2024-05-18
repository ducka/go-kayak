package operator

import (
	"testing"
	"time"

	"github.com/ducka/go-kayak/observe"
	"github.com/stretchr/testify/assert"
)

func TestThrottle(t *testing.T) {
	t.Run("When emitting a sequence of 5 Items", func(t *testing.T) {
		sequence := observe.GenerateIntSequence(0, 5)
		ob := observe.Sequence(sequence)

		t.Run("When throttling the sequence to emit 1 Items every 100ms", func(t *testing.T) {
			ot := Throttle[int](1, time.Millisecond*100)(ob)

			t.Run("Then the sequence should be emitted in 500ms", func(t *testing.T) {
				then := time.Now()
				actual := ot.ToResult()
				assert.Len(t, actual, len(sequence))
				assert.WithinDuration(t, then.Add(500*time.Millisecond), time.Now(), time.Millisecond*20)
			})
		})
	})

}
