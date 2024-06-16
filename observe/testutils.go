package observe

import (
	"testing"
	"time"

	"github.com/ducka/go-kayak/stream"
)

func GenerateIntSequence(start, sequenceSize int) []int {
	sequence := make([]int, 0, sequenceSize)
	for i := start; i < sequenceSize+start; i++ {
		sequence = append(sequence, i)
	}
	return sequence
}

func ConvertToNotifications[T any](sequence ...T) []stream.Notification[T] {
	notifications := make([]stream.Notification[T], len(sequence))
	for i, item := range sequence {
		notifications[i] = stream.Next(item)
	}
	return notifications
}

func ConvertToValues[T any](notifications ...stream.Notification[T]) []T {
	values := make([]T, 0, len(notifications))
	for _, n := range notifications {
		values = append(values, n.Value())
	}
	return values
}

func assertWithinTime(t *testing.T, duration time.Duration, f func()) {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(duration):
		t.Errorf("Test did not excecute within the specified timeout of %v", duration)
	}
}
