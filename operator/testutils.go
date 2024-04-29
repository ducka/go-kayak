package operator

import (
	"github.com/ducka/go-kayak/observe"
)

func generateIntSequence(sequenceSize int) []int {
	sequence := make([]int, sequenceSize)
	for i := 0; i < sequenceSize; i++ {
		sequence[i] = i
	}
	return sequence
}

func convertToNotifications[T any](sequence []T) []observe.Notification[T] {
	notifications := make([]observe.Notification[T], len(sequence))
	for i, item := range sequence {
		notifications[i] = observe.Next(item)
	}
	return notifications
}
