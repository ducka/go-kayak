package operator

import (
	"time"

	"github.com/ducka/go-kayak/observe"
)

func Batch[T any](batchSize int) observe.OperatorFunc[T, T] {

}

type batchOptions struct {
	flushTimeout      time.Duration
	autoFlush         bool
	observableOptions []observe.Option
}

type BatchOption func(options *batchOptions)

func WithFlushTimeout(flushTimeout time.Duration) BatchOption {

}
