package operator

import (
	"math"
	"time"

	"github.com/ducka/go-kayak/observe"
)

// Batch batches up Items from the observable into slices of the specified size.
func Batch[T any](batchSize int, opts ...observe.ObservableOption) observe.OperatorFunc[T, []T] {
	opts = defaultActivityName("Batch", opts)
	return batch[T](batchSize, nil, opts...)
}

// BatchWithTimeout batches up Items from the observable into slices of the specified size. The flushTimeout ensures that
// Items will be batched up and emitted after the specified duration has elapsed, regardless of whether the batch is complete.
func BatchWithTimeout[T any](batchSize int, flushTimeout time.Duration, opts ...observe.ObservableOption) observe.OperatorFunc[T, []T] {
	opts = defaultActivityName("BatchWithTimeout", opts)
	return batch[T](batchSize, &flushTimeout, opts...)
}

func batch[T any](batchSize int, flushTimeout *time.Duration, opts ...observe.ObservableOption) observe.OperatorFunc[T, []T] {
	autoFlush := true
	maxFlushTimeout := time.Duration(math.MaxInt64)

	if flushTimeout == nil {
		flushTimeout = &maxFlushTimeout
		autoFlush = false
	}

	return func(source *observe.Observable[T]) *observe.Observable[[]T] {
		return observe.Operation[T, []T](
			source,
			func(ctx observe.Context, upstream observe.StreamReader[T], downstream observe.StreamWriter[[]T]) {
				batch := make([]T, 0, batchSize)
				exit := false

				for !exit {
					flush := false

					select {
					case <-time.After(*flushTimeout):
						flush = autoFlush
					case item, ok := <-upstream.Read():
						if !ok {
							flush = true
							exit = true
							break
						}

						if item.IsError() {
							downstream.Error(item.Error())
							continue
						}

						batch = append(batch, item.Value())

						flush = len(batch) == batchSize
					}

					if flush && len(batch) > 0 {
						downstream.Write(batch)
						batch = make([]T, 0, batchSize)
					}
				}
			},
			opts...,
		)
	}
}
