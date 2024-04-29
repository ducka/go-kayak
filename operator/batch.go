package operator

import (
	"math"
	"time"

	"github.com/ducka/go-kayak/observe"
)

func Batch[T any](batchSize int, opts ...observe.Option) observe.OperatorFunc[T, []T] {
	return batch[T](batchSize, nil, opts...)
}

func BatchWithTimeout[T any](batchSize int, flushTimeout time.Duration, opts ...observe.Option) observe.OperatorFunc[T, []T] {
	return batch[T](batchSize, &flushTimeout, opts...)
}

func batch[T any](batchSize int, flushTimeout *time.Duration, opts ...observe.Option) observe.OperatorFunc[T, []T] {
	autoFlush := true
	maxFlushTimeout := time.Duration(math.MaxInt64)

	if flushTimeout == nil {
		flushTimeout = &maxFlushTimeout
		autoFlush = false
	}

	return func(source *observe.Observable[T]) *observe.Observable[[]T] {
		return observe.Operation[T, []T](
			source,
			func(upstream observe.StreamReader[T], downstream observe.StreamWriter[[]T]) {
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

						if item.Kind() == observe.ErrorKind {
							downstream.Error(item.Err(), []T{item.Value()})
							continue
						}

						batch = append(batch, item.Value())
					}

					if len(batch) == batchSize || flush {
						downstream.Write(batch)
						batch = make([]T, 0, batchSize)
					}
				}
			},
			opts...,
		)
	}
}
