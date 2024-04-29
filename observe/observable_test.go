package observe

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestObservable(t *testing.T) {
	t.Run("When observing a sequence of {1, 2, 3}", func(t *testing.T) {
		sequence := []any{1, 2, 3}

		sut := Producer[int](produceSequence(sequence...), WithErrorStrategy(StopOnError))

		t.Run("Then the subscriber functions should be invoked as OnNext(1), OnNext(2), OnNext(3), OnComplete(finished)", func(t *testing.T) {
			subscriberMock := makeSubscriber(StopOnError, sequence...)

			sut.Subscribe(
				subscriberMock.OnNext,
				WithOnError(subscriberMock.OnError),
				WithOnComplete(subscriberMock.OnComplete),
			)

			subscriberMock.AssertExpectations(t)
		})
	})

	t.Run("When observing a sequence of {1, error, 3 } and we're using the StopOnError strategy", func(t *testing.T) {
		err := errors.New("error")
		sequence := []any{1, err, 3}

		sut := Producer[int](produceSequence(sequence...), WithErrorStrategy(StopOnError))

		t.Run("Then the subscriber functions should be invoked as OnNext(1), OnError, OnComplete(error)", func(t *testing.T) {
			subscriberMock := makeSubscriber(StopOnError, sequence...)

			sut.Subscribe(
				subscriberMock.OnNext,
				WithOnError(subscriberMock.OnError),
				WithOnComplete(subscriberMock.OnComplete),
			)

			subscriberMock.AssertExpectations(t)
		})
	})

	t.Run("When defining an observable with a context that cancels half way through observation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		sequenceLength := 20

		sut := Producer[int](
			func(streamWriter StreamWriter[int]) {
				for i := 0; i < sequenceLength; i++ {
					// Cancel the context of the observable half way through the producer processing the sequence
					if sequenceLength/2 == i {
						cancel()
					}

					streamWriter.Write(i)
				}
			},
			WithContext(ctx),
		)

		results := sut.ToResult()

		t.Run("Then the last emitted element should be a context cancellation error", func(t *testing.T) {
			assert.Equal(t, Error[int](context.Canceled), results[len(results)-1])
		})

		t.Run("And the emitted sequence should be shorter than the upstream sequence", func(t *testing.T) {
			assert.Less(t, len(results), sequenceLength)
		})
	})

	t.Run("When an observable uses a StopOnError", func(t *testing.T) {
		expected := []any{1, errors.New("error"), 2}
		sut := Producer[int](produceSequence(expected...), WithErrorStrategy(StopOnError))

		t.Run("Then the emitted expected should terminate when an error is encountered", func(t *testing.T) {
			assertSequence(t, expected[:2], sut.ToResult())
		})
	})

	t.Run("When an observable uses a ContinueOnError", func(t *testing.T) {
		expected := []any{1, errors.New("error"), 2}
		sut := Producer[int](produceSequence(expected...), WithErrorStrategy(ContinueOnError))

		t.Run("Then the emitted sequence should complete regardless of encountered errors", func(t *testing.T) {
			assertSequence(t, expected, sut.ToResult())
		})
	})

	t.Run("When an observable uses a Drop backpressure strategy", func(t *testing.T) {
		sequenceLength := 1000
		wg := &sync.WaitGroup{}
		wg.Add(1)

		sut := Producer[int](func(streamWriter StreamWriter[int]) {
			for i := 0; i < sequenceLength; i++ {
				streamWriter.Write(i)
			}
			wg.Done()
		}, WithBackpressureStrategy(Drop))

		t.Run("and backpressure is experienced in the pipeline", func(t *testing.T) {
			items := make([]int, 0, sequenceLength)

			sut.Subscribe(func(item int) {
				wg.Wait()
				items = append(items, item)
			})

			t.Run("Then most of the items should be dropped", func(t *testing.T) {
				assert.Less(t, len(items), sequenceLength)
			})
		})
	})

	t.Run("When an observable uses a Block backpressure strategy", func(t *testing.T) {
		sequenceLength := 100
		sut := Producer[int](func(streamWriter StreamWriter[int]) {
			for i := 0; i < sequenceLength; i++ {
				streamWriter.Write(i)
			}
		}, WithBackpressureStrategy(Block))

		t.Run("and backpressure is experienced in the pipeline", func(t *testing.T) {
			items := make([]int, 0, sequenceLength)

			sut.Subscribe(func(item int) {
				items = append(items, item)
				time.Sleep(1 * time.Millisecond)
			})

			t.Run("Then all items should be emitted in the sequence", func(t *testing.T) {
				assert.Len(t, items, sequenceLength)
			})
		})
	})

	t.Run("When an observable uses a Block backpressure strategy", func(t *testing.T) {
		sequenceLength := 100
		sut := Producer[int](func(streamWriter StreamWriter[int]) {
			for i := 0; i < sequenceLength; i++ {
				streamWriter.Write(i)
			}
		}, WithBackpressureStrategy(Block))

		t.Run("and backpressure is experienced in the pipeline", func(t *testing.T) {
			items := make([]int, 0, sequenceLength)

			sut.Subscribe(func(item int) {
				items = append(items, item)
				time.Sleep(1 * time.Millisecond)
			})

			t.Run("Then all items should be emitted in the sequence", func(t *testing.T) {
				assert.Len(t, items, sequenceLength)
			})
		})
	})

	t.Run("When an observable operator uses a pool for concurrency and a workload is generated to fully utilise the pool", func(t *testing.T) {
		now := time.Now()
		poolSize := 5
		wg := &sync.WaitGroup{}
		wg.Add(poolSize)

		ob := Producer[int](
			produceNumbers(100),
		)

		activeProducers := 0

		op := Operation[int, int](
			ob,
			func(s StreamReader[int], s2 StreamWriter[int]) {
				// The number of times this operator function executes should equal the pool size. Each operator function has its own
				// stream which is written to in a roundrobin style concurrently. If this active producers count doesn't match the pool
				// size, this should indicate a bug in the pool implementation.
				activeProducers++

				for item := range s.Read() {
					time.Sleep(10 * time.Millisecond)
					s2.Send(item)
				}
			},
			WithPool(poolSize),
		)

		op.ToResult()

		t.Run("Then the pool should be fully utilized", func(t *testing.T) {
			assert.Equal(t, poolSize, activeProducers)
		})

		t.Run("Then the pipeline should execute in the expected time", func(t *testing.T) {
			shouldTake := (int64(100) * int64(10) * int64(time.Millisecond)) / int64(poolSize)

			assert.WithinDuration(t, now.Add(time.Duration(shouldTake)), time.Now(), 10*time.Millisecond)
		})
	})
}

func makeSubscriber(strategy ErrorStrategy, sequence ...any) *SubscriberMock[int] {
	subscriber := &SubscriberMock[int]{}
	calls := make([]*mock.Call, 0, len(sequence))
	var err error

	for _, v := range sequence {
		if err2, ok := v.(error); ok {
			err = err2
			calls = append(calls, subscriber.On("OnError", err).Return().NotBefore(calls...).Once())

			if strategy == StopOnError {
				break
			}

			continue
		}

		calls = append(calls, subscriber.On("OnNext", v.(int)).Return().NotBefore(calls...).Once())
	}

	reason := Completed
	if err != nil {
		reason = Failed
	}

	calls = append(calls, subscriber.On("OnComplete", reason, err).Return().NotBefore(calls...).Once())

	return subscriber
}

func produceSequence(sequence ...any) func(stream StreamWriter[int]) {
	return func(stream StreamWriter[int]) {
		for _, v := range sequence {
			if err, ok := v.(error); ok {
				stream.Error(err)
				continue
			}

			stream.Write(v.(int))
		}
	}
}

func produceNumbers(squenceLength int) func(stream StreamWriter[int]) {
	return func(stream StreamWriter[int]) {
		for i := 0; i < squenceLength; i++ {
			stream.Write(i)
		}
	}
}

func assertSequence[T any](t *testing.T, expected []any, actual []Notification[T]) {
	actualValues := make([]any, 0, len(actual))
	for _, n := range actual {
		if n.Kind() == ErrorKind {
			actualValues = append(actualValues, n.Err())
			continue
		}
		actualValues = append(actualValues, n.Value())
	}

	assert.EqualValues(t, expected, actualValues)
}

type SubscriberMock[T any] struct {
	mock.Mock
}

func (s *SubscriberMock[T]) OnNext(next T) {
	s.Called(next)
}

func (s *SubscriberMock[T]) OnError(err error) {
	s.Called(err)
}

func (s *SubscriberMock[T]) OnComplete(reason CompleteReason, err error) {
	s.Called(reason, err)
}
