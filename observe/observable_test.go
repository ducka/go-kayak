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

	t.Run("When observing a sequence that emits an error midway", func(t *testing.T) {
		err := errors.New("error")

		sut := Producer[int](func(stream StreamWriter[int]) {
			stream.Write(1)
			stream.Error(err)
			stream.Write(3)
		}, WithErrorStrategy(ContinueOnError))

		t.Run("And an operator processes the sequence with a ContinueOnError strategy", func(t *testing.T) {
			op := Operation[int, int](sut, func(ctx Context, s StreamReader[int], s2 StreamWriter[int]) {
				for item := range s.Read() {
					s2.Send(item)
				}
			})

			t.Run("Then the observable should emit the full sequence including the error", func(t *testing.T) {
				actual := op.ToResult()
				assert.Equal(t,
					[]Notification[int]{
						Next(1),
						Error[int](err),
						Next(3),
					},
					actual,
				)

			})
		})
	})

	t.Run("When defining an observable with a getContext that cancels half way through observation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		sequenceLength := 20

		sut := Producer[int](
			func(streamWriter StreamWriter[int]) {
				for i := 0; i < sequenceLength; i++ {
					// Cancel the getContext of the observable half way through the producer processing the sequence
					if sequenceLength/2 == i {
						cancel()
					}

					streamWriter.Write(i)
				}
			},
			WithContext(ctx),
		)

		results := sut.ToResult()

		t.Run("Then the last emitted element should be a getContext cancellation error", func(t *testing.T) {
			assert.Equal(t, Error[int](context.Canceled), results[len(results)-1])
		})

		t.Run("And the emitted sequence should be shorter than the upstream sequence", func(t *testing.T) {
			assert.Less(t, len(results), sequenceLength)
		})
	})

	t.Run("When merging multiple observers of time", func(t *testing.T) {
		assertWithinTime(t, 200*time.Millisecond, func() {
			ctx, cancel := context.WithCancel(context.Background())
			ob1, _ := Timer(100 * time.Millisecond)
			ob2, _ := Timer(100*time.Millisecond, WithContext(ctx))

			merged := Merge(ob1, ob2)

			t.Run("And cancelling the observation half way through, the observation should terminate", func(t *testing.T) {
				merged.Subscribe(func(item time.Time) {
					cancel()
				},
					WithOnComplete(
						func(reason CompleteReason, err error) {
							assert.Equal(t, reason, Failed)
							assert.Equal(t, err, context.Canceled)
						},
					))
			})

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

		ob := Sequence(GenerateIntSequence(0, 15))

		activeProducers := 0

		op := Operation[int, int](
			ob,
			func(ctx Context, s StreamReader[int], s2 StreamWriter[int]) {
				// The number of times this operator function executes should equal the pool size. Each operator function has its own
				// stream which is written to in a roundrobin style concurrently. If this active producers count doesn't match the pool
				// size, this should indicate a bug in the pool implementation.
				activeProducers++

				for item := range s.Read() {
					time.Sleep(100 * time.Millisecond)
					s2.Send(item)
				}
			},
			WithPool(poolSize),
		)

		results := op.ToResult()

		t.Run("Then the pool should be fully utilized", func(t *testing.T) {
			assert.Equal(t, poolSize, activeProducers)
		})

		t.Run("Then the pipeline should execute in the expected time", func(t *testing.T) {
			shouldTake := 300 * time.Millisecond
			assert.WithinDuration(t, now.Add(shouldTake), time.Now(), 20*time.Millisecond)
			assert.Len(t, results, 15)
		})
	})

	t.Run("When configuring an observable with a buffer", func(t *testing.T) {
		actual := uint64(0)
		bufferSize := uint64(10)

		ob := Producer[int](
			func(streamWriter StreamWriter[int]) {
				for i := 0; i < 15; i++ {
					// increment the counter to observe the buffer filling up
					actual++

					streamWriter.Write(i)
				}

			},
			WithBuffer(bufferSize),
		)

		// Execute observe, but don't read the sequence. This will trigger the buffer
		// filling up, but it won't drain the observable
		ob.Observe()

		// Sleep to give the go routines internal to the Observer time to execute
		time.Sleep(100 * time.Millisecond)

		t.Run("Then the observables buffer should fill up when the observable is observed", func(t *testing.T) {
			// We add 2 here to account for channels internal to the Observer that are being primed with items
			expected := bufferSize + 2

			assert.Equal(t, expected, actual)
		})
	})

	t.Run("When observing a stream", func(t *testing.T) {
		sw, obs := Stream[int]()
		sequence := GenerateIntSequence(0, 20)

		t.Run("When sending a sequence of integers on the stream", func(t *testing.T) {
			go func() {
				defer sw.Close()

				for _, item := range sequence {
					sw.Write(item)
				}
			}()

			t.Run("Then the sequence of integers should be emitted", func(t *testing.T) {
				actual := obs.ToResult()
				expected := ConvertToNotifications(sequence...)
				assert.Equal(t, expected, actual)
			})
		})
	})

	t.Run("When observing a timer of 100ms intervals", func(t *testing.T) {
		obs, stop := Timer(100 * time.Millisecond)

		t.Run("And we stop the timer after 500ms", func(t *testing.T) {
			time.AfterFunc(500*time.Millisecond, func() {
				stop()
			})

			t.Run("Then the timer should emit 5 items", func(t *testing.T) {
				assertWithinTime(t, 600*time.Millisecond, func() {
					actual := obs.ToResult()
					assert.Len(t, actual, 5)
				})
			})
		})
	})

	t.Run("When observing a cron schedule that fires every 1 second", func(t *testing.T) {
		obs, stop := Cron("* * * * * *")

		t.Run("And we stop the timer after 4 seconds", func(t *testing.T) {
			time.AfterFunc(4*time.Second, func() {
				stop()
			})

			t.Run("Then the timer should emit at least 3 items", func(t *testing.T) {
				assertWithinTime(t, 5*time.Second, func() {
					actual := obs.ToResult()
					assert.GreaterOrEqual(t, len(actual), 3)
				})
			})

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
			actualValues = append(actualValues, n.Error())
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
