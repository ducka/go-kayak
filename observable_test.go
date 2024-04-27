package kayak

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestObservable(t *testing.T) {

	makeSubscriber := func(sequence ...any) *SubscriberMock[int] {
		subscriber := &SubscriberMock[int]{}
		calls := make([]*mock.Call, 0, len(sequence))

		for _, v := range sequence {
			if err, ok := v.(error); ok {
				calls = append(calls, subscriber.On("OnError", err).Return().NotBefore(calls...).Once())
				continue
			}

			calls = append(calls, subscriber.On("OnNext", v.(int)).Return().NotBefore(calls...).Once())
		}

		calls = append(calls, subscriber.On("OnComplete").Return().NotBefore(calls...).Once())

		return subscriber
	}
	makeProducer := func(sequence ...any) func(stream StreamWriter[int]) {
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

	t.Run("When observing a sequence of elements", func(t *testing.T) {
		sequence := []any{1, 2, 3}

		sut := ObserveProducer[int](makeProducer(sequence...), WithErrorStrategy(StopOnErrorStrategy))

		t.Run("Then the subscriber functions should be invoked in the expected order", func(t *testing.T) {
			subscriberMock := makeSubscriber(sequence...)

			sut.Subscribe(
				subscriberMock.OnNext,
				WithOnError(subscriberMock.OnError),
				WithOnComplete(subscriberMock.OnComplete),
			)

			subscriberMock.AssertExpectations(t)
		})
	})

	t.Run("When an observable uses a StopOnErrorStrategy", func(t *testing.T) {
		expected := []any{1, errors.New("error"), 2}
		sut := ObserveProducer[int](makeProducer(expected...), WithErrorStrategy(StopOnErrorStrategy))

		t.Run("Then the emitted expected should terminate when an error is encountered", func(t *testing.T) {
			assertSequence(t, expected[:2], sut.ToArray())
		})
	})

	t.Run("When an observable uses a ContinueOnErrorStrategy", func(t *testing.T) {
		expected := []any{1, errors.New("error"), 2}
		sut := ObserveProducer[int](makeProducer(expected...), WithErrorStrategy(ContinueOnErrorStrategy))

		t.Run("Then the emitted sequence should complete regardless of encountered errors", func(t *testing.T) {
			assertSequence(t, expected, sut.ToArray())
		})
	})

	for _, test := range []struct {
		strategy BackpressureStrategy
	}{
		{DropBackpressureStrategy},
		{BlockBackpressureStrategy},
	} {
		t.Run("When an observable uses a "+string(test.strategy)+" strategy", func(t *testing.T) {
			//wg := &sync.WaitGroup{}
			sut := ObserveProducer[int](func(streamWriter StreamWriter[int]) {
				for i := 0; i < 1000; i++ {
					streamWriter.Write(i)
				}
			}, WithBackpressureStrategy(test.strategy))

			t.Run("and backpressure is experienced in the pipeline", func(t *testing.T) {
				sut = ObserveOperation(
					sut,
					func(upstream StreamReader[int], downstream StreamWriter[int]) {
						for item := range upstream.Read() {
							downstream.Send(item)
						}
					})

				if test.strategy == BlockBackpressureStrategy {
					t.Run("Then all elements will be emitted in the sequence", func(t *testing.T) {
						assertSequence(t, []any{1, 2, 3}, sut.ToArray())
					})
				} else if test.strategy == DropBackpressureStrategy {
					t.Run("Then the elements will be dropped from the emitted sequence", func(t *testing.T) {
						items := sut.ToArray()
						assertSequence(t, []any{1, 3}, items)
					})
				}
			})
		})
	}

	//t.Run("When observing a sequence of integers and errors", func(t *testing.T) {
	//	sequence := []int{1, 2, 3}
	//	err := fmt.Errorf("error")
	//
	//	subscriberMock := SubscriberMock[int]{}
	//	subscriberMock.On("OnNext", mock.Anything).Return()
	//	subscriberMock.On("OnComplete").Return()
	//	subscriberMock.On("OnError", mock.Anything).Return()
	//
	//	sut := ObserveProducer[int](func(stream StreamWriter[int]) {
	//		for i, v := range sequence {
	//			if i == 1 {
	//				stream.Error(err)
	//				continue
	//			}
	//
	//			stream.Write(v)
	//		}
	//	})
	//
	//	sut.Subscribe(
	//		subscriberMock.OnNext,
	//		WithOnError(subscriberMock.OnError),
	//		WithOnComplete(subscriberMock.OnComplete),
	//	)
	//
	//	t.Run("Then the subscriber callbacks should be called in the correct sequence", func(t *testing.T) {
	//		subscriberMock.AssertCalled(t, "OnNext", 1)
	//		subscriberMock.AssertCalled(t, "OnError", err)
	//		subscriberMock.AssertNotCalled(t, "OnNext", 3)
	//		subscriberMock.AssertCalled(t, "OnComplete")
	//	})
	//})

	//t.Run("Observe producer test", func(t *testing.T) {
	//	ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
	//		for i := 0; i < 10; i++ {
	//			subscriber.Write(i)
	//		}
	//	}, WithActivityName("producer observable"))
	//
	//	ob.Subscribe(
	//		func(v int) {
	//			fmt.Println(v)
	//		},
	//		WithOnError(func(err error) {
	//			fmt.Println(err)
	//		}),
	//		WithOnComplete(func() {
	//			fmt.Println("complete")
	//		}))
	//})
	//
	//t.Run("Observe operation test", func(t *testing.T) {
	//	ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
	//		for i := 0; i < 10; i++ {
	//			subscriber.Write(i)
	//		}
	//	}, WithActivityName("producer observable"))
	//
	//	op := ObserveOperation[int, int](
	//		ob,
	//		func(upstream StreamReader[int], downstream StreamWriter[int]) {
	//			for item := range upstream.Read() {
	//				downstream.Send(item)
	//				//downstream.Send() <- item
	//			}
	//		},
	//		WithActivityName("operation observable"))
	//
	//	op.Subscribe(
	//		func(v int) {
	//			fmt.Println(v)
	//		})
	//})
	//
	//t.Run("All test", func(t *testing.T) {
	//	ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
	//		for i := 0; i < 10; i++ {
	//			subscriber.Write(i)
	//			time.Sleep(100 * time.Millisecond)
	//		}
	//	},
	//		WithActivityName("producer observable"),
	//
	//		// TODO: There seems to be a panic occurring when executing both of these strategies together.
	//		// it'll have something to do with that select statement with the drop strategy in Connect.
	//		WithErrorStrategy(ContinueOnErrorStrategy),
	//		WithBackpressureStrategy(DropBackpressureStrategy),
	//	)
	//
	//	pipe := Pipe2(ob,
	//		Map[int, string](
	//			func(v int, index uint) (string, error) {
	//				if index == 2 {
	//					return "", fmt.Errorf("error")
	//				}
	//				return fmt.Sprintf("Digit: %d", v), nil
	//			},
	//		),
	//		Map[string, string](
	//			func(v string, index uint) (string, error) {
	//				time.Sleep(200 * time.Millisecond)
	//				return fmt.Sprintf("String: %v", v), nil
	//			},
	//		),
	//	)
	//
	//	pipe.Subscribe(
	//		func(v string) {
	//			fmt.Println(v)
	//		},
	//		WithOnError(func(err error) {
	//			fmt.Println(err)
	//		}),
	//		WithOnComplete(func() {
	//			fmt.Println("complete")
	//		}),
	//	)
	//})
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

func (s *SubscriberMock[T]) OnComplete() {
	s.Called()
}
