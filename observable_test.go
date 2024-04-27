package kayak

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

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

func TestObserveProducer(t *testing.T) {

	//t.Run("When subscribing to an observable that produces 10 integers", func(t *testing.T) {
	//	sequence := []int{1, 2, 3, 4, 5}
	//	subscriberMock := SubscriberMock[int]{}
	//
	//	sut := ObserveProducer[int](func(stream StreamWriter[int]) {
	//		for _, v := range sequence {
	//			stream.Write(v)
	//		}
	//		stream.Complete()
	//	})
	//
	//	t.Run("And subscribing to the observable", func(t *testing.T) {
	//		sut.Observe()
	//		sut.Subscribe(
	//			func(v int) {
	//
	//			}
	//		)
	//
	//		fmt.Println("test")
	//	})
	//
	//	t.Run("Then the observer should receive all 10 integers", func(t *testing.T) {
	//		fmt.Println("test")
	//
	//	})
	//
	//})

	t.Run("Observe producer test", func(t *testing.T) {
		ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
			for i := 0; i < 10; i++ {
				subscriber.Write(i)
			}
			subscriber.Complete()
		}, WithActivityName("producer observable"))

		ob.Subscribe(
			func(v int) {
				fmt.Println(v)
			},
			WithOnError(func(err error) {
				fmt.Println(err)
			}),
			WithOnComplete(func() {
				fmt.Println("complete")
			}))
	})

	t.Run("Observe operation test", func(t *testing.T) {
		ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
			for i := 0; i < 10; i++ {
				subscriber.Write(i)
			}
			subscriber.Complete()
		}, WithActivityName("producer observable"))

		op := ObserveOperation[int, int](
			ob,
			func(upstream StreamReader[int], downstream StreamWriter[int]) {
				for item := range upstream.Read() {
					downstream.Send(item)
					//downstream.Send() <- item
				}
			},
			WithActivityName("operation observable"))

		op.Subscribe(
			func(v int) {
				fmt.Println(v)
			})
	})

	t.Run("All test", func(t *testing.T) {
		ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
			for i := 0; i < 10; i++ {
				subscriber.Write(i)
				time.Sleep(100 * time.Millisecond)
			}
			subscriber.Complete()
		},
			WithActivityName("producer observable"),

			// TODO: There seems to be a panic occurring when executing both of these strategies together.
			// it'll have something to do with that select statement with the drop strategy in Connect.
			WithErrorStrategy(ContinueOnErrorStrategy),
			WithBackpressureStrategy(DropBackpressureStrategy),
		)

		pipe := Pipe2(ob,
			Map[int, string](
				func(v int, index uint) (string, error) {
					if index == 2 {
						return "", fmt.Errorf("error")
					}
					return fmt.Sprintf("Digit: %d", v), nil
				},
			),
			Map[string, string](
				func(v string, index uint) (string, error) {
					time.Sleep(200 * time.Millisecond)
					return fmt.Sprintf("String: %v", v), nil
				},
			),
		)

		pipe.Subscribe(
			func(v string) {
				fmt.Println(v)
			},
			WithOnError(func(err error) {
				fmt.Println(err)
			}),
			WithOnComplete(func() {
				fmt.Println("complete")
			}),
		)
	})
}
