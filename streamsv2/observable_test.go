package streamsv2

import (
	"fmt"
	"testing"
	"time"
)

func TestObservable(t *testing.T) {
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
