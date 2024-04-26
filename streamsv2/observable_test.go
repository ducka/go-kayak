package streamsv2

import (
	"fmt"
	"testing"
	"time"
)

func TestObservable(t *testing.T) {
	//t.Run("Observe producer test", func(t *testing.T) {
	//	ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
	//		for i := 0; i < 10; i++ {
	//			subscriber.Write(i)
	//		}
	//		subscriber.Complete()
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
	//		subscriber.Complete()
	//	}, WithActivityName("producer observable"))
	//
	//	op := ObserveOperation[int, int](
	//		ob,
	//		func(upstream StreamReader[int], downstream StreamWriter[int]) {
	//			for item := range upstream.Read() {
	//				downstream.Send(item)
	//			}
	//		},
	//		WithActivityName("operation observable"))
	//
	//	op.Subscribe(
	//		func(v int) {
	//			fmt.Println(v)
	//		})
	//})

	t.Run("All test", func(t *testing.T) {
		ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
			for i := 0; i < 10; i++ {
				subscriber.Write(i)
				time.Sleep(100 * time.Millisecond)
			}
			subscriber.Complete()
		},
			WithActivityName("producer observable"),
			WithErrorStrategy(StopOnErrorStrategy),
			WithBackpressureStrategy(BlockBackpressureStrategy),
		)

		obStr1 := Map[int, string](
			func(v int, index uint) (string, error) {
				//if index == 2 {
				//	return "", fmt.Errorf("error")
				//}
				return fmt.Sprintf("Digit: %d", v), nil
			},
		)(ob)

		obStr := Map[string, string](
			func(v string, index uint) (string, error) {
				time.Sleep(200 * time.Millisecond)
				return fmt.Sprintf("String: %v", v), nil
			},
		)(obStr1)

		obStr.Subscribe(
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
