package rxgo

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestObservable(t *testing.T) {
	t.Run("Cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		ob := NewObservable[int](func(subscriber Subscriber[int]) {
			for i := 0; i < 10; i++ {
				subscriber.Send() <- Next(i)
				if i == 2 {
					cancel()
					continue
				}
			}
			subscriber.Send() <- Complete[int]()
		}, WithContext(ctx))

		ob.SubscribeSync(
			func(v int) {
				fmt.Println(v)
			},
			func(err error) {

			},
			func() {

			})
	})
}

func Test_ThisIsHowASubscriberWorks(t *testing.T) {
	t.Run("observer.SubscribeOn: the observer can create a subscriber", func(t *testing.T) {
		ob := Of2(1, 2, 3, 4)

		// You can pass a "finaliser" function to SubscribeOn so you know when the subscription has been registered.
		subscription := ob.SubscribeOn()

		for {
			select {
			case <-subscription.Closed():
				fmt.Println("Stopped!")
				return
			case item, ok := <-subscription.ForEach():
				if !ok {
					return
				}
				if item.Value() == 3 {
					subscription.Stop()
				}

				fmt.Println(item.Value())
			}
		}
	})

	// nb: Unsure of the usecase here. Suspect it's not needed.
	t.Run("observer.SubscribeWith: Or you can create your own subscriber and specify a buffer size", func(t *testing.T) {
		ob := Of2(1, 2, 3, 4)

		subscriber := NewSubscriber[int](2)

		// When you register your subscriber, you need to do it within a go routine.
		go func() {
			defer subscriber.Unsubscribe()
			ob.SubscribeWith(subscriber)
		}()

		for {
			select {
			case <-subscriber.Closed():
				fmt.Println("Stopped!")
				return
			case item, ok := <-subscriber.ForEach():
				if !ok {
					return
				}
				if item.Value() == 3 {
					subscriber.Stop()
				}

				fmt.Println(item.Value())
			}
		}
	})

	t.Run("observer.Subscribe: Mainly used as a synchronous sink for the source", func(t *testing.T) {
		ob := Of2(1, 2, 3, 4)

		ob.SubscribeSync(
			func(v int) {
				fmt.Println(v)
			},
			func(err error) {
				fmt.Println(err)
			},
			func() {
				fmt.Println("Completed")
			},
		)
	})

}

func TestSubjects(t *testing.T) {
	// Subjects act as both Observer and Observable.

	//NB: BehaviorSubject is a Subject that emits the most recent item it has observed.
	t.Run("BehaviorSubject", func(t *testing.T) {
		s := NewBehaviorSubject[uint](52)
		// Emits 52, 118, and 111
		s.Subscribe(func(u uint) {
			fmt.Println(u)
		}, nil, nil)

		s.Next(188)
		s.Next(111)
		// Only emits 111
		s.Subscribe(func(u uint) {
			fmt.Println(u)
		}, nil, nil)
	})

	//NB: NOT FULLY IMPLEMENTED: Replay subscriber is suppose to emit all the items it has observed, regardless of when the subscription occured.
	// There also appears to be a reference to a scheduler in the code, which is interesting. A scheduler is inteded for multi-threading purposes.
	t.Run("ReplaySubscriber", func(t *testing.T) {
		s := NewReplaySubject[uint]()

		s.Next(188)
		// Emits 52, 118, and 111
		//s.Subscribe(func(u uint) {
		//	fmt.Println(u)
		//}, nil, nil)

		s.Next(123)
		s.Next(111)
		// Only emits 111
		s.Subscribe(func(u uint) {
			fmt.Println(u)
		}, nil, nil)
	})
}

func TestAll(t *testing.T) {
	ob := Of2(1, 2, 3, 4)

	obstr := Map[int, string](func(num int, i uint) (string, error) {
		return strconv.Itoa(num) + " numbers", nil
	})(ob)

	obstr.SubscribeSync(
		func(v string) {
			fmt.Println(v)
		},
		func(err error) {
			fmt.Println(err)
		},
		func() {
			fmt.Println("Completed")
		},
	)
}

// Map transforms the items emitted by an Observable by applying a function to each item.
func Map[T any, R any](mapper func(T, uint) (R, error)) OperatorFunc[T, R] {
	if mapper == nil {
		panic(`rxgo: "Map" expected mapper func`)
	}
	return func(source Observable[T]) Observable[R] {
		var (
			index uint
		)
		return createOperatorFunc(
			source,
			func(obs Observer[R], v T) {
				output, err := mapper(v, index)
				index++
				if err != nil {
					obs.Error(err)
					return
				}
				obs.Next(output)
			},
			func(obs Observer[R], err error) {
				obs.Error(err)
			},
			func(obs Observer[R]) {
				obs.Complete()
			},
		)
	}
}

// NB: This is the main engine that propagates items from one operator to another. This is where
// you will want to implement parallelism, logging, backpressure, etc.
func createOperatorFunc[T any, R any](
	source Observable[T],
	onNext func(Observer[R], T),
	onError func(Observer[R], error),
	onComplete func(Observer[R]),
) Observable[R] {
	return NewObservable(func(subscriber Subscriber[R]) {
		var (
			wg   = new(sync.WaitGroup)
			stop bool
		)

		wg.Add(1)

		var (
			upStream = source.SubscribeOn(wg.Done)
		)

		obs := &consumerObserver[R]{
			onNext: func(v R) {
				Next(v).Send(subscriber)
			},
			onError: func(err error) {
				upStream.Stop()
				stop = true
				Error[R](err).Send(subscriber)
			},
			onComplete: func() {
				// Inform the up stream to stop emit value
				upStream.Stop()
				stop = true
				Complete[R]().Send(subscriber)
			},
		}

		for !stop {
			select {
			// If only the stream terminated, break it
			case <-subscriber.Closed():
				stop = true
				upStream.Stop()
				return

			case item, ok := <-upStream.ForEach():
				if !ok {
					// If only the data stream closed, break it
					stop = true
					return
				}

				if err := item.Err(); err != nil {
					onError(obs, err)
					return
				}

				if item.Done() {
					onComplete(obs)
					return
				}

				onNext(obs, item.Value())
			}
		}

		wg.Wait()
	})
}
