package streams_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	streams "github.com/ducka/go-kayak"
)

func TestStreams(t *testing.T) {

	//people := []Person{
	//	{Id: "person1", Name: "Joe Blogs"},
	//	{Id: "person2", Name: "Billy Bob"},
	//	{Id: "person3", Name: "Jane Doe"},
	//}
	//
	orders := []Order{
		{Id: "order1", PersonId: "person1", Description: "Groceries"},
		{Id: "order2", PersonId: "person1", Description: "Takeaway"},
		{Id: "order3", PersonId: "person3", Description: "Sporting Equipment"},
		{Id: "order4", PersonId: "person3", Description: "Back to school"},
		{Id: "order5", PersonId: "person3", Description: "Easter Presents"},
		{Id: "order6", PersonId: "person3", Description: "Date Night"},
	}

	peopleLeft := []Person{
		{Id: "person1", Name: "Joe Blogs"},
		{Id: "person2", Name: "Billy Bob"},
		{Id: "person3", Name: "Jane Doe"},
	}

	ordersRight := []Order{
		{Id: "order1", PersonId: "person1", Description: "Groceries"},
		{Id: "order2", PersonId: "person1", Description: "Takeaway"},
		{Id: "order4", PersonId: "person3", Description: "Back to school"},
		{Id: "order5", PersonId: "person3", Description: "Easter Presents"},
		{Id: "order6", PersonId: "person3", Description: "Date Night"},
		{Id: "order7", PersonId: "person4", Description: "Burgers"},
	}

	//
	//costs := []Cost{
	//	{OrderId: "order1", Amount: 100.00},
	//	{OrderId: "order2", Amount: 60.00},
	//	{OrderId: "order3", Amount: 20.00},
	//	{OrderId: "order4", Amount: 50.00},
	//	{OrderId: "order5", Amount: 2.00},
	//	{OrderId: "order6", Amount: 150.00},
	//}

	//costService := NewCostService(costs)

	t.Run("Process Demo: Log errors, drop messages", func(t *testing.T) {
		ctx := context.Background()
		now := time.Now()

		step := streams.NewArraySource(ctx, orders, true)

		step, err := streams.Process(step, func(ctx context.Context, in <-chan Order, out chan<- Order, errCh chan<- error) {
			for o := range in {

				time.Sleep(500 * time.Millisecond)

				if o.Id == "order3" {
					errCh <- errors.New("cannot process Order 3")
					continue
				}

				out <- o
			}
		})

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			err.Sink(func(err error) {
				fmt.Println(fmt.Sprintf("(%dms) Error: %v", time.Since(now).Milliseconds(), err))
			})

			wg.Done()
		}()

		go func() {
			step.Sink(func(o Order) {
				fmt.Println(fmt.Sprintf("(%dms) Processed:  %+v", time.Since(now).Milliseconds(), o))
			})

			wg.Done()
		}()

		wg.Wait()

	})

	t.Run("Process Demo: transform output", func(t *testing.T) {
		ctx := context.Background()
		now := time.Now()

		step := streams.NewArraySource(ctx, orders, true)

		step2, _ := streams.Process(step, func(ctx context.Context, in <-chan Order, out chan<- string, errCh chan<- error) {
			for o := range in {
				// Send order id instead, to demonstrate the output of process can be different from the input
				out <- o.Id
			}
		})

		step2.Sink(func(o string) {
			fmt.Println(fmt.Sprintf("(%dms) Processed:  %+v", time.Since(now).Microseconds(), o))
		})
	})

	t.Run("Fan Out, Fan In Demo: Parallel vs Synchronous processing", func(t *testing.T) {
		ctx := context.Background()

		step := streams.NewArraySource(ctx, orders, true)

		step, _ = streams.Process(step, func(ctx context.Context, in <-chan Order, out chan<- Order, errCh chan<- error) {
			timer := time.Now()
			i := 0
			for o := range in {
				time.Sleep(200 * time.Millisecond)
				out <- o
				i++
			}

			fmt.Println(fmt.Sprintf("Synchronous Processing  %d items took: (%dms)", i, time.Since(timer).Milliseconds()))
		})

		results := step.ToArray()

		fmt.Println(fmt.Sprintf("Synchronous processing finished with %d items", len(results)))

		step = streams.NewArraySource(ctx, orders, true)

		step = streams.FanOut(step, 2)

		step, _ = streams.Process(step, func(ctx context.Context, in <-chan Order, out chan<- Order, errCh chan<- error) {
			timer := time.Now()
			i := 0
			for o := range in {
				time.Sleep(200 * time.Millisecond)
				out <- o
				i++
			}

			fmt.Println(fmt.Sprintf("Concurrent Processing %d items took: (%dms)", i, time.Since(timer).Milliseconds()))
		})

		step = streams.FanIn(step)

		results = step.ToArray()
		fmt.Println(fmt.Sprintf("Concurrent processing finished with %d items", len(results)))
	})

	t.Run("Batching Demo: instantaneous and temporal batching", func(t *testing.T) {
		ctx := context.Background()
		batchNow := time.Now()
		sourceCh := make(chan Order)

		go func() {
			// Send a random number of orders down the pipeline every 5 seconds
			for demoIx := 0; demoIx < 10; demoIx++ {
				batchNow = time.Now()
				rand.Seed(time.Now().UnixNano())
				maxIx := rand.Intn(len(orders)-2) + 1
				o := orders[:maxIx]

				fmt.Println(fmt.Sprintf("Sending %d more items", len(o)))

				for _, order := range o {
					sourceCh <- order
				}
				time.Sleep(3 * time.Second)
			}
		}()

		step := streams.NewChannelSource(ctx, sourceCh)

		batch := streams.Batch(step, 3, streams.WithFlushTimeout(2*time.Second))

		batch.Sink(func(o []Order) {
			fmt.Println(fmt.Sprintf("Received Batch of %d items (took %dms): %+v", len(o), time.Since(batchNow).Milliseconds(), o))
		})
	})

	t.Run("Throttle Demo", func(t *testing.T) {
		ctx := context.Background()
		now := time.Now()

		step := streams.NewArraySource(ctx, orders, true)

		batch := streams.Throttle(step, 1, time.Second)

		batch.Sink(func(o Order) {
			fmt.Println(fmt.Sprintf("(%dms) Recieved item: %+v", time.Since(now).Milliseconds(), o))
		})
	})

	t.Run("Merge Demo", func(t *testing.T) {
		ctx := context.Background()
		now := time.Now()

		leftStream := streams.NewArraySource(ctx, orders, true)
		rightStream := streams.NewArraySource(ctx, orders, true)

		merge := streams.Merge(leftStream, rightStream, func(left, right Order) OrderPair {
			return OrderPair{
				OrderLeft:  left,
				OrderRight: right,
			}
		})

		merge.Sink(func(o OrderPair) {
			fmt.Println(fmt.Sprintf("(%dms) Received merged item: %+v", time.Since(now).Milliseconds(), o))
		})
	})

	t.Run("Sort Demo", func(t *testing.T) {
		ctx := context.Background()
		now := time.Now()

		stream := streams.NewArraySource(ctx, orders, true)

		merge := streams.Sort(stream, func(left, right Order) bool {
			return left.Description < right.Description
		})

		merge.Sink(func(o Order) {
			fmt.Println(fmt.Sprintf("(%dms) Received sorted item: %+v", time.Since(now).Milliseconds(), o))
		})
	})

	t.Run("Demo all activities", func(t *testing.T) {
		ctx := context.Background()

		//ctx, cancel := context.WithCancel(ctx)
		//
		//go func() {
		//	time.Sleep(time.Second * 8)
		//	cancel()
		//}()

		/*
			TODOS:
			1) Demos
			2) Error handling
			3) Middleware - Logging, Metrics, Tracing
		*/

		now := time.Now()

		step := streams.NewArraySource(ctx, orders, true)

		batchStep := streams.Batch(step, 3, streams.WithFlushTimeout(time.Second))

		step, _ = streams.Process(batchStep, func(ctx context.Context, in <-chan []Order, out chan<- Order, errCh chan<- error) {
			for o := range in {
				fmt.Println(fmt.Sprintf("(%dms) Flattening batch of %d items:  %+v", time.Since(now).Microseconds(), len(o), o))
				for _, i := range o {
					out <- i
				}
			}
		})

		step, _ = streams.Process(step, func(ctx context.Context, in <-chan Order, out chan<- Order, errCh chan<- error) {
			for o := range in {
				fmt.Println(fmt.Sprintf("(%dms) Received flattened batch of items:  %+v", time.Since(now).Microseconds(), o))
				out <- o
			}
		})

		step = streams.FanOut(step, 2)

		step, errs := streams.Process(step, func(ctx context.Context, in <-chan Order, out chan<- Order, errCh chan<- error) {
			for o := range in {
				time.Sleep(200 * time.Millisecond)
				fmt.Println(fmt.Sprintf("(%dms) Parallel processing of: %+v", time.Since(now).Milliseconds(), o))
				errCh <- errors.New(fmt.Sprintf("(%dms) Error: %+v", time.Since(now).Milliseconds(), o))
				out <- o
			}
		})

		go func() {
			errs.Sink(func(err error) {

				fmt.Println(err)
			})
		}()

		step = streams.FanIn(step)

		step, _ = streams.Process(step, func(ctx context.Context, in <-chan Order, out chan<- Order, errCh chan<- error) {
			for o := range in {
				fmt.Println(fmt.Sprintf("(%dms) 2nd Synchronous procesing of:  %+v", time.Since(now).Milliseconds(), o))
				out <- o
			}
		})

		fork := streams.Fork(step, 2)

		fork1, _ := streams.Process(fork[0], func(ctx context.Context, in <-chan Order, out chan<- Order, errCh chan<- error) {
			for o := range in {
				fmt.Println(fmt.Sprintf("(%dms) Fork1:  %+v", time.Since(now).Milliseconds(), o))
				out <- o
			}
		})

		fork2, _ := streams.Process(fork[1], func(ctx context.Context, in <-chan Order, out chan<- Order, errCh chan<- error) {
			for o := range in {
				fmt.Println(fmt.Sprintf("(%dms) Fork2:  %+v", time.Since(now).Milliseconds(), o))
				out <- o
			}
		})

		merge := streams.Merge(fork1, fork2, func(left, right Order) OrderPair {
			return OrderPair{
				OrderLeft:  left,
				OrderRight: right,
			}
		})

		merge = streams.Sort(merge, func(left, right OrderPair) bool {
			return left.OrderLeft.Id > right.OrderLeft.Id
		})

		merge = streams.Throttle(merge, 1, time.Second)

		merge.Sink(func(item OrderPair) {
			fmt.Println(fmt.Sprintf("(%dms) Sink:  %+v", time.Since(now).Milliseconds(), item))
		})
	})

	t.Run("Merge streams", func(t *testing.T) {
		ctx := context.Background()

		//ctx, cancel := context.WithCancel(ctx)
		//
		//go func() {
		//	time.Sleep(time.Second * 8)
		//	cancel()
		//}()

		/*
			TODOS:
			1) Demos
			2) Error handling
			3) Middleware - Logging, Metrics, Tracing
		*/

		now := time.Now()

		peopleLeftStream := streams.NewArraySource(ctx, peopleLeft, true)
		ordersRightStream := streams.NewArraySource(ctx, ordersRight, true)

		merge := streams.MergeJoin(
			peopleLeftStream,
			ordersRightStream,
			streams.LeftJoin,
			func(person Person, order Order) bool {
				return person.Id == order.PersonId
			},
			func(person *Person, order *Order) PersonOrder {
				fmt.Println(fmt.Sprintf("(%dms) Person:  %+v  Order: %+v", time.Since(now).Milliseconds(), person, order))

				personOrder := PersonOrder{}

				if person != nil {
					personOrder.PersonId = person.Id
					personOrder.PersonName = person.Name
				}

				if order != nil {
					personOrder.OrderId = order.Id
					personOrder.OrderDescription = order.Description
				}

				return personOrder
			},
		)

		merge.Sink(func(item PersonOrder) {
			//fmt.Println(fmt.Sprintf("(%dms) Sink:  %+v", time.Since(now).Milliseconds(), item))
		})
	})

}

type OrderPair struct {
	OrderLeft  Order
	OrderRight Order
}

type Person struct {
	Id   string
	Name string
}

type PersonOrder struct {
	PersonId         string
	PersonName       string
	OrderId          string
	OrderDescription string
	OrderCost        float32
}

type Order struct {
	Id          string
	PersonId    string
	Description string
}

type Cost struct {
	OrderId string
	Amount  float32
}

type CostService struct {
	costs map[string]Cost
}

func NewCostService(costs []Cost) CostService {
	costMap := make(map[string]Cost)
	for _, c := range costs {
		costMap[c.OrderId] = c
	}
	return CostService{
		costs: costMap,
	}
}

func (c CostService) GetCosts(orders []Order) []Cost {
	costs := make([]Cost, 0)
	for _, o := range orders {
		cost := c.costs[o.Id]
		costs = append(costs, cost)
	}
	return costs
}
