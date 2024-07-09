package go_kayak

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/operator"
	"github.com/segmentio/kafka-go"
)

func TestKafka(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		// to produce messages
		topic := "test-topic"
		partition := 0

		conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
		if err != nil {
			log.Fatal("failed to dial leader:", err)
		}

		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte("one!")},
			kafka.Message{Value: []byte("two!")},
			kafka.Message{Value: []byte("three!")},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		if err := conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()

	go func() {
		// to consume messages
		topic := "test-topic"
		partition := 0

		conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
		if err != nil {
			log.Fatal("failed to dial leader:", err)
		}

		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

		b := make([]byte, 10e3) // 10KB max per message
		for {
			n, err := batch.Read(b)
			if err != nil {
				break
			}
			fmt.Println(string(b[:n]))
		}

		if err := batch.Close(); err != nil {
			log.Fatal("failed to close batch:", err)
		}

		if err := conn.Close(); err != nil {
			log.Fatal("failed to close connection:", err)
		}

		wg.Done()
	}()

	wg.Wait()
}

func TestDemo(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(1)

	ob := observe.Pipe7(
		observe.Range(0, 100, observe.WithActivityName("Observing kafka")),
		operator.Filter[int](func(item int) bool {
			// only emit even numbers
			return item%2 == 0
		}),
		operator.Sort[int](func(left, right int) bool {
			// sort descending
			return left > right
		}),
		operator.Map[int, string](func(item int, index int) (string, error) {
			// transform ints to strings
			return fmt.Sprintf("item %d", item), nil
		}),
		operator.Throttle[string](1, 100*time.Millisecond),
		// should emit 2 batches concurrently
		operator.Batch[string](5, observe.WithPool(2)),
		operator.Print[[]string]("batches:"),
		operator.Flatten[string](),
	)

	forked := observe.Fork(ob, 2)

	pipe1 := observe.Pipe1(
		forked[0],
		operator.Print[string]("fork1:"),
	)

	pipe2 := observe.Pipe1(
		forked[1],
		operator.Print[string]("fork2:"),
	)

	merged := observe.Merge(pipe1, pipe2)

	merged.Subscribe(func(item string) {
		fmt.Println(item)
	},
		observe.WithOnComplete(func(reason observe.CompleteReason, err error) {
			wg.Done()
		}))

	wg.Wait()
}

//func TestStageDemo(t *testing.T) {
//
//	in1 := observe.Array[testInputItem](
//		[]testInputItem{
//			testInputItem{Id: 1, Value: "In1"},
//			testInputItem{Id: 2, Value: "In1"},
//			testInputItem{Id: 3, Value: "In1"},
//		},
//	)
//
//	in2 := observe.Array[testInputItem](
//		[]testInputItem{
//			testInputItem{Id: 1, Value: "In2"},
//			testInputItem{Id: 3, Value: "In2"},
//		},
//	)
//
//	in3 := observe.Array[testInputItem](
//		[]testInputItem{
//			testInputItem{Id: 1, Value: "In3"},
//		},
//	)
//
//	sut := observe.Pipe1(
//		operator.Stage10(
//			in1, in2, in3, observe.Empty[testInputItem](), observe.Empty[testInputItem](), observe.Empty[testInputItem](), observe.Empty[testInputItem](), observe.Empty[testInputItem](), observe.Empty[testInputItem](), observe.Empty[testInputItem](),
//			func(in1, in2, in3, in4, in5, in6, in7, in8, in9, in10 *testInputItem, out testOuputItem) (*testOuputItem, error) {
//				if in1 != nil {
//					out.Value1 = in1.Value
//					out.Id = in1.Id
//				}
//				if in2 != nil {
//					out.Value2 = in2.Value
//					out.Id = in2.Id
//				}
//				if in3 != nil {
//					out.Value3 = in3.Value
//					out.Id = in3.Id
//				}
//				return &out, nil
//			},
//		),
//		operator.Filter(func(item testOuputItem) bool {
//			return true
//			//return !(item.Value1 == "" || item.Value2 == "" || item.Value3 == "")
//		}),
//	)
//
//	out := sut.ToResult()
//
//	for _, item := range out {
//		fmt.Println(item.Value())
//	}
//}

type testInputItem struct {
	Id    int
	Value string
}

func (i testInputItem) GetKey() []string {
	return []string{strconv.Itoa(i.Id)}
}

type testOuputItem struct {
	Id     int
	Value1 string
	Value2 string
	Value3 string
}
