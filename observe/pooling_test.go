package observe

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ducka/go-kayak/streams"
	"github.com/ducka/go-kayak/testutils"
	"github.com/ducka/go-kayak/utils"
)

type partitionedPoolingStrategyTestCase struct {
	ItemsToProcess int
	PoolSize       uint16
	BufferSize     uint64
	HashFunc       HashFunc
	ShouldTimeOut  bool
}

func Test_PartitionedPoolingStrategy_Execute(t *testing.T) {

	var passThroughHashFunc HashFunc = func(key string) uint16 {
		i, _ := strconv.Atoi(key)
		return uint16(i)
	}

	for _, test := range []partitionedPoolingStrategyTestCase{
		{
			ItemsToProcess: 20,
			PoolSize:       5,
			BufferSize:     0,
			HashFunc:       passThroughHashFunc,
			ShouldTimeOut:  false,
		},
	} {

		ctx := NewContext(context.Background(), "test")
		keySelector := func(i int) string { return strconv.Itoa(i) }

		settings := ParitionedPoolSettings{
			PoolSize:   utils.ToPtr(test.PoolSize),
			BufferSize: utils.ToPtr(test.BufferSize),
			HashFunc:   utils.ToPtr(test.HashFunc),
		}

		sut := NewPartitionedPoolingStrategy[int, int](keySelector, settings)

		chk := testutils.NewConcurrencySync(5)

		wg := sync.WaitGroup{}
		wg.Add(2)
		upstream := streams.NewStream[int]()
		downstream := streams.NewStream[int]()

		operation := func(ctx Context, streamReader streams.Reader[int], streamWriter streams.Writer[int]) {
			for item := range streamReader.Read() {
				chk.Checkpoint()
				streamWriter.Send(item)
			}
			defer streamWriter.Close()
		}

		start := time.Now()

		go sut.Execute(ctx, operation, upstream, downstream)

		go func() {
			for i := 0; i < 100; i++ {
				upstream.Write(i)
			}
			fmt.Printf("Upstream closed\n")
			defer upstream.Close()
			defer wg.Done()
		}()

		go func() {
			for i := range downstream.Read() {
				noop(i.Value())
				fmt.Printf("Received: %d\n", i.Value())
			}
			fmt.Printf("Downstream complete\n")
			defer wg.Done()
		}()

		wg.Wait()

		defer fmt.Printf("Elapsed: %v\n", time.Since(start))

		// TODO: Test cases
		// Buffer size boundaries
		// Pool size boundaries
		// Timings test

	}

}

func Test_RoundRobinPoolingStrategy_Execute(t *testing.T) {
	ctx := NewContext(context.Background(), "test")

	sut := NewRoundRobinPoolingStrategy[int, int](20)

	wg := sync.WaitGroup{}
	wg.Add(2)
	upstream := streams.NewStream[int]()
	downstream := streams.NewStream[int]()

	operation := func(ctx Context, streamReader streams.Reader[int], streamWriter streams.Writer[int]) {
		for item := range streamReader.Read() {
			fmt.Printf("Processing: %d\n", item.Value())
			time.Sleep(1000 * time.Millisecond)
			streamWriter.Send(item)
		}
		defer streamWriter.Close()
	}

	start := time.Now()

	go sut.Execute(ctx, operation, upstream, downstream)

	go func() {
		for i := 0; i < 100; i++ {
			upstream.Write(i)
		}
		fmt.Printf("Upstream closed\n")
		defer upstream.Close()
		defer wg.Done()
	}()

	go func() {
		for i := range downstream.Read() {
			noop(i.Value())
			fmt.Printf("Received: %d\n", i.Value())
		}
		fmt.Printf("Downstream complete\n")
		defer wg.Done()
	}()

	wg.Wait()

	defer fmt.Printf("Elapsed: %v\n", time.Since(start))
}

func noop(i int) {

}
