package observe

import (
	"sync"
	"time"

	"github.com/ducka/go-kayak/streams"
)

type PoolingStrategy[TIn, TOut any] interface {
	Execute(ctx Context, operation OperationFunc[TIn, TOut], upstream streams.Reader[TIn], downstream streams.Writer[TOut])
}

type PartitionKeySelector[T any] func(item T) string
type HashFunc func(string) int64

func DefaultHashFunc(string) int64 {
	return 1
}

type PartitionedPoolingStrategy[TIn, TOut any] struct {
	keySelector PartitionKeySelector[TIn]
	hashFunc    HashFunc
	poolSize    int
}

func NewPartitionedPoolingStrategy[TIn, TOut any](poolSize int, keySelector PartitionKeySelector[TIn], hashFunc HashFunc) *PartitionedPoolingStrategy[TIn, TOut] {
	return &PartitionedPoolingStrategy[TIn, TOut]{keySelector: keySelector, hashFunc: hashFunc, poolSize: poolSize}
}

func (p *PartitionedPoolingStrategy[TIn, TOut]) Execute(ctx Context, operation OperationFunc[TIn, TOut], upstream streams.Reader[TIn], downstream streams.Writer[TOut]) {
	opWg := &sync.WaitGroup{}
	poolWg := &sync.WaitGroup{}

	// Initialise the pool of operations to currently process the upstream
	pool := make(chan *streams.Stream[TIn], s.poolSize)
	poolStreamsToClose := make([]*streams.Stream[TIn], s.poolSize)
	for i := 0; i < s.poolSize; i++ {
		poolStream := streams.NewStream[TIn]()
		pool <- poolStream
		poolStreamsToClose[i] = poolStream

		opWg.Add(1)
		go func(poolStream streams.Reader[TIn], downstream streams.Writer[TOut]) {
			defer opWg.Done()
			now := time.Now()
			operation(ctx, poolStream, downstream)
			measurer.Timing(ctx.Activity, "operation_duration", time.Since(now))
		}(poolStream, downstream)
	}

	// Send items to the next available stream in the pool
	for item := range upstream.Read() {
		select {
		case <-ctx.Done():
			return
		case nextStream := <-pool:
			poolWg.Add(1)

			go func(item streams.Notification[TIn], nextStream *streams.Stream[TIn], pool chan *streams.Stream[TIn]) {
				defer poolWg.Done()

				nextStream.Send(item)
				// return the stream to the pool
				pool <- nextStream

			}(item, nextStream, pool)
		}
	}

	// Wait until the concurrently running poolStream streams have finished draining
	poolWg.Wait()

	// Once drained, close the poolStream streams
	for _, poolStream := range poolStreamsToClose {
		poolStream.Close()
	}

	// And close the pool
	close(pool)

	// Wait until the concurrently executing operations have finished writing to the downstream
	opWg.Wait()
}

type RoundRobinPoolingStrategy[TIn, TOut any] struct {
	poolSize int
}

func NewRoundRobinPoolingStrategy[TIn, TOut any](poolSize int) *RoundRobinPoolingStrategy[TIn, TOut] {
	if poolSize < 1 {
		panic("Pool size must be greater than 1")
	}

	return &RoundRobinPoolingStrategy[TIn, TOut]{poolSize: poolSize}
}

func (s *RoundRobinPoolingStrategy[TIn, TOut]) Execute(ctx Context, operation OperationFunc[TIn, TOut], upstream streams.Reader[TIn], downstream streams.Writer[TOut]) {
	opWg := &sync.WaitGroup{}
	poolWg := &sync.WaitGroup{}

	// Initialise the pool of operations to currently process the upstream
	pool := make(chan *streams.Stream[TIn], s.poolSize)
	poolStreamsToClose := make([]*streams.Stream[TIn], s.poolSize)
	for i := 0; i < s.poolSize; i++ {
		poolStream := streams.NewStream[TIn]()
		pool <- poolStream
		poolStreamsToClose[i] = poolStream

		opWg.Add(1)
		go func(poolStream streams.Reader[TIn], downstream streams.Writer[TOut]) {
			defer opWg.Done()
			now := time.Now()
			operation(ctx, poolStream, downstream)
			measurer.Timing(ctx.Activity, "operation_duration", time.Since(now))
		}(poolStream, downstream)
	}

	// Send items to the next available stream in the pool
	for item := range upstream.Read() {
		select {
		case <-ctx.Done():
			return
		case nextStream := <-pool:
			poolWg.Add(1)

			go func(item streams.Notification[TIn], nextStream *streams.Stream[TIn], pool chan *streams.Stream[TIn]) {
				defer poolWg.Done()

				nextStream.Send(item)
				// return the stream to the pool
				pool <- nextStream

			}(item, nextStream, pool)
		}
	}

	// Wait until the concurrently running poolStream streams have finished draining
	poolWg.Wait()

	// Once drained, close the poolStream streams
	for _, poolStream := range poolStreamsToClose {
		poolStream.Close()
	}

	// And close the pool
	close(pool)

	// Wait until the concurrently executing operations have finished writing to the downstream
	opWg.Wait()
}
