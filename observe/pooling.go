package observe

import (
	"runtime"
	"sync"
	"time"

	"github.com/ducka/go-kayak/streams"
	"github.com/ducka/go-kayak/utils"
)

type PoolingStrategy[TIn, TOut any] interface {
	Execute(ctx Context, operation OperationFunc[TIn, TOut], upstream streams.Reader[TIn], downstream streams.Writer[TOut])
}

type PartitionKeySelector[T any] func(item T) string
type HashFunc func(string) uint16

func DefaultHashFunc(key string) uint16 {
	return utils.Crc16(key)
}

type PartitionedPoolingStrategy[TIn, TOut any] struct {
	keySelector PartitionKeySelector[TIn]
	hashFunc    HashFunc
	poolSize    uint16
	bufferSize  uint64
}

type ParitionedPoolSettings struct {
	PoolSize   *uint16
	BufferSize *uint64
	HashFunc   *HashFunc
}

func NewPartitionedPoolingStrategy[TIn, TOut any](keySelector PartitionKeySelector[TIn], settings ...ParitionedPoolSettings) *PartitionedPoolingStrategy[TIn, TOut] {
	strategy := &PartitionedPoolingStrategy[TIn, TOut]{
		keySelector: keySelector,
		hashFunc:    DefaultHashFunc,
		poolSize:    uint16(runtime.NumCPU()),
		bufferSize:  0,
	}

	if len(settings) > 0 {
		if settings[0].HashFunc != nil {
			strategy.hashFunc = *settings[0].HashFunc
		}
		if settings[0].PoolSize != nil {
			strategy.poolSize = *settings[0].PoolSize
		}
		if settings[0].BufferSize != nil {
			strategy.bufferSize = *settings[0].BufferSize
		}
	}

	return strategy
}

func (p *PartitionedPoolingStrategy[TIn, TOut]) Execute(ctx Context, operation OperationFunc[TIn, TOut], upstream streams.Reader[TIn], downstream streams.Writer[TOut]) {
	opWg := &sync.WaitGroup{}
	pool := make([]*streams.Stream[TIn], p.poolSize)

	// Initialise the pool of operations to currently process the upstream
	for i := uint16(0); i < p.poolSize; i++ {
		poolStream := streams.NewStream[TIn](p.bufferSize)
		pool[i] = poolStream
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
		default:
			key := p.keySelector(item.Value())
			hashTag := p.hashFunc(key)
			slot := hashTag % p.poolSize
			poolStream := pool[slot]
			poolStream.Send(item)
		}
	}

	// Once drained, close the poolStream streams
	for _, poolStream := range pool {
		poolStream.Close()
	}

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
