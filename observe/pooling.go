package observe

import (
	"runtime"
	"sync"
	"time"

	"github.com/ducka/go-kayak/instrumentation"
	"github.com/ducka/go-kayak/streams"
	"github.com/ducka/go-kayak/utils"
)

const (
	slotCtxKey = "slot"
)

type PoolingStrategy[TIn, TOut any] interface {
	Execute(ctx Context, operation OperationFunc[TIn, TOut], upstream streams.Reader[TIn], downstream streams.Writer[TOut])
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
	for slot := 0; slot < s.poolSize; slot++ {
		slotStream := streams.NewStream[TIn]()
		pool <- slotStream
		poolStreamsToClose[slot] = slotStream

		opWg.Add(1)
		go func(slot int, slotStream streams.Reader[TIn], downstream streams.Writer[TOut], ctx Context) {
			defer opWg.Done()
			now := time.Now()
			ctx = NewContextWithValue(ctx, slotCtxKey, slot)
			operation(ctx, slotStream, downstream)
			instrumentation.Metrics().Timing(ctx.Activity, "operation_duration", time.Since(now))
		}(slot, slotStream, downstream, ctx)
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

type PartitionKeySelector[T any] func(item T) string
type HashFunc func(string) uint16

func DefaultHashFunc(key string) uint16 {
	return utils.Crc16(key)
}

type PartitionedPoolingStrategy[TIn, TOut any] struct {
	keySelector PartitionKeySelector[TIn]
	hashFunc    HashFunc
	poolSize    uint16
	bufferSize  int
}

type ParitionedPoolSettings struct {
	PoolSize   *int
	BufferSize *int
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
			if *settings[0].PoolSize <= 0 {
				panic("Pool size must be greater than 0")
			}

			strategy.poolSize = uint16(*settings[0].PoolSize)
		}
		if settings[0].BufferSize != nil {
			if *settings[0].BufferSize < 0-1 {
				panic("BufferSize size must be greater than or equal to 0")
			}

			strategy.bufferSize = *settings[0].BufferSize
		}
	}

	return strategy
}

func (p *PartitionedPoolingStrategy[TIn, TOut]) Execute(ctx Context, operation OperationFunc[TIn, TOut], upstream streams.Reader[TIn], downstream streams.Writer[TOut]) {
	opWg := &sync.WaitGroup{}
	pool := make([]*streams.Stream[TIn], int(p.poolSize))

	// Initialise the pool of operations to currently process the upstream
	for slot := 0; slot < int(p.poolSize); slot++ {
		slotStream := streams.NewStream[TIn](uint64(p.bufferSize))
		pool[slot] = slotStream
		opWg.Add(1)

		go func(slot int, slotStream streams.Reader[TIn], downstream streams.Writer[TOut]) {
			defer opWg.Done()
			now := time.Now()
			ctx = NewContextWithValue(ctx, slotCtxKey, slot)
			operation(ctx, slotStream, downstream)
			instrumentation.Metrics().Timing(ctx.Activity, "operation_duration", time.Since(now))
		}(slot, slotStream, downstream)
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
