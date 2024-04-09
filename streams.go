package streams

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"
)

/*
TODO:
1) The current version of Merge doesn't really make sense. Replace it with Merge and Merge Join
2) Merge: accepts multiple channels of different types, and joins an item from each channel into a single item. No join predicate requried. The input channels must be sorted, obviously they should also be of the same length.
3) MergeJoin: accepts multiple channels of different types, and joins an item from each channel into a single item. A join predicate is required. The input channels must be sorted. Left, Right, Inner, and Outer joins are supported.
4) LookUp: Accepts a single channel and a lookup provider. The lookup provider could support single or batch retrieval of lookup data. The lookup provider could abstract away an in memory store, redis, database, or whatever.
5) Source and Target data sources: Source datasources are reading from, and Target datasources are for writing to. This is your Source and Sink. It would be great to abstract away the implementation detail of reading and writing to OpenSearch and Redis.
6) Operations like Merge and MergeJoin require the input channels to be sorted. Streams need to be decorated with this sorted attribute so such transformations can assert whether or not the stream is sorted. Data sources and the Sort transform should set this attribute.
7) Build a Source Writer data source that Kafka handlers can use to write to a stream. Source writer should accept an array of items to write.
8) I think errors are going to need to be enriched with additional data, like the item that the error relates to.
9) Label each activity in a stream with an "activity name". This will be useful for logging, and error reporting.
10) Sort: Sorts an incoming channel by Asc or Desc. This operation will need to be done in memory.

*/

type Stream[T any] struct {
	ctx    context.Context
	out    []chan T
	sorted bool
}

func (a Stream[T]) IsSorted() bool {
	return a.sorted
}

func (s Stream[t]) IsFannedOut() bool {
	return len(s.out) > 1
}

func (s Stream[T]) Sink(callback func(item T)) {
	if s.IsFannedOut() {
		panic("Stream must be fanned in before calling Sink()")
	}

	for v := range s.out[0] {
		callback(v)
	}
}

func (s Stream[T]) ToArray() []T {
	items := make([]T, 0)
	s.Sink(func(item T) {
		items = append(items, item)
	})
	return items
}

func NewChannelSource[T any](ctx context.Context, source chan T) Stream[T] {
	return Stream[T]{
		ctx: ctx,
		out: []chan T{source},
	}
}

func NewArraySource[T any](ctx context.Context, source []T, isSorted bool) Stream[T] {

	out := make(chan T)

	go func() {
		defer close(out)

		for _, in := range source {
			out <- in
		}
	}()

	return Stream[T]{
		ctx:    ctx,
		out:    []chan T{out},
		sorted: isSorted,
	}
}

func Process[TIn, TOut any](stream Stream[TIn], processor func(context.Context, <-chan TIn, chan<- TOut, chan<- error)) (Stream[TOut], Stream[error]) {
	errQueue := NewQueue[error]()
	errLimit := 1000
	errClose := false
	errOutCh := make(chan error)
	errOutWg := sync.WaitGroup{}
	outChs := make([]chan TOut, len(stream.out))

	// Process each upstream channel in parallel, and send the results to a corresponding downstream channel.
	for i, upstreamCh := range stream.out {
		outCh := make(chan TOut)
		outChs[i] = outCh

		errOutWg.Add(1)

		go func(upstreamCh <-chan TIn, outCh chan TOut, errQueue *ConcurrentQueue[error]) {
			inCh := make(chan TIn)
			errBuffCh := make(chan error)
			defer close(errBuffCh)
			defer close(outCh)
			defer errOutWg.Done()

			// Take items from the upstream channel and send them to the input channel of the
			// processor function. If cancellation is detected, then terminate the processing.
			go func(upstreamCh <-chan TIn, inCh chan TIn) {
				for {
					select {
					case <-stream.ctx.Done():
						return
					case inItem, ok := <-upstreamCh:
						if !ok {
							close(inCh)
							return
						}

						inCh <- inItem
					}
				}
			}(upstreamCh, inCh)

			// Read outputted errors from the processor function and buffer them in a limited queue.
			// If the queue is full, then drop the oldest error.
			go func(upstreamCh <-chan TIn, errBuffCh chan error, errQueue *ConcurrentQueue[error]) {
				for {
					select {
					case <-stream.ctx.Done():
						return
					case err, ok := <-errBuffCh:
						if !ok {
							return
						}
						// Add the error to the error queue.
						errQueue.Enqueue(err)
						// If the error queue is full, then drop the oldest error.
						if errQueue.Len() >= errLimit {
							errQueue.Dequeue()
						}
					}
				}
			}(upstreamCh, errBuffCh, errQueue)

			// Call the processor function for the current upstream channel.
			processor(stream.ctx, inCh, outCh, errBuffCh)
		}(upstreamCh, outCh, errQueue)
	}

	// Close the error output channel when all upstream channels have been processed.
	go func(errOutCh chan error) {
		errOutWg.Wait()
		errClose = true
	}(errOutCh)

	// Send errors to the output error channel as they arrive in the error queue
	go func(errOutCh chan error, errQueue *ConcurrentQueue[error]) {
		defer close(errOutCh)
		for !errClose {
			select {
			case <-stream.ctx.Done():
				return
			default:
				if errQueue.Len() > 0 {
					errOutCh <- errQueue.Dequeue()
				}
			}

		}
	}(errOutCh, errQueue)

	return Stream[TOut]{
			ctx: stream.ctx,
			out: outChs,
		}, Stream[error]{
			ctx: stream.ctx,
			out: []chan error{errOutCh},
		}
}

func FanOut[T any](stream Stream[T], concurrencyLimit int) Stream[T] {
	if len(stream.out) != 1 {
		// don't fan out a stream that is already fanned out.
		return stream
	}

	if concurrencyLimit < 1 {
		concurrencyLimit = 1
	}

	// Create a channel of channels (basically a queue), each channel will be used to fan out the items.
	fanChs := make(chan chan T, concurrencyLimit)
	outChs := make([]chan T, 0, concurrencyLimit)

	for i := 0; i < concurrencyLimit; i++ {
		fanCh := make(chan T)
		fanChs <- fanCh
		outChs = append(outChs, fanCh)
	}

	// The go routine below utilises the fan channel queue to find the next available fan channel to send an item to.
	// It ensures that all fan channels are 100% utilised, and that no one fan channel can block the utilisation of
	// other fan channels. The only time processing will be blocked is if all fan channels are busy.
	go func(upCh <-chan T, fanChs chan chan T) {
		defer close(fanChs)
		wg := sync.WaitGroup{}

		for item := range upCh {
			wg.Add(1)

			select {
			case <-stream.ctx.Done():
				return

			case nextFanCh := <-fanChs:
				// Pop the next fan channel off the queue so no other goroutines can send to it

				// Spin up a go routine to send to the next fan channel
				go func(item T, nextFanCh chan T, fanChs chan chan T, wg *sync.WaitGroup) {
					// Send the item to the next fan channel.
					nextFanCh <- item
					// Pop the fan channel back onto the queue so it's available for subsequent items
					fanChs <- nextFanCh

					wg.Done()
				}(item, nextFanCh, fanChs, &wg)
			}
		}

		wg.Wait()

		for fanCh := range fanChs {
			close(fanCh)
		}
	}(stream.out[0], fanChs)

	return Stream[T]{
		ctx: stream.ctx,
		out: outChs,
	}
}

func FanIn[T any](stream Stream[T]) Stream[T] {
	if len(stream.out) <= 1 {
		return stream
	}

	chOut := make(chan T)
	chOutWg := sync.WaitGroup{}
	chOutWg.Add(len(stream.out))

	for _, upCh := range stream.out {
		go func(upCh <-chan T, chOut chan T) {
			for item := range upCh {
				select {
				case <-stream.ctx.Done():
					return
				case chOut <- item:
				}
			}
			chOutWg.Done()
		}(upCh, chOut)
	}

	go func() {
		chOutWg.Wait()
		close(chOut)
	}()

	return Stream[T]{
		ctx: stream.ctx,
		out: []chan T{chOut},
	}
}

type BatchOption func(*batchConfig)

type batchConfig struct {
	flushTimeout time.Duration
	autoFlush    bool
}

func WithFlushTimeout(timeout time.Duration) BatchOption {
	return func(config *batchConfig) {
		config.flushTimeout = timeout
		config.autoFlush = true
	}
}

func Batch[T any](stream Stream[T], batchSize int, options ...BatchOption) Stream[[]T] {
	if batchSize < 1 {
		batchSize = 1
	}

	config := &batchConfig{
		flushTimeout: time.Duration(math.MaxInt64),
		autoFlush:    false,
	}

	for _, option := range options {
		option(config)
	}

	result, _ := Process[T, []T](stream, func(ctx context.Context, in <-chan T, out chan<- []T, errCh chan<- error) {
		accumulator := make([]T, 0, batchSize)

		exit := false

		for !exit {
			flush := false

			select {
			case <-time.After(config.flushTimeout):
				flush = config.autoFlush
			case item, ok := <-in:
				if !ok {
					flush = true
					exit = true
					break
				}

				accumulator = append(accumulator, item)
				flush = len(accumulator) >= batchSize
			}

			if flush && len(accumulator) > 0 {
				out <- accumulator

				accumulator = make([]T, 0, batchSize)
			}
		}
	})

	return result
}

func Throttle[T any](stream Stream[T], flowRate int64, perDuration time.Duration) Stream[T] {
	if flowRate < 1 {
		flowRate = 1
	}

	result, _ := Process[T, T](stream, func(ctx context.Context, in <-chan T, out chan<- T, errCh chan<- error) {
		for i := range in {
			select {
			case out <- i:
				time.Sleep(time.Duration(int64(perDuration) / flowRate))
			case <-ctx.Done():
				return
			}
		}
	})

	return result
}

func Fork[T any](stream Stream[T], numberOfForks int) []Stream[T] {
	if numberOfForks < 1 {
		numberOfForks = 1
	}

	if stream.IsFannedOut() {
		panic("Stream must be fanned in before calling Fork()")
	}
	forkChs := make([]chan T, 0, numberOfForks)

	forks := make([]Stream[T], 0, numberOfForks)

	for i := 0; i < numberOfForks; i++ {
		outCh := make(chan T)
		forkChs = append(forkChs, outCh)

		forks = append(forks, Stream[T]{
			ctx: stream.ctx,
			out: []chan T{outCh},
		})
	}

	// Read the current output channel and forward its items onto the forked channels
	go func(upCh <-chan T, forkChs []chan T) {
		for _, forkCh := range forkChs {
			defer close(forkCh)
		}

		for {
			select {
			case <-stream.ctx.Done():
				return
			case item, ok := <-upCh:
				if !ok {
					return
				}

				for _, forkCh := range forkChs {
					forkCh <- item
				}
			}
		}
	}(stream.out[0], forkChs)

	return forks
}

func Merge[TInLeft, TInRight, TOut any](streamLeft Stream[TInLeft], streamRight Stream[TInRight], merger func(itemLeft TInLeft, itemRight TInRight) TOut) Stream[TOut] {
	if streamLeft.IsFannedOut() {
		panic("The left stream must be fanned in before calling Merge()")
	}

	if streamRight.IsFannedOut() {
		panic("The right stream must be fanned in before calling Merge()")
	}

	//if !streamLeft.IsSorted() {
	//	panic("The left stream must be sorted before calling Merge()")
	//}
	//
	//if !streamRight.IsSorted() {
	//	panic("The right stream must be sorted before calling Merge()")
	//}

	outCh := make(chan TOut)
	outCtx := joinContexts(streamLeft.ctx, streamRight.ctx)

	go func(outCh chan TOut) {
		defer close(outCh)

		for {
			select {
			case <-outCtx.Done():
				return
			default:
				itemLeft, okLeft := <-streamLeft.out[0]
				itemRight, okRight := <-streamRight.out[0]

				if !okLeft && !okRight {
					return
				}

				outCh <- merger(itemLeft, itemRight)
			}
		}
	}(outCh)

	return Stream[TOut]{
		ctx: outCtx,
		out: []chan TOut{outCh},
	}
}

type JoinType int

/*
type LookupProvider[TIn, TOut any] func(items TIn[]) chan TOut
type LookupProvider[TOut any] func(keys []any) TOut[]

type LookupProvider[TKeys, TResult any] interface {
	GetValues(keys [][]TKeys) (TResult[], error)
}

func Lookup[TIn, TOut any](stream Stream[TIn], processor LookupProcessor[TIn, TOut], provider LookupProvider[TOut]) (Stream[TOut], Stream[error]) {
	result, err := Process[TIn, TOut](
		stream,
		func(ctx context.Context, inCh <-chan TIn, outCh chan<- TOut, errCh chan<- error) {
			// TODO:
			// 1) How do you extract a key from the inbound items?
			// 2) How do you correlate the items with results retrieved from the cache / lookup
			// 3) You'll need a cache provider which will be used to bulk get and set values retrieved from the service.
			// 4) You'll need a lookup service for retrieval of lookup values from the source service. This values will be cached.
			// 5) The default cache will be in memory, but we should support redis as well. You should be able to disable the cache.
		})

	return result, err
}*/

const (
	InnerJoin JoinType = 0
	LeftJoin           = 1
	RightJoin          = 2
	OuterJoin          = 3
)

func MergeJoin[TInLeft, TInRight, TOut any](
	streamLeft Stream[TInLeft],
	streamRight Stream[TInRight],
	joinType JoinType,
	joiner func(itemLeft TInLeft, itemRight TInRight) bool,
	merger func(itemLeft *TInLeft, itemRight *TInRight) TOut,
) Stream[TOut] {
	if streamLeft.IsFannedOut() {
		panic("The left stream must be fanned in before calling Merge()")
	}

	if streamRight.IsFannedOut() {
		panic("The right stream must be fanned in before calling Merge()")
	}

	//if !streamLeft.IsSorted() {
	//	panic("The left stream must be sorted before calling Merge()")
	//}
	//
	//if !streamRight.IsSorted() {
	//	panic("The right stream must be sorted before calling Merge()")
	//}

	outCh := make(chan TOut)
	outCtx := joinContexts(streamLeft.ctx, streamRight.ctx)

	go func(outCh chan TOut) {
		defer close(outCh)

		var itemLeft TInLeft
		var itemRight TInRight
		lastOuterJoin := RightJoin
		okLeft, okRight := true, true
		fetchLeft, fetchRight := true, true

		for {
			select {
			case <-outCtx.Done():
				return
			default:
				if fetchLeft {
					itemLeft, okLeft = <-streamLeft.out[0]
				}
				fetchLeft = true

				if fetchRight {
					itemRight, okRight = <-streamRight.out[0]
				}
				fetchRight = true

				if !okLeft && !okRight {
					return
				}

				isMatch := joiner(itemLeft, itemRight)

				/*
					TODO:
					So I think the fetchRight / fetchLeft logic is part of the solution. The other part of the solution
					I think is to cache the items that you encounter as you advance the stream. Psydocode:

					(Left Join)
					1. Select the left item. Add to cache.
					2. Select the right item. Add to cache.
					3. Right item matches the left. Pass to out channel.
					4. Advance right item.
					5. Right item matches the left. Pass to out channel.
					6. Advance right item.
					7. Right item does not match the left.
					8. Advance the left item. Left item has the same key as the previous item.
					9. Check if the left hand item matches with the items in the right hand side cache.
					10. If they match, send to output.
					11. If they don't match, dump the cache and test against the right hand side channel.

				*/

				switch joinType {
				case InnerJoin:
					if !isMatch {
						continue
					}
				case LeftJoin:
					if !isMatch {
						fetchRight = false
						outCh <- merger(&itemLeft, nil)
						continue
					}
				case RightJoin:
					if !isMatch {
						fetchLeft = false
						outCh <- merger(nil, &itemRight)
						continue
					}
				case OuterJoin:
					if !isMatch {
						if lastOuterJoin == RightJoin {
							fetchRight = false
							outCh <- merger(&itemLeft, nil)
							lastOuterJoin = LeftJoin
							continue
						} else if lastOuterJoin == LeftJoin {
							fetchLeft = false
							outCh <- merger(nil, &itemRight)
							lastOuterJoin = RightJoin
							continue
						}
					}
				}
				outCh <- merger(&itemLeft, &itemRight)
			}
		}
	}(outCh)

	return Stream[TOut]{
		ctx: outCtx,
		out: []chan TOut{outCh},
	}
}

func Sort[T any](stream Stream[T], comparer func(left, right T) bool) Stream[T] {
	streamOut, _ := Process(stream, func(ctx context.Context, in <-chan T, out chan<- T, errCh chan<- error) {
		buffer := make([]T, 0)

		// Read the items into the buffer, in preparation for a sort.
		for i := range in {
			buffer = append(buffer, i)
		}

		sort.Sort(newSorter(buffer, comparer))

		for _, i := range buffer {
			out <- i
		}
	})

	streamOut.sorted = true

	return streamOut
}

type sorter[T any] struct {
	items    []T
	comparer func(a, b T) bool
}

func newSorter[T any](items []T, comparer func(a, b T) bool) sorter[T] {
	return sorter[T]{
		items:    items,
		comparer: comparer,
	}
}
func (s sorter[T]) Len() int           { return len(s.items) }
func (s sorter[T]) Swap(i, j int)      { s.items[i], s.items[j] = s.items[j], s.items[i] }
func (s sorter[T]) Less(i, j int) bool { return s.comparer(s.items[i], s.items[j]) }

func Filter[T any](stream Stream[T], predicate func(item T) bool) Stream[T] {
	result, _ := Process[T, T](stream, func(ctx context.Context, in <-chan T, out chan<- T, errCh chan<- error) {
		for i := range in {
			if predicate(i) {
				out <- i
			}
		}
	})
	return result
}

func Transform[TIn, TOut any](stream Stream[TIn], expression func(item TIn) TOut) Stream[TOut] {
	result, _ := Process[TIn, TOut](stream, func(ctx context.Context, in <-chan TIn, out chan<- TOut, errCh chan<- error) {
		for i := range in {
			out <- expression(i)
		}
	})

	return result
}

func joinContexts(ctx1, ctx2 context.Context) context.Context {
	ctxOut, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-ctx1.Done():
			case <-ctx2.Done():
				cancel()
			}
		}
	}()

	return ctxOut
}

// Generic Struct
type ConcurrentQueue[T comparable] struct {
	// array of items
	items []T
	// Mutual exclusion lock
	lock sync.Mutex
	// Cond is used to pause mulitple goroutines and wait
	cond *sync.Cond
}

// Initialize ConcurrentQueue
func NewQueue[T comparable]() *ConcurrentQueue[T] {
	q := &ConcurrentQueue[T]{}
	q.cond = sync.NewCond(&q.lock)
	return q
}

// Put the item in the queue
func (q *ConcurrentQueue[T]) Enqueue(item T) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.items = append(q.items, item)
	// Cond signals other go routines to execute
	q.cond.Signal()
}

// Gets the item from queue
func (q *ConcurrentQueue[T]) Dequeue() T {
	q.lock.Lock()
	defer q.lock.Unlock()
	// if Get is called before Put, then cond waits until the Put signals.
	for len(q.items) == 0 {
		q.cond.Wait()
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item
}

func (q *ConcurrentQueue[T]) Len() int {
	return len(q.items)
}
