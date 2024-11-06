package observe

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ducka/go-kayak/streams"
	"github.com/ducka/go-kayak/utils"
)

type IterLock struct {
	muCk sync.Mutex
	muWg sync.RWMutex
	wg   sync.WaitGroup

	reset bool
	iter  int
	limit int
}

func NewIterLock(limit int) *IterLock {
	i := &IterLock{
		limit: limit,
	}

	return i
}

func (s *IterLock) Checkpoint() {
	s.muCk.Lock()
	s.iter++

	//fmt.Printf("iter: %d\n", s.iter%s.limit)
	if s.iter%s.limit == 0 {
		//fmt.Printf("unlocking: %d\n", s.iter)
		s.wg.Done()
		s.reset = false
		s.muCk.Unlock()
	} else {
		//fmt.Printf("waiting: %d\n", s.iter)
		if !s.reset {
			s.muWg.Lock()
			s.wg.Add(1)
			s.reset = true
			s.muWg.Unlock()
		}
		s.muCk.Unlock()
		s.muWg.RLock()
		s.wg.Wait()
		s.muWg.RUnlock()
	}
}

func Test_Test(t *testing.T) {
	itrLock := NewIterLock(3)

	for i := 0; i < 20; i++ {
		go func(i int) {
			itrLock.Checkpoint()
			fmt.Printf("released: %d\n", i)
		}(i)
		time.Sleep(1 * time.Second)
	}
}

func Test_PartitionedPoolingStrategy_Execute(t *testing.T) {
	ctx := NewContext(context.Background(), "test")
	keySelector := func(i int) string { return strconv.Itoa(i) }
	var hashFunc HashFunc = func(key string) uint16 {
		i, _ := strconv.Atoi(key)
		return uint16(i)
	}
	settings := ParitionedPoolSettings{
		PoolSize: utils.ToPtr(uint16(10)),
		//BufferSize: utils.ToPtr(uint64(10)),
		HashFunc: utils.ToPtr(hashFunc),
	}

	sut := NewPartitionedPoolingStrategy[int, int](keySelector, settings)

	chk := NewIterLock(5)

	wg := sync.WaitGroup{}
	wg.Add(2)
	upstream := streams.NewStream[int]()
	downstream := streams.NewStream[int]()

	operation := func(ctx Context, streamReader streams.Reader[int], streamWriter streams.Writer[int]) {
		for item := range streamReader.Read() {
			fmt.Printf("Processing: %d\n", item.Value())
			chk.Checkpoint()
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
