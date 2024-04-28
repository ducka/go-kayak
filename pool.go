package kayak

import (
	"sync"
)

type goPool struct {
	size uint
}

func newPool(size uint) *goPool {
	return &goPool{
		size: size,
	}
}

func (p *goPool) run(f func()) {
	wg := &sync.WaitGroup{}

	wg.Add(int(p.size))

	for i := 0; i < int(p.size); i++ {
		go func() {
			defer wg.Done()
			f()
		}()
	}

	wg.Wait()
}
