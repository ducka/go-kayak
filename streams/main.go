package streams

import (
	"github.com/ducka/go-kayak/streamsv2"
)

func main() {
	streamsv2.ObserveProducer[int](func(subscriber streamsv2.StreamWriter[int]) {
		for i := 0; i < 10; i++ {
			subscriber.Write(i)
		}
		subscriber.Complete()
	})
}
