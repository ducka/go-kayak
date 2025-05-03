package observe

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ducka/go-kayak/streams"
)

type benchmarkConfig struct {
	items       int
	poolSize    int
	concurrency int
	processor   func(Context, streams.Reader[int], streams.Writer[int])
	writeItems  func(*streams.Stream[int], int)
}

func runPoolingBenchmark(b *testing.B, config benchmarkConfig) {
	// Create a new Round Robin pooling strategy
	strategy := NewRoundRobinPoolingStrategy[int, int](config.poolSize)

	// Create test context
	ctx := NewContext(context.Background(), "benchmark")

	// Create streams
	upstream := streams.NewStream[int]()
	downstream := streams.NewStream[int]()

	// Reset timer before actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Start the strategy execution in a goroutine
		go func() {
			strategy.Execute(ctx, config.processor, upstream, downstream)
			downstream.Close()
		}()

		// Send items through the stream
		config.writeItems(upstream, config.items)
		upstream.Close()

		// Consume all items from downstream
		for range downstream.Read() {
			// Just consume the items
		}
	}
}

func BenchmarkRoundRobinPoolingStrategy(b *testing.B) {
	poolSizes := []int{1, 2, 4, 8, 16, 32, 64}
	itemCounts := []int{100, 1000, 10000}

	for _, items := range itemCounts {
		b.Run("Items_"+strconv.Itoa(items), func(b *testing.B) {
			for _, poolSize := range poolSizes {
				b.Run("PoolSize_"+strconv.Itoa(poolSize), func(b *testing.B) {
					config := benchmarkConfig{
						items:    items,
						poolSize: poolSize,
						processor: func(ctx Context, reader streams.Reader[int], writer streams.Writer[int]) {
							for item := range reader.Read() {
								writer.Send(item)
							}
						},
						writeItems: func(stream *streams.Stream[int], count int) {
							for j := 0; j < count; j++ {
								stream.Write(j)
							}
						},
					}
					runPoolingBenchmark(b, config)
				})
			}
		})
	}
}

func BenchmarkRoundRobinPoolingStrategy_Concurrent(b *testing.B) {
	poolSizes := []int{1, 2, 4, 8, 16, 32, 64}
	itemCounts := []int{100, 1000, 10000}
	concurrencyLevels := []int{1, 2, 4, 8}

	for _, items := range itemCounts {
		b.Run("Items_"+strconv.Itoa(items), func(b *testing.B) {
			for _, concurrency := range concurrencyLevels {
				b.Run("Concurrency_"+strconv.Itoa(concurrency), func(b *testing.B) {
					for _, poolSize := range poolSizes {
						b.Run("PoolSize_"+strconv.Itoa(poolSize), func(b *testing.B) {
							config := benchmarkConfig{
								items:       items,
								poolSize:    poolSize,
								concurrency: concurrency,
								processor: func(ctx Context, reader streams.Reader[int], writer streams.Writer[int]) {
									for item := range reader.Read() {
										time.Sleep(time.Microsecond) // Simulate 1µs of work
										writer.Send(item)
									}
								},
								writeItems: func(stream *streams.Stream[int], count int) {
									for j := 0; j < count; j++ {
										go func(val int) {
											stream.Write(val)
										}(j)
									}
								},
							}
							runPoolingBenchmark(b, config)
						})
					}
				})
			}
		})
	}
}
