package streamsv2

import (
	"fmt"
	"testing"
)

func TestObservable(t *testing.T) {
	t.Run("All test", func(t *testing.T) {
		ob := ObserveProducer[int](func(subscriber StreamWriter[int]) {
			for i := 0; i < 10; i++ {
				subscriber.Write(i)
			}
			subscriber.Complete()
		})

		obStr := Map[int, string](
			func(v int, index uint) (string, error) {
				return fmt.Sprintf("Digit: %d", v), nil
			},
		)(ob)

		obStr.Subscribe(
			func(v string) {
				fmt.Println(v)
			},
			func(err error) {

			},
			func() {

			})
	})
}

// Map transforms the items emitted by an Observable by applying a function to each item.
func Map[TIn any, TOut any](mapper func(TIn, uint) (TOut, error)) OperatorFunc[TIn, TOut] {
	if mapper == nil {
		panic(`rxgo: "Map" expected mapper func`)
	}
	return func(source *Observable[TIn]) *Observable[TOut] {
		var (
			index uint
		)

		// TODO: So I like this ObserveOperation function here, as it'll be use to link the observables together
		// including the connect of each observable. What you need to figure out is how youire' going to handle
		// errors in a chain of observers.. Have a look at RX go for inspiration..
		return ObserveOperation[TIn, TOut](
			source,
			func(streamReader StreamReader[TIn], streamWriter StreamWriter[TOut]) {
				for item := range streamReader.Read() {
					output, err := mapper(item.Value(), index)
					index++

					if err != nil {
						streamWriter.Error(err)
						return
					}

					streamWriter.Write(output)
				}

				streamWriter.Complete()
			})
	}
}
