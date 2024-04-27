package kayak

// Map transforms the items emitted by an Observable by applying a function to each item.
func Map[TIn any, TOut any](mapper func(TIn, uint) (TOut, error)) OperatorFunc[TIn, TOut] {
	if mapper == nil {
		panic(`rxgo: "Map" expected mapper func`)
	}
	return func(source *Observable[TIn]) *Observable[TOut] {
		var index uint

		return ObserveOperation[TIn, TOut](
			source,
			func(upstream StreamReader[TIn], downstream StreamWriter[TOut]) {
				for item := range upstream.Read() {
					switch item.Kind() {
					case NextKind:
						output, err := mapper(item.Value(), index)
						index++

						if err != nil {
							downstream.Error(err)
							continue
						}

						downstream.Write(output)
					case ErrorKind:
						downstream.Error(item.Err())
					}
				}
			}, WithActivityName("map observable"))
	}
}
