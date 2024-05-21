package observe

func MergeMap2[TIn1, TIn2, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	mapper func(*TIn1, *TIn2) (TOut, error),
) *Observable[TOut] {
	return MergeMap10(
		in1,
		in2,
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		func(in1 *TIn1, in2 *TIn2, in3 *any, in4 *any, in5 *any, in6 *any, in7 *any, in8 *any, in9 *any, in10 *any) (TOut, error) {
			return mapper(in1, in2)
		},
	)
}

func MergeMap3[TIn1, TIn2, TIn3, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	mapper func(*TIn1, *TIn2, *TIn3) (TOut, error),
) *Observable[TOut] {
	return MergeMap10(
		in1,
		in2,
		in3,
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		func(in1 *TIn1, in2 *TIn2, in3 *TIn3, in4 *any, in5 *any, in6 *any, in7 *any, in8 *any, in9 *any, in10 *any) (TOut, error) {
			return mapper(in1, in2, in3)
		},
	)
}

func MergeMap4[TIn1, TIn2, TIn3, TIn4, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	in4 *Observable[TIn4],
	mapper func(*TIn1, *TIn2, *TIn3, *TIn4) (TOut, error),
) *Observable[TOut] {
	return MergeMap10(
		in1,
		in2,
		in3,
		in4,
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		func(in1 *TIn1, in2 *TIn2, in3 *TIn3, in4 *TIn4, in5 *any, in6 *any, in7 *any, in8 *any, in9 *any, in10 *any) (TOut, error) {
			return mapper(in1, in2, in3, in4)
		},
	)
}

func MergeMap5[TIn1, TIn2, TIn3, TIn4, TIn5, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	in4 *Observable[TIn4],
	in5 *Observable[TIn5],
	mapper func(*TIn1, *TIn2, *TIn3, *TIn4, *TIn5) (TOut, error),
) *Observable[TOut] {
	return MergeMap10(
		in1,
		in2,
		in3,
		in4,
		in5,
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		func(in1 *TIn1, in2 *TIn2, in3 *TIn3, in4 *TIn4, in5 *TIn5, in6 *any, in7 *any, in8 *any, in9 *any, in10 *any) (TOut, error) {
			return mapper(in1, in2, in3, in4, in5)
		},
	)
}

func MergeMap6[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	in4 *Observable[TIn4],
	in5 *Observable[TIn5],
	in6 *Observable[TIn6],
	mapper func(*TIn1, *TIn2, *TIn3, *TIn4, *TIn5, *TIn6) (TOut, error),
) *Observable[TOut] {
	return MergeMap10(
		in1,
		in2,
		in3,
		in4,
		in5,
		in6,
		Empty[any](),
		Empty[any](),
		Empty[any](),
		Empty[any](),
		func(in1 *TIn1, in2 *TIn2, in3 *TIn3, in4 *TIn4, in5 *TIn5, in6 *TIn6, in7 *any, in8 *any, in9 *any, in10 *any) (TOut, error) {
			return mapper(in1, in2, in3, in4, in5, in6)
		},
	)
}

func MergeMap7[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	in4 *Observable[TIn4],
	in5 *Observable[TIn5],
	in6 *Observable[TIn6],
	in7 *Observable[TIn7],
	mapper func(*TIn1, *TIn2, *TIn3, *TIn4, *TIn5, *TIn6, *TIn7) (TOut, error),
) *Observable[TOut] {
	return MergeMap10(
		in1,
		in2,
		in3,
		in4,
		in5,
		in6,
		in7,
		Empty[any](),
		Empty[any](),
		Empty[any](),
		func(in1 *TIn1, in2 *TIn2, in3 *TIn3, in4 *TIn4, in5 *TIn5, in6 *TIn6, in7 *TIn7, in8 *any, in9 *any, in10 *any) (TOut, error) {
			return mapper(in1, in2, in3, in4, in5, in6, in7)
		},
	)
}

func MergeMap8[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	in4 *Observable[TIn4],
	in5 *Observable[TIn5],
	in6 *Observable[TIn6],
	in7 *Observable[TIn7],
	in8 *Observable[TIn8],
	mapper func(*TIn1, *TIn2, *TIn3, *TIn4, *TIn5, *TIn6, *TIn7, *TIn8) (TOut, error),
) *Observable[TOut] {
	return MergeMap10(
		in1,
		in2,
		in3,
		in4,
		in5,
		in6,
		in7,
		in8,
		Empty[any](),
		Empty[any](),
		func(in1 *TIn1, in2 *TIn2, in3 *TIn3, in4 *TIn4, in5 *TIn5, in6 *TIn6, in7 *TIn7, in8 *TIn8, in9 *any, in10 *any) (TOut, error) {
			return mapper(in1, in2, in3, in4, in5, in6, in7, in8)
		},
	)
}

func MergeMap9[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8, TIn9, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	in4 *Observable[TIn4],
	in5 *Observable[TIn5],
	in6 *Observable[TIn6],
	in7 *Observable[TIn7],
	in8 *Observable[TIn8],
	in9 *Observable[TIn9],
	mapper func(*TIn1, *TIn2, *TIn3, *TIn4, *TIn5, *TIn6, *TIn7, *TIn8, *TIn9) (TOut, error),
) *Observable[TOut] {
	return MergeMap10(
		in1,
		in2,
		in3,
		in4,
		in5,
		in6,
		in7,
		in8,
		in9,
		Empty[any](),
		func(in1 *TIn1, in2 *TIn2, in3 *TIn3, in4 *TIn4, in5 *TIn5, in6 *TIn6, in7 *TIn7, in8 *TIn8, in9 *TIn9, in10 *any) (TOut, error) {
			return mapper(in1, in2, in3, in4, in5, in6, in7, in8, in9)
		},
	)
}

func MergeMap10[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8, TIn9, TIn10, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	in4 *Observable[TIn4],
	in5 *Observable[TIn5],
	in6 *Observable[TIn6],
	in7 *Observable[TIn7],
	in8 *Observable[TIn8],
	in9 *Observable[TIn9],
	in10 *Observable[TIn10],
	mapper func(*TIn1, *TIn2, *TIn3, *TIn4, *TIn5, *TIn6, *TIn7, *TIn8, *TIn9, *TIn10) (TOut, error),
) *Observable[TOut] {
	parents := []upstreamObservable{in1, in2, in3, in4, in5, in6, in7, in8, in9, in10}
	downstream, output := newStreamObservable[TOut](parents)

	go func() {
		defer downstream.Close()

		var done1, done2, done3, done4, done5, done6, done7, done8, done9, done10 bool

		for done1 != true || done2 != true || done3 != true || done4 != true || done5 != true || done6 != true || done7 != true || done8 != true || done9 != true || done10 != true {

			select {
			case item, ok := <-in1.ToStream().Read():
				if !ok {
					done1 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn1) (TOut, error) {
					return mapper(&value, nil, nil, nil, nil, nil, nil, nil, nil, nil)
				})
			case item, ok := <-in2.ToStream().Read():
				if !ok {
					done2 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn2) (TOut, error) {
					return mapper(nil, &value, nil, nil, nil, nil, nil, nil, nil, nil)
				})
			case item, ok := <-in3.ToStream().Read():
				if !ok {
					done3 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn3) (TOut, error) {
					return mapper(nil, nil, &value, nil, nil, nil, nil, nil, nil, nil)
				})
			case item, ok := <-in4.ToStream().Read():
				if !ok {
					done4 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn4) (TOut, error) {
					return mapper(nil, nil, nil, &value, nil, nil, nil, nil, nil, nil)
				})
			case item, ok := <-in5.ToStream().Read():
				if !ok {
					done5 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn5) (TOut, error) {
					return mapper(nil, nil, nil, nil, &value, nil, nil, nil, nil, nil)
				})
			case item, ok := <-in6.ToStream().Read():
				if !ok {
					done6 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn6) (TOut, error) {
					return mapper(nil, nil, nil, nil, nil, &value, nil, nil, nil, nil)
				})
			case item, ok := <-in7.ToStream().Read():
				if !ok {
					done7 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn7) (TOut, error) {
					return mapper(nil, nil, nil, nil, nil, nil, &value, nil, nil, nil)
				})
			case item, ok := <-in8.ToStream().Read():
				if !ok {
					done8 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn8) (TOut, error) {
					return mapper(nil, nil, nil, nil, nil, nil, nil, &value, nil, nil)
				})
			case item, ok := <-in9.ToStream().Read():
				if !ok {
					done9 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn9) (TOut, error) {
					return mapper(nil, nil, nil, nil, nil, nil, nil, nil, &value, nil)
				})
			case item, ok := <-in10.ToStream().Read():
				if !ok {
					done10 = true
					break
				}
				processMergeInput(item, downstream, func(value TIn10) (TOut, error) {
					return mapper(nil, nil, nil, nil, nil, nil, nil, nil, nil, &value)
				})
			}

		}

	}()

	return output
}

func processMergeInput[TIn any, TOut any](input Notification[TIn], downstream StreamWriter[TOut], callback func(TIn) (TOut, error)) {
	if input.IsError() {
		downstream.Error(input.Error())
		return
	}

	output, err := callback(input.Value())

	if err != nil {
		downstream.Error(err)
		return
	}

	if output != nil {
		downstream.Write(output)
	}
}
