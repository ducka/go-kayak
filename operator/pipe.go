package operator

import (
	"github.com/ducka/go-kayak/observe"
)

type (
	OperatorFunc[I any, O any] func(source *observe.Observable[I]) *observe.Observable[O]
)

// If there is a commonly used sequence of operators in your code, use the `Pipe` function to
// extract the sequence into a new operator. Even if a sequence is not that common, breaking
// it out into a single operator can improve readability.
func Pipe[S any, O1 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, any],
	f ...OperatorFunc[any, any],
) *observe.Observable[any] {
	result := f1(stream)
	for _, cb := range f {
		result = cb(result)
	}
	return result
}

func Pipe1[S any, O1 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
) *observe.Observable[O1] {
	return f1(stream)
}

func Pipe2[S any, O1 any, O2 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
) *observe.Observable[O2] {
	return f2(f1(stream))
}

func Pipe3[S any, O1 any, O2 any, O3 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
	f3 OperatorFunc[O2, O3],
) *observe.Observable[O3] {
	return f3(f2(f1(stream)))
}

func Pipe4[S any, O1 any, O2 any, O3 any, O4 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
	f3 OperatorFunc[O2, O3],
	f4 OperatorFunc[O3, O4],
) *observe.Observable[O4] {
	return f4(f3(f2(f1(stream))))
}

func Pipe5[S any, O1 any, O2 any, O3 any, O4 any, O5 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
	f3 OperatorFunc[O2, O3],
	f4 OperatorFunc[O3, O4],
	f5 OperatorFunc[O4, O5],
) *observe.Observable[O5] {
	return f5(f4(f3(f2(f1(stream)))))
}

func Pipe6[S any, O1 any, O2 any, O3 any, O4 any, O5 any, O6 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
	f3 OperatorFunc[O2, O3],
	f4 OperatorFunc[O3, O4],
	f5 OperatorFunc[O4, O5],
	f6 OperatorFunc[O5, O6],
) *observe.Observable[O6] {
	return f6(f5(f4(f3(f2(f1(stream))))))
}

func Pipe7[S any, O1 any, O2 any, O3 any, O4 any, O5 any, O6 any, O7 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
	f3 OperatorFunc[O2, O3],
	f4 OperatorFunc[O3, O4],
	f5 OperatorFunc[O4, O5],
	f6 OperatorFunc[O5, O6],
	f7 OperatorFunc[O6, O7],
) *observe.Observable[O7] {
	return f7(f6(f5(f4(f3(f2(f1(stream)))))))
}

func Pipe8[S any, O1 any, O2 any, O3 any, O4 any, O5 any, O6 any, O7 any, O8 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
	f3 OperatorFunc[O2, O3],
	f4 OperatorFunc[O3, O4],
	f5 OperatorFunc[O4, O5],
	f6 OperatorFunc[O5, O6],
	f7 OperatorFunc[O6, O7],
	f8 OperatorFunc[O7, O8],
) *observe.Observable[O8] {
	return f8(f7(f6(f5(f4(f3(f2(f1(stream))))))))
}

func Pipe9[S any, O1 any, O2 any, O3 any, O4 any, O5 any, O6 any, O7 any, O8 any, O9 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
	f3 OperatorFunc[O2, O3],
	f4 OperatorFunc[O3, O4],
	f5 OperatorFunc[O4, O5],
	f6 OperatorFunc[O5, O6],
	f7 OperatorFunc[O6, O7],
	f8 OperatorFunc[O7, O8],
	f9 OperatorFunc[O8, O9],
) *observe.Observable[O9] {
	return f9(f8(f7(f6(f5(f4(f3(f2(f1(stream)))))))))
}

func Pipe10[S any, O1 any, O2 any, O3 any, O4 any, O5 any, O6 any, O7 any, O8 any, O9 any, O10 any](
	stream *observe.Observable[S],
	f1 OperatorFunc[S, O1],
	f2 OperatorFunc[O1, O2],
	f3 OperatorFunc[O2, O3],
	f4 OperatorFunc[O3, O4],
	f5 OperatorFunc[O4, O5],
	f6 OperatorFunc[O5, O6],
	f7 OperatorFunc[O6, O7],
	f8 OperatorFunc[O7, O8],
	f9 OperatorFunc[O8, O9],
	f10 OperatorFunc[O9, O10],
) *observe.Observable[O10] {
	return f10(f9(f8(f7(f6(f5(f4(f3(f2(f1(stream))))))))))
}
