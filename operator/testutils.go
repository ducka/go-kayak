package operator

import (
	"github.com/ducka/go-kayak/observe"
)

func toResultValues[T any](o *observe.Observable[T]) []T {
	values := make([]T, 0)
	for _, v := range o.ToResult() {
		values = append(values, v.Value())
	}
	return values
}
