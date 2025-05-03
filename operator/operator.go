package operator

import (
	"github.com/ducka/go-kayak/observe"
)

func defaultActivityName[TIn, TOut any](name string, opts []observe.OperationOption[TIn, TOut]) []observe.OperationOption[TIn, TOut] {
	return append(
		[]observe.OperationOption[TIn, TOut]{
			func(settings *observe.OperationSettings[TIn, TOut]) {
				settings.WithActivityName(name)
			},
		},
		opts...,
	)
}
