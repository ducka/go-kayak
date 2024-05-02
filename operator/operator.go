package operator

import (
	"github.com/ducka/go-kayak/observe"
)

func defaultActivityName(name string, opts []observe.Option) []observe.Option {
	return append([]observe.Option{observe.WithActivityName(name)}, opts...)
}
