package operator

import (
	"github.com/ducka/go-kayak/observe"
)

func defaultActivityName(name string, opts []observe.ObservableOption) []observe.ObservableOption {
	return append([]observe.ObservableOption{observe.WithActivityName(name)}, opts...)
}
