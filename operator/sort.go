package operator

import (
	"sort"

	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/streams"
)

type (
	SorterFunc[T any] func(left, right T) bool
)

func Sort[T any](comparer SorterFunc[T], opts ...observe.ObservableOption) observe.OperatorFunc[T, T] {
	opts = defaultActivityName("Sort", opts)
	return func(source *observe.Observable[T]) *observe.Observable[T] {
		return observe.Operation[T, T](
			source,
			func(ctx observe.Context, upstream streams.Reader[T], downstream streams.Writer[T]) {
				sorted := make([]streams.Notification[T], 0)
				unsorted := make([]streams.Notification[T], 0)

				// Read the Items into the buffer, in preparation for a sort.
				for i := range upstream.Read() {
					if !i.IsError() {
						sorted = append(sorted, i)
					} else {
						unsorted = append(unsorted, i)
					}
				}

				sort.Sort(newSorter[T](sorted, comparer))

				for _, i := range sorted {
					downstream.Send(i)
				}

				for _, i := range unsorted {
					downstream.Send(i)
				}
			},
			opts...,
		)
	}
}

type sorter[T any] struct {
	items    []streams.Notification[T]
	comparer SorterFunc[T]
}

func newSorter[T any](items []streams.Notification[T], comparer SorterFunc[T]) sorter[T] {
	return sorter[T]{
		items:    items,
		comparer: comparer,
	}
}
func (s sorter[T]) Len() int           { return len(s.items) }
func (s sorter[T]) Swap(i, j int)      { s.items[i], s.items[j] = s.items[j], s.items[i] }
func (s sorter[T]) Less(i, j int) bool { return s.comparer(s.items[i].Value(), s.items[j].Value()) }
