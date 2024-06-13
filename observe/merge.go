package observe

import (
	"sync"
)

// Merge combines multiple observables into a single observable.The
// order of the emitted items is as they come, with no order guaranteed.
func Merge[T any](observables ...*Observable[T]) *Observable[T] {
	return merge(observables, false)
}

// MergeWithDelayedErrors combines multiple observables into a single
// observable, delaying errors until all observables have completed. The
// order of the emitted items is as they come, with no order guaranteed.
func MergeWithDelayedErrors[T any](observables ...*Observable[T]) *Observable[T] {
	return merge(observables, true)
}

func merge[T any](
	observables []*Observable[T],
	delayErrors bool,
	opts ...ObservableOption,
) *Observable[T] {
	return newObservable[T](
		func(downstream stream.StreamWriter[T], opts observableOptions) {
			mu := &sync.Mutex{}
			startWg := &sync.WaitGroup{}
			startWg.Add(1)
			closeWg := &sync.WaitGroup{}
			closeWg.Add(len(observables))

			errors := make([]Notification[T], 0)

			f := func(o *Observable[T]) {
				defer closeWg.Done()
				// block until all observables are observing
				startWg.Wait()
				for item := range o.ToStream().Read() {
					if item.IsError() && delayErrors {
						mu.Lock()
						errors = append(errors, item)
						mu.Unlock()
						continue
					}
					downstream.Send(item)
				}
			}

			for _, o := range observables {
				go f(o)
				// TODO: Why am I doing a connect here? This doesn't seem right.
				o.Connect()
			}

			startWg.Done()
			closeWg.Wait()
		},
		mapToParentObservable(observables...),
		opts...,
	)
}
