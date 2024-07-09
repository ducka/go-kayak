package observe

import (
	"github.com/ducka/go-kayak/streams"
)

// Fork splits the producer observable into multiple downstream observables.
func Fork[T any](source *Observable[T], count int, opts ...ObservableOption) []*Observable[T] {
	observables := make([]*Observable[T], count)
	streams := make([]streams.Writer[T], count)

	// Create the downstream observables in preparation for propagating the upstream items
	for i := 0; i < count; i++ {
		// TODO: Use the new newStreamObservable contructor here.
		sw, ob := Stream[T](opts...)
		observables[i] = ob
		streams[i] = sw
	}

	// Propagate the upstream items to the downstream observables
	go func() {
		for _, sw := range streams {
			defer sw.Close()
		}

		for i := range source.ToStream().Read() {
			for _, sw := range streams {
				// TODO: There's an optimisation here to ensure no particular downstream observable blocks the fork operation. This may be overkill.
				sw.Send(i)
			}
		}
	}()

	// TODO: I'm not sure this is correct. Pretty sure you shouldn't connect here.
	// Start the observation of the upstream observable.
	source.Connect()

	return observables
}
