package operator

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/store"
	"github.com/ducka/go-kayak/streams"
	"github.com/ducka/go-kayak/utils"
)

type (
	KeySelectorFunc[TIn any]      func(TIn) []string
	StateMapFunc[TIn, TState any] func(TIn, TState) (*TState, error)
)

type itemWithKey[TItem any] struct {
	Key  string
	Item TItem
}

func Stage[TIn, TOut any](keySelector KeySelectorFunc[TIn], stateMapper StateMapFunc[TIn, TOut], stateStore store.StateStore[TOut], opts ...observe.ObservableOption) observe.OperatorFunc[TIn, TOut] {
	opts = defaultActivityName("Stage", opts)
	opts = defaultPool(1, opts)

	return func(source *observe.Observable[TIn]) *observe.Observable[TOut] {
		return observe.Operation[TIn, TOut](
			source,
			func(ctx observe.Context, upstream streams.Reader[TIn], downstream streams.Writer[TOut]) {
				//batchStream := streams.NewStream[[]TIn]()
				// TODO: make batch size a configurable setting
				//batcher := newBatcher[TIn](10, utils.ToPtr(time.Millisecond*200))
				stager := newStager[TIn, TOut](keySelector, stateMapper, stateStore)

				//wg := new(sync.WaitGroup)
				//wg.Add(1)
				//
				//go func() {
				//	defer batchStream.Close()
				//	batcher(ctx, upstream, batchStream)
				//}()
				//go func() {
				//	defer wg.Done()
				stager(ctx, batchStream, downstream)
				//}()
				//wg.Wait()
			},
			opts...,
		)
	}
}

func newStager[TIn, TOut any](keySelector KeySelectorFunc[TIn], stateMapper StateMapFunc[TIn, TOut], stateStore store.StateStore[TOut]) observe.OperationFunc[[]TIn, TOut] {
	mapItems := func(items []TIn) (
		[]string,
		[]itemWithKey[TIn],
	) {
		distinctKeys := make(map[string]struct{}, len(items))
		keys := make([]string, 0, len(items))

		mappedItems := make([]itemWithKey[TIn], 0, len(items))

		for _, item := range items {
			key := strings.Join(keySelector(item), ":")
			if _, ok := distinctKeys[key]; !ok {
				distinctKeys[key] = struct{}{}
				keys = append(keys, key)
			}
			mappedItems = append(mappedItems, itemWithKey[TIn]{Key: key, Item: item})
		}

		return keys, mappedItems
	}

	return func(ctx observe.Context, upstream streams.Reader[[]TIn], downstream streams.Writer[TOut]) {
		for batch := range upstream.Read() {
			if batch.IsError() {
				downstream.Error(batch.Error())
				continue
			}

			var distinctKeys, upstreamItems = mapItems(batch.Value())

			var stateEntriesMap map[string]store.StateEntry[TOut]

			var err retry.Error
			errors.As(
				retry.Do(
					func() error {
						stateEntries, err2 := stateStore.Get(ctx, distinctKeys...)
						stateEntriesMap = utils.ArrayToMap(stateEntries, func(entry store.StateEntry[TOut]) string {
							return entry.Key
						})

						if err2 != nil {
							return err2
						}

						for _, upstreamItem := range upstreamItems {
							stateEntry, found := stateEntriesMap[upstreamItem.Key]

							if !found {
								stateEntry = store.StateEntry[TOut]{
									Key:   upstreamItem.Key,
									State: new(TOut),
								}
							}

							state := stateEntry.State

							state, err2 = stateMapper(upstreamItem.Item, *state)

							if err2 != nil {
								return err2
							}

							stateEntry.State = state
							stateEntriesMap[upstreamItem.Key] = stateEntry
						}

						stateEntries = utils.MapToArray(stateEntriesMap)
						err2 = stateStore.Set(ctx, stateEntries...)

						return err2
					},
					retry.Attempts(3),
					retry.MaxJitter(100*time.Millisecond),
					retry.DelayType(retry.RandomDelay),
					retry.Context(ctx),
				),
				&err,
			)

			var lastErr error
			if len(err) > 0 {
				lastErr = err[len(err)-1]
			}

			conflicts := make(map[string]interface{})
			var conflictsErr *store.StateStoreConflict
			if errors.As(lastErr, &conflictsErr) {
				for _, key := range conflictsErr.GetConflicts() {
					conflicts[key] = nil
				}
			}

			for _, stateEntry := range stateEntriesMap {
				if _, found := conflicts[stateEntry.Key]; found {
					downstream.Error(fmt.Errorf("State entry was modified concurrently"))
				} else {
					downstream.Write(*stateEntry.State)
				}
			}
		}
	}
}
