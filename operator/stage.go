package operator

import (
	"strings"
	"sync"
	"time"

	"github.com/ducka/go-kayak/observe"
)

type (
	KeySelectorFunc[TIn any]      func(TIn) []string
	StateMapFunc[TIn, TState any] func(TIn, TState) (*TState, error)
)

func DefaultSelector[T Identifiable]() KeySelectorFunc[T] {
	return func(item T) []string {
		return item.GetKey()
	}
}

type Identifiable interface {
	GetKey() []string
}

//type TestType struct {
//}
//
//func (t TestType) GetKey() []string {
//	return []string{"test"}
//}
//
//func test() {
//	Stage[TestType, TestType](DefaultSelector[TestType](), func(item TestType, state TestType) (*TestType, error) {
//		return nil, nil
//	})
//}

// TODO: You probably want to receive a key selector argument, for returning a key for each upstream item.
// TODO: You'll also need a state mutator function

func Stage[TIn, TState any](keySelector KeySelectorFunc[TIn], stateMapper StateMapFunc[TIn, TState], stateStore StateStore[TState], opts ...observe.ObservableOption) observe.OperatorFunc[TIn, TState] {
	opts = defaultActivityName("Stage", opts)
	return func(source *observe.Observable[TIn]) *observe.Observable[TState] {

		mapItems := func(items []TIn) ([]StateKey, map[StateKey]TIn) {
			k := make([]StateKey, 0, len(items))
			i := make(map[StateKey]TIn, len(items))

			for _, item := range items {
				key := StateKey(strings.Join(keySelector(item), ":"))
				k = append(k, key)
				i[key] = item
			}

			return k, i
		}

		out := observe.Pipe2(
			source,
			BatchWithTimeout[TIn](10, 50*time.Millisecond),
			Process(func(ctx observe.Context, upstream observe.StreamReader[[]TIn], downstream observe.StreamWriter[TState]) {
				for batch := range upstream.Read() {
					// TODO: Get a distinct list of itemKeys, get the original list of items + extracted key
					itemKeys, itemMap := mapItems(batch.Value())

					tx := stateStore.CreatePipe()
					defer tx.Close()

					// Retrieve state for the item keys
					tx.Lock(itemKeys...)

					// TODO: load the state returned from the store into a key value map
					stateEntries := tx.Get(itemKeys...)
					err := tx.Execute()

					if err != nil {
						// TODO: what to do...
					}

					stateMapperResults := make([]struct {
						state TState
						err   error
					}, 0, len(itemKeys))

					// TODO: Loop over your itemList + extracted keys
					for _, e := range stateEntries {
						entry := e.Result()
						state := entry.State

						if state == nil {
							state = new(TState)
						}

						// TODO: Pass your item + state to the mapper.
						state, err := stateMapper(itemMap[entry.Key], *state)

						if state == nil {
							tx.Delete(entry.Key)
							continue
						}

						stateMapperResults = append(
							stateMapperResults,
							struct {
								state TState
								err   error
							}{*state, err},
						)

						tx.Set(StateEntry[TState]{
							Key:   entry.Key,
							State: state,
						})
					}

					err = tx.Execute()

					if err != nil {
						// What to do....
					}

					for _, result := range stateMapperResults {
						if result.err != nil {
							downstream.Error(result.err)
							continue
						}

						downstream.Write(result.state)
					}
				}
			}, opts...),
		)

		return out
	}
}

type StateKey string

type StateEntry[T any] struct {
	Key   StateKey
	State *T
}

type StateStoreResults struct {
}

type StateStore[TState any] interface {
	CreatePipe() StateStorePipe[TState]
}

type StateStoreGetCmd[TState any] struct {
	key   StateKey
	state *TState
}

func (s StateStoreGetCmd[TState]) Result() *StateEntry[TState] {
	return &StateEntry[TState]{
		Key:   s.key,
		State: s.state,
	}
}

type StateStorePipe[TState any] interface {
	Lock(keys ...StateKey)
	Get(keys ...StateKey) []StateStoreGetCmd[TState]
	Set(entries ...StateEntry[TState])
	Delete(keys ...StateKey)
	Execute() error
	Close()
}

func NewInMemoryStateStore[TState any]() *InMemoryStateStore[TState] {
	return &InMemoryStateStore[TState]{}
}

type InMemoryStateStore[TState any] struct {
}

func (s *InMemoryStateStore[TState]) CreatePipe() StateStorePipe[TState] {
	return NewInMemoryStateStorePipe[TState]()
}

func NewInMemoryStateStorePipe[TState any]() *InMemoryStateStorePipe[TState] {
	return &InMemoryStateStorePipe[TState]{
		store: make(map[StateKey]*TState),
		mu:    new(sync.Mutex),
	}
}

type InMemoryStateStorePipe[TState any] struct {
	store map[StateKey]*TState
	mu    *sync.Mutex
}

func (i InMemoryStateStorePipe[TState]) Lock(keys ...StateKey) {
	i.mu.Lock()
}

func (i InMemoryStateStorePipe[TState]) Get(keys ...StateKey) []StateStoreGetCmd[TState] {
	results := make([]StateStoreGetCmd[TState], 0, len(keys))
	for _, k := range keys {
		results = append(results, StateStoreGetCmd[TState]{
			key:   k,
			state: i.store[k],
		})
	}
	return results
}

func (i InMemoryStateStorePipe[TState]) Set(entries ...StateEntry[TState]) {
	for _, k := range entries {
		i.store[k.Key] = k.State
	}
}

func (i InMemoryStateStorePipe[TState]) Delete(keys ...StateKey) {
	for _, k := range keys {
		delete(i.store, k)
	}
}

func (i InMemoryStateStorePipe[TState]) Execute() error {
	return nil
}

func (i InMemoryStateStorePipe[TState]) Close() {
	i.mu.Unlock()
}
