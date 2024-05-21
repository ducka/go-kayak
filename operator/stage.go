package operator

import (
	"strings"

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

type TestType struct {
}

func (t TestType) GetKey() []string {
	return []string{"test"}
}

func test() {
	Stage[TestType, TestType](DefaultSelector[TestType](), func(item TestType, state TestType) (*TestType, error) {
		return nil, nil
	})
}

// TODO: You probably want to receive a key selector argument, for returning a key for each upstream item.
// TODO: You'll also need a state mutator function

func Stage[TIn, TState any](keySelector KeySelectorFunc[TIn], stateMapper StateMapFunc[TIn, TState], opts ...observe.ObservableOption) OperatorFunc[TIn, TState] {
	opts = defaultActivityName("Stage", opts)
	return func(source *observe.Observable[TIn]) *observe.Observable[TState] {

		out := Pipe3(
			source,
			Batch[TIn](10),
			Map(func(items []TIn, ix int) ([]TState, error) {
				keySelector(items[0])
				// TODO: Perform your state loading / mutation / persisting here
				return []TState{}, nil
			}),
			Flatten[TState](),
		)

		return out
	}
}

type StateStore[TState any] interface {
	//BeginTransaction(key []string)
	Set(key []string, value interface{})
	Get(key []string) interface{}
	Delete(key []string)
	//EndTransaction(key []string)
}

type InMemoryStateStore struct {
	store map[string]interface{}
}

func NewInMemoryStateStore() *InMemoryStateStore {
	return &InMemoryStateStore{
		store: make(map[string]interface{}),
	}
}

func (s *InMemoryStateStore) Set(key []string, value interface{}) {
	k := s.makeKey(key)
	s.store[k] = value
}

func (s *InMemoryStateStore) Get(key []string) interface{} {
	k := s.makeKey(key)
	return s.store[k]
}

func (s *InMemoryStateStore) Delete(key []string) {
	k := s.makeKey(key)
	delete(s.store, k)
}

func (s *InMemoryStateStore) makeKey(key []string) string {
	return strings.Join(key, ":")
}

type stageOptions struct {
	store             StateStore
	observableOptions []observe.ObservableOption
}

type StageOption func(options stageOptions)

func WithStore(store StateStore) StageOption {
	return func(options stageOptions) {
		options.store = store
	}
}

func WithObservableOptions(options ...observe.ObservableOption) StageOption {
	return func(o stageOptions) {
		o.observableOptions = options
	}
}
