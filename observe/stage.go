package observe

import (
	"fmt"
	"strings"

	"github.com/ducka/go-kayak/utils"
)

type (
	SelectorFunc[T any] func(item T) string

	JoinType       string
	StoreOperation string
)

const (
	InnerJoin JoinType = "left"
	LeftJoin  JoinType = "left"
	RightJoin JoinType = "right"
	FullJoin  JoinType = "full"

	Upsert StoreOperation = "upsert"
	Delete StoreOperation = "delete"
)

// Joinable is an interface that must be implemented by the types that are to be joined.
type Identifiable interface {
	GetKey() []string
}

type Storeable interface {
	Identifiable
	GetStoreOp() StoreOperation
}

type StateStore interface {
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

type StageInput string

/*
StateOutput10[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8, TIn9, TIn10]
*/

func Stage10[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8, TIn9, TIn10 Identifiable, TOut any](
	in1 *Observable[TIn1],
	in2 *Observable[TIn2],
	in3 *Observable[TIn3],
	in4 *Observable[TIn4],
	in5 *Observable[TIn5],
	in6 *Observable[TIn6],
	in7 *Observable[TIn7],
	in8 *Observable[TIn8],
	in9 *Observable[TIn9],
	in10 *Observable[TIn10],
	stateMapper func(*TIn1, *TIn2, *TIn3, *TIn4, *TIn5, *TIn6, *TIn7, *TIn8, *TIn9, *TIn10, TOut) (*TOut, error),
	options ...StageOption,
) *Observable[TOut] {

	opts := stageOptions{
		store: NewInMemoryStateStore(),
	}

	for _, option := range options {
		option(opts)
	}

	combinedCtx := utils.CombinedContexts(in1.getContext(), in2.getContext(), in3.getContext(), in4.getContext(), in5.getContext(), in6.getContext(), in7.getContext(), in8.getContext(), in9.getContext(), in10.getContext())
	obsOptions := append([]ObservableOption{WithContext(combinedCtx)}, opts.observableOptions...)

	// TODO: This is basically the same as the Stream observable, except we're suplying parents. See if this can be consolidated.
	sw := newStream[TOut]()
	output := newObservableWithParent[TOut](
		func(streamWriter StreamWriter[TOut], _ observableOptions) {
			for item := range sw.Read() {
				streamWriter.Send(item)
			}
		},
		[]parentObservable{in1, in2, in3, in4, in5, in6, in7, in8, in9, in10},
		obsOptions...,
	)

	var done1, done2, done3, done4, done5, done6, done7, done8, done9, done10 bool

	go func() {
		defer sw.Close()

		for done1 != true || done2 != true || done3 != true || done4 != true || done5 != true || done6 != true || done7 != true || done8 != true || done9 != true || done10 != true {
			select {
			case item, ok := <-in1.ToStream().Read():
				if !ok {
					done1 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(&value, nil, nil, nil, nil, nil, nil, nil, nil, nil, state)
				})
			case item, ok := <-in2.ToStream().Read():
				if !ok {
					done2 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, &value, nil, nil, nil, nil, nil, nil, nil, nil, state)
				})
			case item, ok := <-in3.ToStream().Read():
				if !ok {
					done3 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, nil, &value, nil, nil, nil, nil, nil, nil, nil, state)
				})
			case item, ok := <-in4.ToStream().Read():
				if !ok {
					done4 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, nil, nil, &value, nil, nil, nil, nil, nil, nil, state)
				})
			case item, ok := <-in5.ToStream().Read():
				if !ok {
					done5 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, nil, nil, nil, &value, nil, nil, nil, nil, nil, state)
				})
			case item, ok := <-in6.ToStream().Read():
				if !ok {
					done6 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, nil, nil, nil, nil, &value, nil, nil, nil, nil, state)
				})
			case item, ok := <-in7.ToStream().Read():
				if !ok {
					done7 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, nil, nil, nil, nil, nil, &value, nil, nil, nil, state)
				})
			case item, ok := <-in8.ToStream().Read():
				if !ok {
					done8 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, nil, nil, nil, nil, nil, nil, &value, nil, nil, state)
				})
			case item, ok := <-in9.ToStream().Read():
				if !ok {
					done9 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, nil, nil, nil, nil, nil, nil, nil, &value, nil, state)
				})
			case item, ok := <-in10.ToStream().Read():
				if !ok {
					done10 = true
					break
				}

				processInput(opts.store, item, sw, func(state TOut) (*TOut, error) {
					value := item.Value()
					return stateMapper(nil, nil, nil, nil, nil, nil, nil, nil, nil, &value, state)
				})
			}
		}

	}()

	return output
}

func processInput[TIn Identifiable, TOut any](store StateStore, in Notification[TIn], sw StreamWriter[TOut], callback func(TOut) (*TOut, error)) {
	if in.IsError() {
		sw.Error(in.Error())
		return
	}

	stateIn := new(TOut)
	value := in.Value()
	key := value.GetKey()

	if s := store.Get(key); s != nil {
		stateIn = s.(*TOut)
	}

	stateOut, err := callback(*stateIn)

	if err != nil {
		sw.Error(err)
		return
	}

	if stateOut == nil {
		store.Delete(key)
		return
	}

	fmt.Println(stateOut)

	store.Set(key, stateOut)
	sw.Write(*stateOut)
}

type stageOptions struct {
	store             StateStore
	observableOptions []ObservableOption
}

type StageOption func(options stageOptions)

func WithStore(store StateStore) StageOption {
	return func(options stageOptions) {
		options.store = store
	}
}

func WithObservableOptions(options ...ObservableOption) StageOption {
	return func(o stageOptions) {
		o.observableOptions = options
	}
}

type StateOutput1[TIn1 any] struct {
	Input1 TIn1
}
type StateOutput2[TIn1, TIn2 any] struct {
	StateOutput1[TIn1]
	Input2 TIn2
}
type StateOutput3[TIn1, TIn2, TIn3 any] struct {
	StateOutput2[TIn1, TIn2]
	Input3 TIn3
}
type StateOutput4[TIn1, TIn2, TIn3, TIn4 any] struct {
	StateOutput3[TIn1, TIn2, TIn3]
	Input4 TIn4
}
type StateOutput5[TIn1, TIn2, TIn3, TIn4, TIn5 any] struct {
	StateOutput4[TIn1, TIn2, TIn3, TIn4]
	Input5 TIn5
}
type StateOutput6[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6 any] struct {
	StateOutput5[TIn1, TIn2, TIn3, TIn4, TIn5]
	Input6 TIn6
}
type StateOutput7[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7 any] struct {
	StateOutput6[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6]
	Input7 TIn7
}
type StateOutput8[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8 any] struct {
	StateOutput7[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7]
	Input8 TIn8
}
type StateOutput9[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8, TIn9 any] struct {
	StateOutput8[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8]
	Input9 TIn9
}
type StateOutput10[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8, TIn9, TIn10 any] struct {
	StateOutput9[TIn1, TIn2, TIn3, TIn4, TIn5, TIn6, TIn7, TIn8, TIn9]
	Input10 TIn10
}
