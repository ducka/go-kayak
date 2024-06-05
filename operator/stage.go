package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ducka/go-kayak/observe"
	"github.com/redis/go-redis/v9"
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

type upstreamItem[TItem any] struct {
	Key  string
	Item TItem
}

type stateChange[TState any] struct {
	State TState
	Err   error
}

type stateEnvelope[TState any] struct {
	State     TState
	Timestamp int64
}

func Stage[TIn, TState any](keySelector KeySelectorFunc[TIn], stateMapper StateMapFunc[TIn, TState], stateStore StateStore[TState], opts ...observe.ObservableOption) observe.OperatorFunc[TIn, TState] {
	opts = defaultActivityName("Stage", opts)

	return func(source *observe.Observable[TIn]) *observe.Observable[TState] {

		mapItems := func(items []TIn) (
			[]string,
			[]upstreamItem[TIn],
		) {
			distinctKeys := make(map[string]struct{}, len(items))
			keys := make([]string, 0, len(items))
			mappedItems := make([]upstreamItem[TIn], 0, len(items))

			for _, item := range items {
				key := strings.Join(keySelector(item), ":")
				if _, ok := distinctKeys[key]; !ok {
					distinctKeys[key] = struct{}{}
					keys = append(keys, key)
				}
				mappedItems = append(mappedItems, upstreamItem[TIn]{Key: key, Item: item})
			}

			return keys, mappedItems
		}

		out := observe.Pipe2(
			source,
			BatchWithTimeout[TIn](10, 50*time.Millisecond),
			Process(func(ctx observe.Context, upstream observe.StreamReader[[]TIn], downstream observe.StreamWriter[TState]) {

				for batch := range upstream.Read() {
					if batch.IsError() {
						downstream.Error(batch.Error())
						continue
					}

					var distinctKeys, upstreamItems = mapItems(batch.Value())

					// TODO: handle error...
					err := stateStore.Transaction(ctx, distinctKeys, func(tx Transaction[TState]) error {
						getCmds := tx.Get(ctx, distinctKeys...)

						err := tx.Execute(ctx)

						//if err != nil {
						//	return err
						//}

						activeState := make(map[string]*TState, len(getCmds))
						stateOverTime := make([]stateChange[TState], 0, len(upstreamItems))

						// Read the state retrieved from redis
						for k, v := range getCmds {
							state, err := v.Result()

							if err != nil {
								return err
							}

							if state == nil {
								state = new(TState)
							}

							activeState[k] = state
						}

						// Iterate over the upstream items and apply it to the retrieved state
						for _, upstreamItem := range upstreamItems {
							state := activeState[upstreamItem.Key]

							state, err = stateMapper(upstreamItem.Item, *state)

							if err != nil {
								return err
							}

							if state == nil {
								tx.Delete(ctx, upstreamItem.Key)
								delete(activeState, upstreamItem.Key)
							}

							activeState[upstreamItem.Key] = state
							stateOverTime = append(stateOverTime, stateChange[TState]{State: *state, Err: err})
						}

						// Save the active state back to redis
						for k, v := range activeState {
							tx.Set(ctx, StateEntry[TState]{Key: k, State: v})
						}

						err = tx.Execute(ctx)

						if err != nil {
							return err
						}

						// Emit the state changes over time downstream
						for _, state := range stateOverTime {
							downstream.Write(state.State)
						}

						return nil
					})

					if err != nil {
						fmt.Println(err)

					}
				}

			}, opts...),
		)

		return out
	}
}

type (
	RetryApproach string
)

const (
	DontRetry   RetryApproach = "DontRetry"
	RandomRetry RetryApproach = "LinearRetry"
	ReduceBatch RetryApproach = "ReduceBatch"
)

type RetryProcessor[TItem any] func(ctx observe.Context, batch []TItem) error

type RetryStrategy[TItem any] interface {
	Process(ctx observe.Context, batch []TItem, processor RetryProcessor[TItem]) error
	ShouldRetry(err error) bool
}

type DefaultRetryStrategy[TItem any] struct {
}

func (d *DefaultRetryStrategy[TItem]) Process(ctx observe.Context, batch []TItem, processor RetryProcessor[TItem]) error {
	//err := processor(ctx, batch)

	return nil
}

func (d *DefaultRetryStrategy[TItem]) shouldRetry(err error) RetryApproach {
	return DontRetry
}

var ConcurrencyError error = concurrencyError{}

type concurrencyError struct{}

func (c concurrencyError) Error() string {
	return "State entry was modified concurrently"
}

type StateEntry[TState any] struct {
	Key    string
	State  *TState
	Expiry *time.Duration
}

type Marshaller[T any] interface {
	Serialize(T) string
	Deserialize(string) T
}

type JsonMarshaller[T any] struct {
}

func (j JsonMarshaller[T]) Serialize(t T) string {
	bytes, _ := json.Marshal(t)
	return string(bytes)
}

func (j JsonMarshaller[T]) Deserialize(s string) T {
	var t T
	json.Unmarshal([]byte(s), &t)
	return t
}

type StateStore[TState any] interface {
	Transaction(ctx context.Context, keys []string, callback func(tx Transaction[TState]) error) error
}

type Transaction[TState any] interface {
	Get(ctx context.Context, keys ...string) map[string]GetCmd[TState]
	Set(ctx context.Context, entries ...StateEntry[TState]) map[string]SetCmd
	Delete(ctx context.Context, keys ...string) map[string]DeleteCmd
	Execute(ctx context.Context) error
}

type SetCmd interface {
	Err() error
}

type DeleteCmd interface {
	Err() error
}

type GetCmd[TState any] interface {
	Result() (*TState, error)
}

// Redis State Store

func NewRedisStateStore[TState any](client *redis.Client) *RedisStateStore[TState] {
	if client == nil {
		panic("Argument 'client' must be specified")
	}
	return &RedisStateStore[TState]{
		client:  client,
		marshal: &JsonMarshaller[TState]{},
	}
}

type RedisStateStore[TState any] struct {
	client  *redis.Client
	marshal Marshaller[TState]
}

func (r *RedisStateStore[TState]) Transaction(ctx context.Context, keys []string, callback func(tx Transaction[TState]) error) error {
	return r.client.Watch(ctx, func(tx *redis.Tx) error {
		pipe := tx.TxPipeline()

		return callback(&RedisTransaction[TState]{
			pipe:    pipe,
			marshal: r.marshal,
		})
	}, keys...)
}

type RedisTransaction[TState any] struct {
	pipe    redis.Pipeliner
	marshal Marshaller[TState]
}

func (r *RedisTransaction[TState]) Get(ctx context.Context, keys ...string) map[string]GetCmd[TState] {
	results := make(map[string]GetCmd[TState])
	for _, key := range keys {
		results[key] = &RedisGetCmd[TState]{
			cmd:     r.pipe.Get(ctx, key),
			marshal: r.marshal,
		}
	}
	return results
}

func (r *RedisTransaction[TState]) Set(ctx context.Context, entries ...StateEntry[TState]) map[string]SetCmd {
	results := make(map[string]SetCmd)

	for _, entry := range entries {
		serializedState := r.marshal.Serialize(*entry.State)

		if entry.Expiry != nil {
			results[entry.Key] = &RedisSetCmd{
				cmd: r.pipe.Set(ctx, entry.Key, serializedState, *entry.Expiry),
			}
			continue
		}

		results[entry.Key] = &RedisSetCmd{
			cmd: r.pipe.Set(ctx, entry.Key, serializedState, 0),
		}
	}

	return results
}

func (r *RedisTransaction[TState]) Delete(ctx context.Context, keys ...string) map[string]DeleteCmd {
	results := make(map[string]DeleteCmd)
	for _, key := range keys {
		results[key] = &RedisDeleteCmd{
			cmd: r.pipe.Del(ctx, key),
		}
	}
	return results
}

func (r *RedisTransaction[TState]) Execute(ctx context.Context) error {
	_, err := r.pipe.Exec(ctx)
	return err
}

type RedisGetCmd[TState any] struct {
	cmd     *redis.StringCmd
	marshal Marshaller[TState]
}

func (c *RedisGetCmd[TState]) Result() (*TState, error) {
	if c.cmd.Err() == redis.Nil {
		return nil, nil
	} else if c.cmd.Err() != nil {
		return nil, c.cmd.Err()
	}

	result := c.marshal.Deserialize(c.cmd.Val())

	return &result, nil
}

type RedisSetCmd struct {
	cmd *redis.StatusCmd
}

func (c *RedisSetCmd) Err() error {
	return c.cmd.Err()
}

type RedisDeleteCmd struct {
	cmd *redis.IntCmd
}

func (c *RedisDeleteCmd) Err() error {
	return c.cmd.Err()
}
