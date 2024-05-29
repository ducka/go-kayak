package operator

import (
	"context"
	"encoding/json"
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
					distinctKeys, upstreamItems := mapItems(batch.Value())

					stateStore.Transaction(ctx, distinctKeys, func(tx Transaction[TState]) error {
						getCmds := tx.Get(ctx, distinctKeys...)

						err := tx.Execute(ctx)

						if err != nil {
							// ??
						}

						activeState := make(map[string]*TState, len(getCmds))
						stateOverTime := make([]stateChange[TState], 0, len(upstreamItems))

						// Read the state retrieved from redis
						for k, v := range getCmds {
							state, err := v.Result()

							if err != nil {
								// ??
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

							if err == nil {
								// ??
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
							// ??
						}

						// Emit the state changes over time downstream
						for _, state := range stateOverTime {
							downstream.Write(state.State)
						}

						return nil
					})
				}
			}, opts...),
		)

		return out
	}
}

type StateEntry[TState any] struct {
	Key    string
	State  *TState
	Expiry *time.Duration
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

func NewRedisStateStore[TState any](client redis.UniversalClient) *RedisStateStore[TState] {
	return &RedisStateStore[TState]{
		client:  client,
		marshal: &JsonMarshaller[TState]{},
	}
}

// TODO: You've got an issue with using Watch on a clustered client. Each key may be stored on a different cluster node,
// and you cant execute watch across multple nodes with the same client.
type RedisStateStore[TState any] struct {
	client  redis.UniversalClient
	marshal Marshaller[TState]
}

func (r *RedisStateStore[TState]) Transaction(ctx context.Context, keys []string, callback func(tx Transaction[TState]) error) error {
	//_, err := r.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
	//	tx := &RedisTransaction[TState]{
	//		pipe:    pipe,
	//		marshal: r.marshal,
	//	}
	//
	//	return callback(tx)
	//})
	//
	//return err

	return r.client.Watch(ctx, func(tx *redis.Tx) error {
		pipe := tx.TxPipeline()

		return callback(&RedisTransaction[TState]{
			pipe:    pipe,
			marshal: r.marshal,
		})
	}, keys...)
}

type Transaction[TState any] interface {
	Get(ctx context.Context, keys ...string) map[string]GetCmd[TState]
	Set(ctx context.Context, entries ...StateEntry[TState]) map[string]SetCmd
	Delete(ctx context.Context, keys ...string) map[string]DeleteCmd
	Execute(ctx context.Context) error
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

type GetCmd[TState any] interface {
	Result() (*TState, error)
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

type SetCmd interface {
	Err() error
}

type RedisSetCmd struct {
	cmd *redis.StatusCmd
}

func (c *RedisSetCmd) Err() error {
	return c.cmd.Err()
}

type DeleteCmd interface {
	Err() error
}

type RedisDeleteCmd struct {
	cmd *redis.IntCmd
}

func (c *RedisDeleteCmd) Err() error {
	return c.cmd.Err()
}

type Marshaller[T any] interface {
	Serialize(T) string
	Deserialize(string) T
}
