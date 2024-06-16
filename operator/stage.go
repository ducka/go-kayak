package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/ducka/go-kayak/observe"
	"github.com/ducka/go-kayak/stream"
	"github.com/ducka/go-kayak/utils"
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

type itemWithKey[TItem any] struct {
	Key  string
	Item TItem
}

type stateChange[TState any] struct {
	State TState
	Err   error
}

func Stage[TIn, TOut any](keySelector KeySelectorFunc[TIn], stateMapper StateMapFunc[TIn, TOut], stateStore StateStore[TOut], opts ...observe.ObservableOption) observe.OperatorFunc[TIn, TOut] {
	opts = defaultActivityName("Stage", opts)

	return func(source *observe.Observable[TIn]) *observe.Observable[TOut] {
		return observe.Operation[TIn, TOut](
			source,
			func(ctx observe.Context, upstream stream.Reader[TIn], downstream stream.Writer[TOut]) {
				batchStream := stream.NewStream[[]TIn]()
				batcher := newBatcher[TIn](10, utils.ToPtr(time.Millisecond*200))
				stager := newStager[TIn, TOut](keySelector, stateMapper, stateStore)

				// TODO: Consider wrapping this pattern up into some sort of "flow" abstraction. Something that
				// handles the instantiation of the stream, calling of the operation, etc.
				// TODO: How does client cancellation work with below?
				// TODO: Would this new flow abstraction have to support client cancellation?
				// TODO: Should flow some how be used inside Observable?

				wg := new(sync.WaitGroup)
				wg.Add(1)

				go func() {
					defer batchStream.Close()
					batcher(ctx, upstream, batchStream)
				}()
				go func() {
					defer wg.Done()
					stager(ctx, batchStream, downstream)
				}()
				wg.Wait()
			},
			opts...,
		)
	}
}

func newStager[TIn, TOut any](keySelector KeySelectorFunc[TIn], stateMapper StateMapFunc[TIn, TOut], stateStore StateStore[TOut]) observe.OperationFunc[[]TIn, TOut] {
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

	return func(ctx observe.Context, upstream stream.Reader[[]TIn], downstream stream.Writer[TOut]) {
		for batch := range upstream.Read() {
			if batch.IsError() {
				downstream.Error(batch.Error())
				continue
			}

			var distinctKeys, upstreamItems = mapItems(batch.Value())

			var stateEntries map[string]StateEntry[TOut]

			var err retry.Error
			errors.As(
				retry.Do(
					func() error {
						var err2 error
						stateEntries, err2 = stateStore.Get(ctx, distinctKeys...)

						if err2 != nil {
							return err2
						}

						for _, upstreamItem := range upstreamItems {
							stateEntry := stateEntries[upstreamItem.Key]
							state := stateEntry.State

							if state == nil {
								state = new(TOut)
							}

							state, err2 = stateMapper(upstreamItem.Item, *state)

							if err2 != nil {
								return err2
							}

							stateEntry.State = state
							stateEntries[upstreamItem.Key] = stateEntry
						}

						err2 = stateStore.Set(ctx, stateEntries)

						return err2
					},
					retry.Attempts(3),
				),
				&err,
			)

			var lastErr error
			if len(err) > 0 {
				lastErr = err[len(err)-1]
			}

			conflicts := make(map[string]interface{})
			var conflictsErr *StateStoreConflict
			if errors.As(lastErr, &conflictsErr) {
				for _, key := range conflictsErr.GetConflicts() {
					conflicts[key] = nil
				}
			}

			for _, stateEntry := range stateEntries {
				if _, found := conflicts[stateEntry.Key]; found {
					downstream.Error(fmt.Errorf("State entry was modified concurrently"))
				} else {
					downstream.Write(*stateEntry.State)
				}
			}
		}
	}
}

type StateEntry[TState any] struct {
	Key       string
	State     *TState
	Timestamp *int64
	Expiry    *time.Duration
}

type Marshaller interface {
	Serialize(inObj interface{}) (string, error)
	Deserialize(inJson string, outObj interface{}) error
}

type JsonMarshaller struct {
}

func (j JsonMarshaller) Serialize(t any) (string, error) {
	bytes, err := json.Marshal(t)
	return string(bytes), err
}

func (j JsonMarshaller) Deserialize(s string, out interface{}) error {
	return json.Unmarshal([]byte(s), out)
}

type StateStore[TState any] interface {
	Get(ctx context.Context, keys ...string) (map[string]StateEntry[TState], error)
	Set(ctx context.Context, entries map[string]StateEntry[TState]) error
}

type StateStoreConflict struct {
	conflicts []string
}

func (s *StateStoreConflict) Error() string {
	return "State entry was modified concurrently"
}

func (s *StateStoreConflict) GetConflicts() []string {
	return s.conflicts
}

type RedisStateStore[TState any] struct {
	client     *redis.Client
	marshaller Marshaller
}

func NewRedisStateStore[TState any](client *redis.Client) *RedisStateStore[TState] {
	return &RedisStateStore[TState]{
		client:     client,
		marshaller: &JsonMarshaller{},
	}
}

// TODO: Tweak this script so it doesn't have to send back empty entries. If the entry doesn't exist in the response it should be assumed to not be in the cache.
const (
	getStateLuaScript = `
local results = {}

for i, key in ipairs(KEYS) do
	local values = redis.call('HMGET', key, 'value', 'timestamp')
	local hasValue = values[1] ~= false

	if hasValue then
		local result  = { 
			State = nil,
			Timestamp = nil,
		}

		result.State = values[1]
		result.Timestamp = tonumber(values[2])

		results[key] = result
	end
end

return cjson.encode(results)
`
)

func (r *RedisStateStore[TState]) Get(ctx context.Context, keys ...string) (map[string]StateEntry[TState], error) {
	cmd := r.client.Eval(ctx, getStateLuaScript, keys)

	redisResult, err := cmd.Result()
	redisResultJson := redisResult.(string)

	if err != nil {
		return nil, err
	}

	getResults := make(map[string]redisGetResult)

	err = r.marshaller.Deserialize(redisResultJson, &getResults)

	if err != nil {
		return nil, err
	}

	stateEntries := make(map[string]StateEntry[TState])

	for _, k := range keys {
		stateEntry := StateEntry[TState]{
			Key:   k,
			State: new(TState),
		}

		if getResult, ok := getResults[k]; ok {
			err = r.marshaller.Deserialize(getResult.State, stateEntry.State)

			if err != nil {
				return nil, err
			}

			stateEntry.Timestamp = &getResult.Timestamp
		}

		stateEntries[k] = stateEntry
	}

	return stateEntries, nil
}

const (
	setStateLuaScript = `
local conflicts = {}
local conflictCount = 0
for i, key in ipairs(KEYS) do
	local ix = ((i - 1) * #KEYS) + 1
    local value = ARGV[ix]
    local expectedTimestamp = tonumber(ARGV[ix + 1])
    local expire = tonumber(ARGV[ix + 2])  -- Expiration in seconds
    local currentTimestamp = tonumber(redis.call('HGET', key, 'timestamp'))

    -- Check if the timestamp has been modified. If it has, some other process has modified the state concurrently
    if currentTimestamp == nil or currentTimestamp == expectedTimestamp then
		redis.call('HMSET', key, 'value', value, 'timestamp', redis.call('TIME')[1]) -- Set the timestamp to the latest unix timestamp
		if expire > 0 then
			redis.call('EXPIRE', key, expire)  -- Set the expiration time for the key
		end
    else
		conflictCount = conflictCount + 1
		table.insert(conflicts, key)
    end
end

if conflictCount == 0 then 
	return "[]"
end

return cjson.encode(conflicts)
`
)

func (r *RedisStateStore[TState]) Set(ctx context.Context, entries map[string]StateEntry[TState]) error {
	keys := make([]string, 0, len(entries))
	args := make([]interface{}, 0, len(entries)*3)

	for _, entry := range entries {
		keys = append(keys, entry.Key)

		stateJson, _ := r.marshaller.Serialize(*entry.State)

		var timestamp int64 = -1
		if entry.Timestamp != nil {
			timestamp = *entry.Timestamp
		}

		var expiration int64 = -1
		if entry.Expiry != nil {
			expiration = int64(entry.Expiry.Seconds())
		}

		args = append(args, stateJson, timestamp, expiration)
	}

	setCmd := r.client.Eval(ctx, setStateLuaScript, keys, args...)
	setCmdResp, err := setCmd.Result()

	if err != nil {
		return err
	}

	conflicts := make([]string, 0)

	err = r.marshaller.Deserialize(setCmdResp.(string), &conflicts)

	if err != nil {
		return err
	}

	if len(conflicts) > 0 {
		return &StateStoreConflict{
			conflicts: conflicts,
		}
	}

	return nil
}

type redisGetResult struct {
	State     string
	Timestamp int64
}
