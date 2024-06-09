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

		// TODO:
		// 1) You're going to need to do your own form of batching here, instead of relying off a pipeline activity.
		// 2) How are you going to handle retries? How are you going to handle different types of errors?
		// 3) Does the concurrency error really need to return state entries, or should it just return keys? It would be better if it returned keys.
		// 4) Could it be possible to simply feed retried entries back into the pipeline, instead of retrying an entire batch?
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

					stateEntries, err := stateStore.Get(ctx, distinctKeys...)

					if err != nil {
						// ??
					}

					for _, upstreamItem := range upstreamItems {
						stateEntry := stateEntries[upstreamItem.Key]
						state := stateEntry.State

						if state == nil {
							state = new(TState)
						}

						state, err = stateMapper(upstreamItem.Item, *state)

						if err != nil {
							// ??
						}

						stateEntry.State = state
						stateEntries[upstreamItem.Key] = stateEntry
					}

					err = stateStore.Set(ctx, stateEntries)

					conflicts := make(map[string]StateEntry[TState])

					if conflictsErr, ok := err.(StateStoreConflict[TState]); ok {
						conflicts = conflictsErr.GetConflicts()
					} else if err != nil {
						// ??
					}

					for _, stateEntry := range stateEntries {
						if _, found := conflicts[stateEntry.Key]; found {
							downstream.Error(fmt.Errorf("State entry was modified concurrently"))
						} else {
							downstream.Write(*stateEntry.State)
						}
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

type StateStoreConflict[TState any] struct {
	conflicts map[string]StateEntry[TState]
}

func (s StateStoreConflict[TState]) Error() string {
	return "State entry was modified concurrently"
}

func (s StateStoreConflict[TState]) GetConflicts() map[string]StateEntry[TState] {
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
		table.insert(conflicts, key)
    end
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

	conflictKeys := make([]string, 0)

	err = r.marshaller.Deserialize(setCmdResp.(string), &conflictKeys)

	if err != nil {
		return err
	}

	if len(conflictKeys) > 0 {
		conflicts := make(map[string]StateEntry[TState])
		for _, key := range conflictKeys {
			conflicts[key] = entries[key]
		}

		return StateStoreConflict[TState]{conflicts: conflicts}
	}

	return nil
}

type redisGetResult struct {
	State     string
	Timestamp int64
}
