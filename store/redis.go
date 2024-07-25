package store

import (
	"context"
	"strconv"
	"time"

	"github.com/ducka/go-kayak/utils"
	"github.com/redis/go-redis/v9"
)

type RedisStore[TState any] struct {
	client     redis.UniversalClient
	marshaller utils.Marshaller
}

func NewRedisStore[TState any](client redis.UniversalClient) *RedisStore[TState] {
	if client == nil {
		panic("client should not be nil")
	}

	return &RedisStore[TState]{
		client:     client,
		marshaller: utils.NewJsonMarshaller(),
	}
}

func (r *RedisStore[TState]) Get(ctx context.Context, keys ...string) ([]StateEntry[TState], error) {
	pipe := r.client.Pipeline()

	cmds := make([]*redis.SliceCmd, 0, len(keys))

	for _, key := range keys {
		cmd := pipe.HMGet(ctx, key, "value", "timestamp")

		cmds = append(cmds, cmd)
	}

	_, err := pipe.Exec(ctx)

	if err != nil {
		return nil, err
	}

	results := make([]StateEntry[TState], 0, len(cmds))

	for _, cmd := range cmds {
		values, err := cmd.Result()

		if err != nil {
			return nil, err
		}

		args := cmd.Args()
		var timestamp *int64
		var state stateEnvelope[TState]

		if values[0] == nil {
			return nil, err
		}

		if str, ok := values[0].(string); ok && str != "" {
			r.marshaller.Deserialize(str, &state)
		}

		if str, ok := values[1].(string); ok && str != "" {
			i, err := strconv.ParseInt(str, 10, 64)

			if err != nil {
				return nil, err
			}

			timestamp = &i
		}

		stateEntry := StateEntry[TState]{
			Key:       args[1].(string),
			Timestamp: timestamp,
		}

		if state.V != nil {
			stateEntry.State = state.V
		}

		results = append(results, stateEntry)
	}

	return results, nil
}

const (
	setStateLuaScript = `
local key = KEYS[1]
local value = ARGV[1]
local expectedTimestamp = ARGV[2]
local expire = tonumber(ARGV[3])  -- Expiration in seconds
local currentTimestamp = redis.call('HGET', key, 'timestamp')
local nextTimestamp = ARGV[4]

-- Check if the timestamp has been modified. If it has, some other process has modified the state concurrently
if not currentTimestamp or currentTimestamp == expectedTimestamp then
	if value == "nil" then
		redis.call('DEL', key)
	else
		redis.call('HMSET', key, 'value', value, 'timestamp', nextTimestamp)
		if expire > 0 then
			redis.call('EXPIRE', key, expire)  -- Set the expiration time for the key
		end
	end
	return "ok"
else
	return "conflict"
end

`
)

func (r *RedisStore[TState]) Set(ctx context.Context, entries ...StateEntry[TState]) error {
	pipe := r.client.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(entries))

	for _, entry := range entries {
		var stateJson string = "nil"
		var err error

		if entry.State != nil {
			stateJson, err = r.marshaller.Serialize(
				&stateEnvelope[TState]{V: entry.State},
			)
		}

		if err != nil {
			return err
		}

		var currentTimestamp int64 = -1
		if entry.Timestamp != nil {
			currentTimestamp = *entry.Timestamp
		}

		var expiration int64 = -1
		if entry.Expiry != nil {
			expiration = int64(entry.Expiry.Seconds())
		}

		nextTimestamp := time.Now().UnixNano()

		cmd := pipe.Eval(ctx, setStateLuaScript, []string{entry.Key}, stateJson, currentTimestamp, expiration, nextTimestamp)

		cmds = append(cmds, cmd)
	}

	_, err := pipe.Exec(ctx)

	if err != nil {
		return err
	}

	conflicts := make([]string, 0)

	for _, cmd := range cmds {
		resp, err := cmd.Result()

		if err != nil {
			return err
		}

		args := cmd.Args()
		keys := args[3].(string)

		var result string
		if str, ok := resp.(string); ok {
			result = str
		}

		if result == "conflict" {
			conflicts = append(conflicts, keys)
		}
	}

	if len(conflicts) > 0 {
		return &StateStoreConflict{conflicts: conflicts}
	}

	return nil
}

type stateEnvelope[TState any] struct {
	V *TState
}
