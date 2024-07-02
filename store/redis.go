package store

import (
	"context"
	"strconv"
	"time"

	"github.com/ducka/go-kayak/utils"
	"github.com/redis/go-redis/v9"
)

type RedisStore[TState any] struct {
	client     *redis.Client
	marshaller utils.Marshaller
}

func NewRedisStore[TState any](client *redis.Client) *RedisStore[TState] {
	return &RedisStore[TState]{
		client:     client,
		marshaller: utils.NewJsonMarshaller(),
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
		result.Timestamp = values[2]

		results[key] = result
	end
end

return cjson.encode(results)
`
)

func (r *RedisStore[TState]) Get(ctx context.Context, keys ...string) ([]StateEntry[TState], error) {
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

	stateEntries := make([]StateEntry[TState], 0, len(keys))

	for key, value := range getResults {
		timestamp, _ := strconv.ParseInt(value.Timestamp, 10, 64)
		stateEntry := StateEntry[TState]{
			Key:       key,
			Timestamp: utils.ToPtr(timestamp),
		}
		state := &stateEnvelope[TState]{}

		err = r.marshaller.Deserialize(value.State, state)

		if err != nil {
			return nil, err
		}

		stateEntry.State = state.V

		stateEntries = append(stateEntries, stateEntry)
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
    local expectedTimestamp = ARGV[ix + 1]
    local expire = tonumber(ARGV[ix + 2])  -- Expiration in seconds
    local currentTimestamp = redis.call('HGET', key, 'timestamp')
	local nextTimestamp = ARGV[ix + 3]

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

func (r *RedisStore[TState]) Set(ctx context.Context, entries ...StateEntry[TState]) error {
	keys := make([]string, 0, len(entries))
	args := make([]interface{}, 0, len(entries)*3)

	for _, entry := range entries {
		keys = append(keys, entry.Key)

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

		args = append(args, stateJson, currentTimestamp, expiration, nextTimestamp)
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
	Timestamp string
}

type stateEnvelope[TState any] struct {
	V *TState
}
