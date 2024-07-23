package store

import (
	"context"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/ducka/go-kayak/utils"
	"github.com/redis/go-redis/v9"
)

var (
	redisPartitionKeyRegex = regexp.MustCompile(`{([^{}]+)}`)
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
	errCh := make(chan error)
	stateEntriesCh := make(chan StateEntry[TState], len(keys))

	done := executeRedisClusterBulkOperation(
		keys,
		func(key string) string { return key },
		func(partitionedKeys []string) {
			cmd := r.client.Eval(ctx, getStateLuaScript, partitionedKeys)

			redisResult, err := cmd.Result()

			if err != nil {
				errCh <- err
				return
			}

			redisResultJson := redisResult.(string)

			getResults := make(map[string]redisGetResult)

			err = r.marshaller.Deserialize(redisResultJson, &getResults)

			if err != nil {
				errCh <- err
				return
			}

			for key, value := range getResults {
				timestamp, _ := strconv.ParseInt(value.Timestamp, 10, 64)
				stateEntry := StateEntry[TState]{
					Key:       key,
					Timestamp: utils.ToPtr(timestamp),
				}
				state := &stateEnvelope[TState]{}

				err = r.marshaller.Deserialize(value.State, state)

				if err != nil {
					errCh <- err
					return
				}

				stateEntry.State = state.V

				stateEntriesCh <- stateEntry
			}
		},
	)

	select {
	case <-done:
		close(stateEntriesCh)
		return utils.ChanToArray(stateEntriesCh), nil
	case err := <-errCh:
		return nil, err
	}
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
	errCh := make(chan error, len(entries))

	done := executeRedisClusterBulkOperation(
		entries,
		func(entry StateEntry[TState]) string { return entry.Key },
		func(partitionedEntries []StateEntry[TState]) {
			keys := make([]string, 0, len(partitionedEntries))
			args := make([]interface{}, 0, len(partitionedEntries)*3)

			for _, entry := range partitionedEntries {
				keys = append(keys, entry.Key)

				var stateJson string = "nil"
				var err error

				if entry.State != nil {
					stateJson, err = r.marshaller.Serialize(
						&stateEnvelope[TState]{V: entry.State},
					)
				}

				if err != nil {
					errCh <- err
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
				errCh <- err
				return
			}

			conflicts := make([]string, 0)

			err = r.marshaller.Deserialize(setCmdResp.(string), &conflicts)

			if err != nil {
				errCh <- err
				return
			}

			if len(conflicts) > 0 {
				errCh <- &StateStoreConflict{
					conflicts: conflicts,
				}
				return
			}
		},
	)

	select {
	case <-done:
		return nil
	case err := <-errCh:
		return err
	}
}

func assignHashSlot(key string) uint16 {
	k := redisPartitionKeyRegex.FindStringSubmatch(key)

	if len(k) > 0 {
		key = k[1]
	}

	return Crc16(key) % 16384
}

// TODO: i think you're going to need to execute CLUSTER SHARDS to cache a map of slots for each node. Your calchlateHashSlot
// function could then be used to determine which node to send the request to.
// You would need to keep this hash map of slots up to date as redirects are encountered from redis.

// executeRedisClusterBulkOperation executes a bulk operation on a redis cluster. It does this by partitioning the supplied
// keys according to Redis's hash slot algorithm. It then executes the operation for each partition in parallel.
func executeRedisClusterBulkOperation[T any](
	itemsToPartition []T,
	partitioningKeySelector func(item T) string,
	partitionedOperation func(partitionedItems []T),
) chan struct{} {
	partitions := make(map[uint16][]T)
	done := make(chan struct{})

	for _, item := range itemsToPartition {
		partitioningKey := partitioningKeySelector(item)
		hashTag := assignHashSlot(partitioningKey)
		partitions[hashTag] = append(partitions[hashTag], item)
	}

	partitionLen := len(partitions)

	if partitionLen == 0 {
		close(done)
		return done
	} else if partitionLen == 1 {
		// If there is only one partition, there's no need to execute the operation in a go routine
		for _, v := range partitions {
			partitionedOperation(v)
		}
		close(done)
		return done
	}

	// If there are more than one partition, execute an operation for each partition in parallel
	wg := &sync.WaitGroup{}
	wg.Add(len(partitions))

	for hashTag, partitionedItems := range partitions {
		go func(hashTag uint16, partitionedItems []T) {
			defer wg.Done()
			partitionedOperation(partitionedItems)
		}(hashTag, partitionedItems)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	return done
}

type redisGetResult struct {
	State     string
	Timestamp string
}

type stateEnvelope[TState any] struct {
	V *TState
}
