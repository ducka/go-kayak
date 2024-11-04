package kayak

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func TestExampleClient(t *testing.T) {
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7001", ":7002", ":7003", ":7004", ":7005", ":7006"},
	})

	//err := rdb.Set(ctx, "key", "value", 0).Err()
	//if err != nil {
	//	panic(err)
	//}

	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := rdb.Get(ctx, "key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
	// Output: key value
	// key2 does not exist
}
