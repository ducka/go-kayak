package store

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
)

func TestRedisStoreTestSuite(t *testing.T) {
	testSuite := NewStoreTestSuite(func() StateStore[string] {
		rdb := redis.NewClient(&redis.Options{
			Addr: ":6379",
		})

		return NewRedisStore[string](rdb)
	})
	suite.Run(t, testSuite)
}

func TestRedisClusterStoreTestSuite(t *testing.T) {
	testSuite := NewStoreTestSuite(func() StateStore[string] {
		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{":7001", ":7002", ":7003", ":7004", ":7005", ":7006"},
		})

		return NewRedisStore[string](rdb)
	})
	suite.Run(t, testSuite)
}
