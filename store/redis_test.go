package store

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
)

func TestRedisStoreTestSuite(t *testing.T) {
	testSuite := NewStoreTestSuite(func() StateStore[string] {
		rdb := redis.NewClient(&redis.Options{
			Addr: ":6389",
		})

		return NewRedisStore[string](rdb)
	})
	suite.Run(t, testSuite)
}

func TestRedisClusterStoreTestSuite(t *testing.T) {
	testSuite := NewStoreTestSuite(func() StateStore[string] {
		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{":7021", ":7022", ":7023", ":7024", ":7025", ":7026"},
		})

		return NewRedisStore[string](rdb)
	})
	suite.Run(t, testSuite)
}
