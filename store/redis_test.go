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
