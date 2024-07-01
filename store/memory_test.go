package store

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestInMemoryStoreTestSuite(t *testing.T) {
	testSuite := NewStoreTestSuite(func() StateStore[string] {
		return NewInMemoryStore[string]()
	})
	suite.Run(t, testSuite)
}
