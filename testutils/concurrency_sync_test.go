package testutils

import (
	"sync"
	"testing"
	"time"

	"github.com/ducka/go-kayak/utils"
	"github.com/stretchr/testify/assert"
)

func Test_ConcurrencySync_Checkpoint(t *testing.T) {

	for _, test := range []struct {
		name             string
		concurrencyLimit int
		spawnCount       int
		shouldTimeOut    bool
		releaseCount     int
	}{
		{
			name:             "Spawn 9 routines with a concurrency limit of 3. Should not time out.",
			concurrencyLimit: 3,
			spawnCount:       9,
			shouldTimeOut:    false,
			releaseCount:     3,
		},
		{
			name:             "Spawn 3 routines with a concurrency limit of 3. Should not time out.",
			concurrencyLimit: 3,
			spawnCount:       3,
			shouldTimeOut:    false,
			releaseCount:     1,
		},
		{
			name:             "Spawn 100 routines with a concurrency limit of 5. Should not time out.",
			concurrencyLimit: 5,
			spawnCount:       100,
			shouldTimeOut:    false,
			releaseCount:     20,
		},
		{
			name:             "Spawn 100 routines with a concurrency limit of 1. Should not time out.",
			concurrencyLimit: 1,
			spawnCount:       100,
			shouldTimeOut:    false,
			releaseCount:     100,
		},
		// Should time out because the spawn count is not divisible by the concurrency limit
		{
			name:             "Spawn 8 routines with a concurrency limit of 3. Should time out.",
			concurrencyLimit: 3,
			spawnCount:       8,
			shouldTimeOut:    true,
			releaseCount:     2,
		},
		// Should time out because the spawn count is not divisible by the concurrency limit
		{
			name:             "Spawn 2 routines with a concurrency limit of 3. Should time out.",
			concurrencyLimit: 3,
			spawnCount:       2,
			shouldTimeOut:    true,
			releaseCount:     0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sut := NewConcurrencySync(test.concurrencyLimit)

			wg := sync.WaitGroup{}
			wg.Add(test.spawnCount)

			for i := 0; i < test.spawnCount; i++ {
				go func(i int) {
					sut.Checkpoint()
					wg.Done()
				}(i)
			}

			ok := utils.WaitFor(&wg, 1*time.Second)
			assert.Equal(t, test.shouldTimeOut, !ok)
			assert.Equal(t, test.spawnCount, sut.HitCount())
			assert.Equal(t, test.releaseCount, sut.ReleaseCount())

		})
	}
}
