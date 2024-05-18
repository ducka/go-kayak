package utils

import (
	"context"
)

func CombinedContexts(ctxs ...context.Context) context.Context {
	combinedCtx, cancel := context.WithCancel(context.Background())

	for _, ctx := range ctxs {
		go func(ctx context.Context) {
			defer cancel()
			<-ctx.Done()
		}(ctx)
	}

	return combinedCtx
}
