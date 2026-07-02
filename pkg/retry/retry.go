package retry

import (
	"context"
	"time"
)

func Do(ctx context.Context, attempts int, baseDelay time.Duration, fn func() error) error {
	var lastErr error
	for i := 0; i < attempts; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		if i == attempts-1 {
			break
		}

		select {
		case <-time.After(baseDelay * time.Duration(1<<i)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return lastErr
}
