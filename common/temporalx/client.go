package temporalx

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
)

func Dial(ctx context.Context, opts client.Options) (client.Client, error) {
	c, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("temporal client failed to connect: %w", err)
	}

	if _, err := c.CheckHealth(ctx, nil); err != nil {
		c.Close()
		return nil, fmt.Errorf("temporal client health check failed: %w", err)
	}

	return c, nil
}
