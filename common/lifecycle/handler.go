package lifecycle

import (
	"context"
	"errors"
	"fmt"
)

// Handler represents something that can be gracefully shut down.
type Handler interface {
	Shutdown(context.Context) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context) error

func (f HandlerFunc) Shutdown(ctx context.Context) error {
	return f(ctx)
}

type gracefulStopWithForceStop interface {
	GracefulStop()
	Stop()
}

type closeWithContext interface {
	Close(context.Context) error
}

type closeWithError interface {
	Close() error
}

type closeWithoutError interface {
	Close()
}

//type shutdownWithContext interface {
//	Shutdown(context.Context) error
//}

type shutdownWithError interface {
	Shutdown() error
}

type shutdownWithoutError interface {
	Shutdown()
}

type stopWithContext interface {
	Stop(context.Context) error
}

type stopWithError interface {
	Stop() error
}

type stopWithoutError interface {
	Stop()
}

// Adapt converts common lifecycle interfaces into a Handler.
//
// The matching priority is:
//  1. Shutdown(context.Context) error
//  2. Shutdown() error
//  3. Shutdown()
//  4. Close(context.Context) error
//  5. Close() error
//  6. Close()
//  7. GracefulStop() + Stop()
//  8. Stop(context.Context) error
//  9. Stop() error
//  10. Stop()
func Adapt(v any) (Handler, error) {
	if v == nil {
		return nil, errors.New("lifecycle: cannot adapt nil")
	}

	switch v := v.(type) {
	case Handler:
		return v, nil

	case shutdownWithError:
		return HandlerFunc(func(context.Context) error {
			return v.Shutdown()
		}), nil

	case shutdownWithoutError:
		return HandlerFunc(func(context.Context) error {
			v.Shutdown()
			return nil
		}), nil

	case closeWithContext:
		return HandlerFunc(v.Close), nil

	case closeWithError:
		return HandlerFunc(func(context.Context) error {
			return v.Close()
		}), nil

	case closeWithoutError:
		return HandlerFunc(func(context.Context) error {
			v.Close()
			return nil
		}), nil

	case gracefulStopWithForceStop:
		return HandlerFunc(func(ctx context.Context) error {
			done := make(chan struct{})
			go func() {
				defer close(done)
				v.GracefulStop()
			}()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				v.Stop()
				return ctx.Err()
			}
		}), nil

	case stopWithContext:
		return HandlerFunc(v.Stop), nil

	case stopWithError:
		return HandlerFunc(func(context.Context) error {
			return v.Stop()
		}), nil

	case stopWithoutError:
		return HandlerFunc(func(context.Context) error {
			v.Stop()
			return nil
		}), nil

	default:
		return nil, fmt.Errorf(
			"lifecycle: unsupported type %T",
			v,
		)
	}
}
