package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type entry struct {
	name    string
	handler Handler
}

type Manager struct {
	mu       sync.Mutex
	handlers []entry
	closed   bool
}

func New() *Manager {
	return &Manager{}
}

// Add registers a resource for shutdown.
//
// Resources are shut down in reverse registration order.
func (m *Manager) Add(v any) error {
	return m.AddNamed("", v)
}

// AddNamed is the same as Add but associates a name with the resource.
// The name is included in shutdown errors.
func (m *Manager) AddNamed(name string, v any) error {
	handler, err := Adapt(v)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return errors.New("lifecycle: manager already shut down")
	}

	m.handlers = append(m.handlers, entry{
		name:    name,
		handler: handler,
	})

	return nil
}

// AddFunc registers an arbitrary shutdown function.
func (m *Manager) AddFunc(fn func(context.Context) error) error {
	return m.Add(HandlerFunc(fn))
}

// Shutdown shuts down registered resources in reverse order.
//
// Shutdown attempts every registered handler even when one fails.
// Multiple failures are returned using errors.Join.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()

	if m.closed {
		m.mu.Unlock()
		return nil
	}

	m.closed = true

	handlers := make([]entry, len(m.handlers))
	copy(handlers, m.handlers)

	m.mu.Unlock()

	var errs []error

	for i := len(handlers) - 1; i >= 0; i-- {
		entry := handlers[i]

		if err := entry.handler.Shutdown(ctx); err != nil {
			if entry.name != "" {
				err = fmt.Errorf("%s: %w", entry.name, err)
			}

			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
