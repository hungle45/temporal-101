package temporalx

import (
	"errors"
	"fmt"
	"slices"

	"go.temporal.io/sdk/client"
	sdkinterceptor "go.temporal.io/sdk/interceptor"
	sdkworker "go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
)

type registration struct {
	apply     func(sdkworker.Worker)
	taskQueue TaskQueue
	hasQueue  bool
}

func BindWorkflow[I, O any](def Workflow[I, O], fn WorkflowFn[I, O]) registration {
	return registration{
		taskQueue: def.TaskQueue(),
		hasQueue:  true,
		apply: func(w sdkworker.Worker) {
			w.RegisterWorkflowWithOptions(fn, sdkworkflow.RegisterOptions{Name: def.Name()})
		},
	}
}

func BindActivity(fn any) registration {
	return registration{
		apply: func(w sdkworker.Worker) {
			w.RegisterActivity(fn)
		},
	}
}

type WorkerGroup struct {
	taskQueue TaskQueue
	regs      []registration
	opts      sdkworker.Options
}

func ForQueue(taskQueue TaskQueue, regs ...registration) WorkerGroup {
	return WorkerGroup{
		taskQueue: taskQueue,
		regs:      regs,
	}
}

func (g WorkerGroup) WithInterceptors(interceptors ...sdkinterceptor.WorkerInterceptor) WorkerGroup {
	g.opts.Interceptors = append(slices.Clone(g.opts.Interceptors), interceptors...)
	return g
}

func (g WorkerGroup) WithOptions(opts sdkworker.Options) WorkerGroup {
	existing := g.opts.Interceptors
	g.opts = opts
	if len(existing) > 0 {
		g.opts.Interceptors = append(slices.Clone(opts.Interceptors), existing...)
	}
	return g
}

type Manager struct {
	workers []sdkworker.Worker
}

func NewManager(c client.Client, groups ...WorkerGroup) (*Manager, error) {
	workers := make([]sdkworker.Worker, 0, len(groups))

	for _, group := range groups {
		if group.taskQueue == "" {
			return nil, errors.New("worker group: empty task queue")
		}

		for _, reg := range group.regs {
			if reg.hasQueue && reg.taskQueue != group.taskQueue {
				return nil, fmt.Errorf(
					"workflow registered on %q but worker is for %q",
					reg.taskQueue,
					group.taskQueue,
				)
			}
		}

		w := sdkworker.New(c, group.taskQueue.String(), group.opts)
		for _, reg := range group.regs {
			reg.apply(w)
		}
		workers = append(workers, w)
	}

	return &Manager{workers: workers}, nil
}

func (m *Manager) Start() error {
	started := make([]sdkworker.Worker, 0, len(m.workers))

	for _, w := range m.workers {
		if err := w.Start(); err != nil {
			stopErr := stopWorkers(started)
			return errors.Join(fmt.Errorf("start worker: %w", err), stopErr)
		}
		started = append(started, w)
	}

	return nil
}

func (m *Manager) Stop() error {
	return stopWorkers(m.workers)
}

func stopWorkers(workers []sdkworker.Worker) error {
	var errs []error

	for i := len(workers) - 1; i >= 0; i-- {
		func(w sdkworker.Worker) {
			defer func() {
				if r := recover(); r != nil {
					errs = append(errs, fmt.Errorf("panic occurred while stopping worker: %v", r))
				}
			}()
			w.Stop()
		}(workers[i])
	}

	return errors.Join(errs...)
}
