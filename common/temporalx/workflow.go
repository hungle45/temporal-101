package temporalx

import (
	"go.temporal.io/sdk/workflow"
)

type TaskQueue string

func (q TaskQueue) String() string {
	return string(q)
}

// Definition is the type-erased view of a workflow used by dynamic callers
// (proto dispatch, registry lookup).
type Definition interface {
	Name() string
	TaskQueue() TaskQueue
	NewInput() any
	NewOutput() any
}

type WorkflowFn[I, O any] func(ctx workflow.Context, in I) (O, error)

// Workflow keeps input/output types so worker registration can be checked
// at compile time. It also implements Definition.
type Workflow[I, O any] struct {
	name      string
	taskQueue TaskQueue
}

func NewWorkflow[I, O any](name string, taskQueue TaskQueue) Workflow[I, O] {
	return Workflow[I, O]{
		name:      name,
		taskQueue: taskQueue,
	}
}

func (w Workflow[I, O]) Name() string {
	return w.name
}

func (w Workflow[I, O]) TaskQueue() TaskQueue {
	return w.taskQueue
}

func (w Workflow[I, O]) NewInput() any {
	return new(I)
}

func (w Workflow[I, O]) NewOutput() any {
	return new(O)
}
