package workflowcontracts

import (
	"fmt"

	"go.101.temporal/common/temporalx"
	temporal101client "go.101.temporal/proto/gen/go/com/temporal_101/client"
)

const HelloQueue temporalx.TaskQueue = "hello-task-queue"

var Registry = temporalx.MustRegistry(
	GreetSomeone,
	Order,
	LongRunning,
)

var protoWorkflows = map[temporal101client.WorkflowName]temporalx.Definition{
	temporal101client.WorkflowName_WORKFLOW_NAME_GREET_SOMEONE: GreetSomeone,
	temporal101client.WorkflowName_WORKFLOW_NAME_PROCESS_ORDER: Order,
	temporal101client.WorkflowName_WORKFLOW_NAME_LONG_RUNNING:  LongRunning,
}

func WorkflowFromProto(name temporal101client.WorkflowName) (temporalx.Definition, error) {
	if name == temporal101client.WorkflowName_WORKFLOW_NAME_UNSPECIFIED {
		return nil, fmt.Errorf("workflow name is required")
	}

	def, ok := protoWorkflows[name]
	if !ok {
		return nil, fmt.Errorf("unknown workflow: %v", name)
	}

	return def, nil
}

func WorkflowNameFromType(name string) temporal101client.WorkflowName {
	for protoName, def := range protoWorkflows {
		if def.Name() == name {
			return protoName
		}
	}
	return temporal101client.WorkflowName_WORKFLOW_NAME_UNSPECIFIED
}
