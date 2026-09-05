package workflows

import (
	workflowcontracts "go.101.temporal/contract/workflows"
	"go.temporal.io/sdk/workflow"
)

func GreetSomeone(ctx workflow.Context, input workflowcontracts.GreetSomeoneInput) (workflowcontracts.GreetSomeoneOutput, error) {
	return workflowcontracts.GreetSomeoneOutput{Message: "Hello " + input.Name}, nil
}
