package workflowcontracts

import "go.101.temporal/common/temporalx"

type GreetSomeoneInput struct {
	Name string `json:"name"`
}

type GreetSomeoneOutput struct {
	Message string `json:"message"`
}

var GreetSomeone = temporalx.NewWorkflow[GreetSomeoneInput, GreetSomeoneOutput]("greetsomeone", HelloQueue)
