package workflowcontracts

import "go.101.temporal/common/temporalx"

const (
	LongRunningContinueSignal = "continue"
	LongRunningStatusQuery    = "status"
)

type LongRunningInput struct {
	Name string `json:"name"`
	// SleepSeconds wakes the workflow if no continue signal arrives.
	// 0 means wait for the signal only, so the workflow stays open.
	SleepSeconds int64 `json:"sleepSeconds"`
}

type LongRunningOutput struct {
	Name  string   `json:"name"`
	Steps []string `json:"steps"`
}

type LongRunningStatus struct {
	Step    string   `json:"step"`
	Steps   []string `json:"steps"`
	Waiting bool     `json:"waiting"`
}

type LongRunningContinue struct {
	Reason string `json:"reason"`
}

var LongRunning = temporalx.NewWorkflow[LongRunningInput, LongRunningOutput]("longrunning", HelloQueue)
