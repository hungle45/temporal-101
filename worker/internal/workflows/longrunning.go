package workflows

import (
	"context"
	"fmt"
	"time"

	"go.101.temporal/common/temporalx"
	workflowcontracts "go.101.temporal/contract/workflows"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var LongRunning = temporalx.Versioned("longrunning",
	map[workflow.Version]temporalx.WorkflowFn[workflowcontracts.LongRunningInput, workflowcontracts.LongRunningOutput]{
		workflow.DefaultVersion: LongRunningV1,
		1:                       LongRunningV2,
	},
)

func LongRunningV1(ctx workflow.Context, input workflowcontracts.LongRunningInput) (workflowcontracts.LongRunningOutput, error) {
	status := workflowcontracts.LongRunningStatus{Step: "started"}

	if err := workflow.SetQueryHandler(ctx, workflowcontracts.LongRunningStatusQuery, func() (workflowcontracts.LongRunningStatus, error) {
		return status, nil
	}); err != nil {
		return workflowcontracts.LongRunningOutput{}, err
	}

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	var prepared string
	if err := workflow.ExecuteActivity(activityCtx, PrepareLongRunning, input.Name).Get(ctx, &prepared); err != nil {
		return workflowcontracts.LongRunningOutput{}, err
	}
	status.Steps = append(status.Steps, prepared)
	status.Step = "waiting"
	status.Waiting = true

	// Stay open here so a new worker build can be deployed before step two.
	// Later, wrap FinishLongRunning in workflow.GetVersion when that step changes.
	if err := waitToContinue(ctx, input.SleepSeconds); err != nil {
		return workflowcontracts.LongRunningOutput{}, err
	}

	status.Waiting = false
	status.Step = "finishing"

	var finished string
	if err := workflow.ExecuteActivity(activityCtx, FinishLongRunning, input.Name).Get(ctx, &finished); err != nil {
		return workflowcontracts.LongRunningOutput{}, err
	}
	status.Steps = append(status.Steps, finished)
	status.Step = "completed"

	return workflowcontracts.LongRunningOutput{
		Name:  input.Name,
		Steps: status.Steps,
	}, nil
}

func LongRunningV2(ctx workflow.Context, input workflowcontracts.LongRunningInput) (workflowcontracts.LongRunningOutput, error) {
	status := workflowcontracts.LongRunningStatus{Step: "started"}

	if err := workflow.SetQueryHandler(ctx, workflowcontracts.LongRunningStatusQuery, func() (workflowcontracts.LongRunningStatus, error) {
		return status, nil
	}); err != nil {
		return workflowcontracts.LongRunningOutput{}, err
	}

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	var prepared string
	if err := workflow.ExecuteActivity(activityCtx, PrepareLongRunningV2, input.Name).Get(ctx, &prepared); err != nil {
		return workflowcontracts.LongRunningOutput{}, err
	}
	status.Steps = append(status.Steps, prepared)
	status.Step = "waiting"
	status.Waiting = true

	// Stay open here so a new worker build can be deployed before step two.
	// Later, wrap FinishLongRunning in workflow.GetVersion when that step changes.
	if err := waitToContinue(ctx, input.SleepSeconds); err != nil {
		return workflowcontracts.LongRunningOutput{}, err
	}

	status.Waiting = false
	status.Step = "finishing"

	var finished string
	if err := workflow.ExecuteActivity(activityCtx, FinishLongRunningV2, input.Name).Get(ctx, &finished); err != nil {
		return workflowcontracts.LongRunningOutput{}, err
	}
	status.Steps = append(status.Steps, finished)
	status.Step = "completed"

	return workflowcontracts.LongRunningOutput{
		Name:  input.Name,
		Steps: status.Steps,
	}, nil
}

func waitToContinue(ctx workflow.Context, sleepSeconds int64) error {
	signal := workflow.GetSignalChannel(ctx, workflowcontracts.LongRunningContinueSignal)
	selector := workflow.NewSelector(ctx)

	selector.AddReceive(signal, func(c workflow.ReceiveChannel, more bool) {
		var payload workflowcontracts.LongRunningContinue
		c.Receive(ctx, &payload)
	})

	if sleepSeconds > 0 {
		selector.AddFuture(workflow.NewTimer(ctx, time.Duration(sleepSeconds)*time.Second), func(workflow.Future) {})
	}

	selector.Select(ctx)
	return ctx.Err()
}

func PrepareLongRunning(ctx context.Context, name string) (string, error) {
	return fmt.Sprintf("prepared:%s", name), nil
}

func FinishLongRunning(ctx context.Context, name string) (string, error) {
	return fmt.Sprintf("finished:%s", name), nil
}

func PrepareLongRunningV2(ctx context.Context, name string) (string, error) {
	return fmt.Sprintf("prepared:v2:%s", name), nil
}

func FinishLongRunningV2(ctx context.Context, name string) (string, error) {
	return fmt.Sprintf("finished:v2:%s", name), nil
}
