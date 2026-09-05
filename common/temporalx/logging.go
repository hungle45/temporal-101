package temporalx

import (
	"context"
	"log/slog"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/workflow"
)

// LoggingInterceptor can be used by both Temporal clients and workers.
type LoggingInterceptor struct {
	interceptor.InterceptorBase

	logger *slog.Logger
}

func NewLoggingInterceptor(logger *slog.Logger) *LoggingInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingInterceptor{logger: logger}
}

func (i *LoggingInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	return &clientLoggingInterceptor{
		ClientOutboundInterceptorBase: interceptor.ClientOutboundInterceptorBase{Next: next},
		logger:                        i.logger,
	}
}

type clientLoggingInterceptor struct {
	interceptor.ClientOutboundInterceptorBase
	logger *slog.Logger
}

func (i *clientLoggingInterceptor) ExecuteWorkflow(ctx context.Context, in *interceptor.ClientExecuteWorkflowInput) (client.WorkflowRun, error) {
	start := time.Now()

	i.logger.InfoContext(ctx, "workflow starting",
		"workflow", in.WorkflowType,
		"workflow_id", in.Options.ID,
		"task_queue", in.Options.TaskQueue,
	)

	run, err := i.Next.ExecuteWorkflow(ctx, in)
	if err != nil {
		i.logger.ErrorContext(ctx, "workflow start failed",
			"workflow", in.WorkflowType,
			"workflow_id", in.Options.ID,
			"task_queue", in.Options.TaskQueue,
			"duration", time.Since(start),
			"error", err,
		)
		return nil, err
	}

	i.logger.InfoContext(ctx, "workflow started",
		"workflow", in.WorkflowType,
		"workflow_id", run.GetID(),
		"run_id", run.GetRunID(),
		"task_queue", in.Options.TaskQueue,
		"duration", time.Since(start),
	)

	return run, nil
}

func (i *LoggingInterceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &workflowLoggingInterceptor{
		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
	}
}

type workflowLoggingInterceptor struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (i *workflowLoggingInterceptor) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (any, error) {
	info := workflow.GetInfo(ctx)
	logger := workflow.GetLogger(ctx)
	attrs := []any{
		"workflow", info.WorkflowType.Name,
		"workflow_id", info.WorkflowExecution.ID,
		"run_id", info.WorkflowExecution.RunID,
		"task_queue", info.TaskQueueName,
		"attempt", info.Attempt,
	}

	// Workflow code can replay. Skip logs during replay and do not use time.Now().
	if !workflow.IsReplaying(ctx) {
		logger.Info("workflow executing", attrs...)
	}

	result, err := i.Next.ExecuteWorkflow(ctx, in)

	if !workflow.IsReplaying(ctx) {
		if err != nil {
			logger.Error("workflow failed", append(attrs, "error", err)...)
		} else {
			logger.Info("workflow completed", attrs...)
		}
	}

	return result, err
}

func (i *LoggingInterceptor) InterceptActivity(ctx context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &activityLoggingInterceptor{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
	}
}

type activityLoggingInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
}

func (i *activityLoggingInterceptor) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	info := activity.GetInfo(ctx)
	logger := activity.GetLogger(ctx)
	start := time.Now()
	attrs := []any{
		"activity", info.ActivityType.Name,
		"activity_id", info.ActivityID,
		"workflow_id", info.WorkflowExecution.ID,
		"run_id", info.WorkflowExecution.RunID,
		"task_queue", info.TaskQueue,
		"attempt", info.Attempt,
	}

	logger.Info("activity executing", attrs...)

	result, err := i.Next.ExecuteActivity(ctx, in)
	attrs = append(attrs, "duration", time.Since(start))

	if err != nil {
		logger.Error("activity failed", append(attrs, "error", err)...)
		return nil, err
	}

	logger.Info("activity completed", attrs...)
	return result, nil
}
