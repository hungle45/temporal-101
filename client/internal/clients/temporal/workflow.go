package temporal

import (
	"context"
	"fmt"

	"go.101.temporal/common/protox"
	"go.101.temporal/common/temporalx"
	workflowcontracts "go.101.temporal/contract/workflows"
	temporal101client "go.101.temporal/proto/gen/go/com/temporal_101/client"
	sdkclient "go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/structpb"
)

func (c *client) StartWorkflow(ctx context.Context, req *temporal101client.StartWorkflowRequest) (*temporal101client.StartWorkflowResponse, error) {
	_, we, err := c.startWorkflow(ctx, req.GetWorkflowName(), req.GetInput(), req.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	return &temporal101client.StartWorkflowResponse{
		WorkflowId: we.GetID(),
		RunId:      we.GetRunID(),
	}, nil
}

func (c *client) SignalWorkflow(ctx context.Context, req *temporal101client.SignalWorkflowRequest) (*temporal101client.SignalWorkflowResponse, error) {
	if req.GetWorkflowId() == "" {
		return nil, fmt.Errorf("workflow_id is required")
	}
	if req.GetSignalName() == "" {
		return nil, fmt.Errorf("signal_name is required")
	}

	payload, err := structPayload(req.GetPayload())
	if err != nil {
		return nil, err
	}

	if err := c.cli.SignalWorkflow(ctx, req.GetWorkflowId(), req.GetRunId(), req.GetSignalName(), payload); err != nil {
		return nil, fmt.Errorf("failed to signal workflow: %w", err)
	}

	return &temporal101client.SignalWorkflowResponse{}, nil
}

func (c *client) QueryWorkflow(ctx context.Context, req *temporal101client.QueryWorkflowRequest) (*temporal101client.QueryWorkflowResponse, error) {
	if req.GetWorkflowId() == "" {
		return nil, fmt.Errorf("workflow_id is required")
	}
	if req.GetQueryName() == "" {
		return nil, fmt.Errorf("query_name is required")
	}

	result, err := c.cli.QueryWorkflow(ctx, req.GetWorkflowId(), req.GetRunId(), req.GetQueryName())
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow: %w", err)
	}

	decoded := queryResult(req.GetQueryName())
	if err := result.Get(decoded); err != nil {
		return nil, fmt.Errorf("failed to decode query result: %w", err)
	}

	out, err := protox.ToStructPB(decoded)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query result: %w", err)
	}

	return &temporal101client.QueryWorkflowResponse{Result: out}, nil
}

func (c *client) GetWorkflow(ctx context.Context, req *temporal101client.GetWorkflowRequest) (*temporal101client.ExecuteWorkflowResponse, error) {
	if req.GetWorkflowId() == "" {
		return nil, fmt.Errorf("workflow_id is required")
	}

	def, err := workflowcontracts.WorkflowFromProto(req.GetWorkflowName())
	if err != nil {
		return nil, fmt.Errorf("workflow not found: %w", err)
	}

	return c.awaitWorkflow(ctx, def, c.cli.GetWorkflow(ctx, req.GetWorkflowId(), req.GetRunId()))
}

func (c *client) startWorkflow(ctx context.Context, name temporal101client.WorkflowName, inputPB *structpb.Struct, workflowID string) (temporalx.Definition, sdkclient.WorkflowRun, error) {
	def, err := workflowcontracts.WorkflowFromProto(name)
	if err != nil {
		return nil, nil, fmt.Errorf("workflow not found: %w", err)
	}

	input := def.NewInput()
	if err := protox.FromStructPB(inputPB, input); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal input: %w", err)
	}

	we, err := c.cli.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: def.TaskQueue().String(),
	}, def.Name(), input)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute workflow: %w", err)
	}

	return def, we, nil
}

func (c *client) awaitWorkflow(ctx context.Context, def temporalx.Definition, we sdkclient.WorkflowRun) (*temporal101client.ExecuteWorkflowResponse, error) {
	output := def.NewOutput()
	if err := we.Get(ctx, output); err != nil {
		return nil, fmt.Errorf("failed to get output: %w", err)
	}

	outputPB, err := protox.ToStructPB(output)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return &temporal101client.ExecuteWorkflowResponse{
		WorkflowId: we.GetID(),
		RunId:      we.GetRunID(),
		Output:     outputPB,
	}, nil
}

func structPayload(s *structpb.Struct) (any, error) {
	if s == nil {
		return nil, nil
	}

	raw := map[string]any{}
	if err := protox.FromStructPB(s, &raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return raw, nil
}

func queryResult(name string) any {
	if name == workflowcontracts.LongRunningStatusQuery {
		return new(workflowcontracts.LongRunningStatus)
	}
	return new(map[string]any)
}
