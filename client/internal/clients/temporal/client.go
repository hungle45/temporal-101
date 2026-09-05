package temporal

import (
	"context"

	temporal101client "go.101.temporal/proto/gen/go/com/temporal_101/client"
	sdkclient "go.temporal.io/sdk/client"
)

type Client interface {
	ExecuteWorkflow(ctx context.Context, req *temporal101client.ExecuteWorkflowRequest) (*temporal101client.ExecuteWorkflowResponse, error)
	StartWorkflow(ctx context.Context, req *temporal101client.StartWorkflowRequest) (*temporal101client.StartWorkflowResponse, error)
	SignalWorkflow(ctx context.Context, req *temporal101client.SignalWorkflowRequest) (*temporal101client.SignalWorkflowResponse, error)
	QueryWorkflow(ctx context.Context, req *temporal101client.QueryWorkflowRequest) (*temporal101client.QueryWorkflowResponse, error)
	GetWorkflow(ctx context.Context, req *temporal101client.GetWorkflowRequest) (*temporal101client.ExecuteWorkflowResponse, error)

	CreateSchedule(ctx context.Context, req *temporal101client.CreateScheduleRequest) (*temporal101client.CreateScheduleResponse, error)
	UpdateSchedule(ctx context.Context, req *temporal101client.UpdateScheduleRequest) (*temporal101client.DescribeScheduleResponse, error)
	DescribeSchedule(ctx context.Context, req *temporal101client.DescribeScheduleRequest) (*temporal101client.DescribeScheduleResponse, error)
	ListSchedules(ctx context.Context, req *temporal101client.ListSchedulesRequest) (*temporal101client.ListSchedulesResponse, error)
	TriggerSchedule(ctx context.Context, req *temporal101client.TriggerScheduleRequest) (*temporal101client.TriggerScheduleResponse, error)
	PauseSchedule(ctx context.Context, req *temporal101client.PauseScheduleRequest) (*temporal101client.PauseScheduleResponse, error)
	UnpauseSchedule(ctx context.Context, req *temporal101client.UnpauseScheduleRequest) (*temporal101client.UnpauseScheduleResponse, error)
	DeleteSchedule(ctx context.Context, req *temporal101client.DeleteScheduleRequest) (*temporal101client.DeleteScheduleResponse, error)

	Close()
}

type client struct {
	cli sdkclient.Client
}

func NewClient(cli sdkclient.Client) Client {
	return &client{
		cli: cli,
	}
}

func (c *client) ExecuteWorkflow(ctx context.Context, req *temporal101client.ExecuteWorkflowRequest) (*temporal101client.ExecuteWorkflowResponse, error) {
	def, we, err := c.startWorkflow(ctx, req.GetWorkflowName(), req.GetInput(), "")
	if err != nil {
		return nil, err
	}

	return c.awaitWorkflow(ctx, def, we)
}

func (c *client) Close() {
	c.cli.Close()
}
