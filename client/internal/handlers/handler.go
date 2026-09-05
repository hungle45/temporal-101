package handlers

import (
	"context"

	"go.101.temporal/client/internal/clients/temporal"
	"go.101.temporal/proto/gen/go/com/temporal_101/client"
)

type handler struct {
	temporal101client.UnimplementedClientServiceServer
	temporalClient temporal.Client
}

func NewHandler(temporalClient temporal.Client) temporal101client.ClientServiceServer {
	return &handler{
		temporalClient: temporalClient,
	}
}

func (h *handler) ExecuteWorkflow(ctx context.Context, req *temporal101client.ExecuteWorkflowRequest) (*temporal101client.ExecuteWorkflowResponse, error) {
	return h.temporalClient.ExecuteWorkflow(ctx, req)
}

func (h *handler) StartWorkflow(ctx context.Context, req *temporal101client.StartWorkflowRequest) (*temporal101client.StartWorkflowResponse, error) {
	return h.temporalClient.StartWorkflow(ctx, req)
}

func (h *handler) SignalWorkflow(ctx context.Context, req *temporal101client.SignalWorkflowRequest) (*temporal101client.SignalWorkflowResponse, error) {
	return h.temporalClient.SignalWorkflow(ctx, req)
}

func (h *handler) QueryWorkflow(ctx context.Context, req *temporal101client.QueryWorkflowRequest) (*temporal101client.QueryWorkflowResponse, error) {
	return h.temporalClient.QueryWorkflow(ctx, req)
}

func (h *handler) GetWorkflow(ctx context.Context, req *temporal101client.GetWorkflowRequest) (*temporal101client.ExecuteWorkflowResponse, error) {
	return h.temporalClient.GetWorkflow(ctx, req)
}

func (h *handler) CreateSchedule(ctx context.Context, req *temporal101client.CreateScheduleRequest) (*temporal101client.CreateScheduleResponse, error) {
	return h.temporalClient.CreateSchedule(ctx, req)
}

func (h *handler) UpdateSchedule(ctx context.Context, req *temporal101client.UpdateScheduleRequest) (*temporal101client.DescribeScheduleResponse, error) {
	return h.temporalClient.UpdateSchedule(ctx, req)
}

func (h *handler) DescribeSchedule(ctx context.Context, req *temporal101client.DescribeScheduleRequest) (*temporal101client.DescribeScheduleResponse, error) {
	return h.temporalClient.DescribeSchedule(ctx, req)
}

func (h *handler) ListSchedules(ctx context.Context, req *temporal101client.ListSchedulesRequest) (*temporal101client.ListSchedulesResponse, error) {
	return h.temporalClient.ListSchedules(ctx, req)
}

func (h *handler) TriggerSchedule(ctx context.Context, req *temporal101client.TriggerScheduleRequest) (*temporal101client.TriggerScheduleResponse, error) {
	return h.temporalClient.TriggerSchedule(ctx, req)
}

func (h *handler) PauseSchedule(ctx context.Context, req *temporal101client.PauseScheduleRequest) (*temporal101client.PauseScheduleResponse, error) {
	return h.temporalClient.PauseSchedule(ctx, req)
}

func (h *handler) UnpauseSchedule(ctx context.Context, req *temporal101client.UnpauseScheduleRequest) (*temporal101client.UnpauseScheduleResponse, error) {
	return h.temporalClient.UnpauseSchedule(ctx, req)
}

func (h *handler) DeleteSchedule(ctx context.Context, req *temporal101client.DeleteScheduleRequest) (*temporal101client.DeleteScheduleResponse, error) {
	return h.temporalClient.DeleteSchedule(ctx, req)
}
