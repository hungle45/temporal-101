package temporal

import (
	"context"
	"fmt"
	"time"

	"go.101.temporal/common/protox"
	workflowcontracts "go.101.temporal/contract/workflows"
	temporal101client "go.101.temporal/proto/gen/go/com/temporal_101/client"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *client) CreateSchedule(ctx context.Context, req *temporal101client.CreateScheduleRequest) (*temporal101client.CreateScheduleResponse, error) {
	if req.GetScheduleId() == "" {
		return nil, fmt.Errorf("schedule_id is required")
	}

	def, err := workflowcontracts.WorkflowFromProto(req.GetWorkflowName())
	if err != nil {
		return nil, fmt.Errorf("workflow not found: %w", err)
	}

	input := def.NewInput()
	if err := protox.FromStructPB(req.GetInput(), input); err != nil {
		return nil, fmt.Errorf("failed to unmarshal input: %w", err)
	}

	spec, err := scheduleSpecFromProto(req.GetSpec())
	if err != nil {
		return nil, err
	}

	handle, err := c.cli.ScheduleClient().Create(ctx, sdkclient.ScheduleOptions{
		ID:   req.GetScheduleId(),
		Spec: spec,
		Action: &sdkclient.ScheduleWorkflowAction{
			ID:        req.GetWorkflowId(),
			Workflow:  def.Name(),
			TaskQueue: def.TaskQueue().String(),
			Args:      []any{input},
		},
		Overlap:            overlapFromProto(req.GetOverlap()),
		Paused:             req.GetPaused(),
		TriggerImmediately: req.GetTriggerImmediately(),
		Note:               req.GetNote(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create schedule: %w", err)
	}

	return &temporal101client.CreateScheduleResponse{
		ScheduleId: handle.GetID(),
	}, nil
}

func (c *client) UpdateSchedule(ctx context.Context, req *temporal101client.UpdateScheduleRequest) (*temporal101client.DescribeScheduleResponse, error) {
	if req.GetScheduleId() == "" {
		return nil, fmt.Errorf("schedule_id is required")
	}
	if !hasScheduleUpdate(req) {
		return nil, fmt.Errorf("update requires at least one field")
	}

	handle := c.cli.ScheduleClient().GetHandle(ctx, req.GetScheduleId())
	err := handle.Update(ctx, sdkclient.ScheduleUpdateOptions{
		DoUpdate: func(input sdkclient.ScheduleUpdateInput) (*sdkclient.ScheduleUpdate, error) {
			schedule := input.Description.Schedule
			if err := applyScheduleUpdate(&schedule, req); err != nil {
				return nil, err
			}
			return &sdkclient.ScheduleUpdate{Schedule: &schedule}, nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update schedule: %w", err)
	}

	return c.DescribeSchedule(ctx, &temporal101client.DescribeScheduleRequest{
		ScheduleId: req.GetScheduleId(),
	})
}

func (c *client) DescribeSchedule(ctx context.Context, req *temporal101client.DescribeScheduleRequest) (*temporal101client.DescribeScheduleResponse, error) {
	if req.GetScheduleId() == "" {
		return nil, fmt.Errorf("schedule_id is required")
	}

	desc, err := c.cli.ScheduleClient().GetHandle(ctx, req.GetScheduleId()).Describe(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to describe schedule: %w", err)
	}

	workflowType := workflowTypeFromAction(desc.Schedule.Action)

	resp := &temporal101client.DescribeScheduleResponse{
		ScheduleId:      req.GetScheduleId(),
		WorkflowName:    workflowcontracts.WorkflowNameFromType(workflowType),
		WorkflowType:    workflowType,
		Spec:            scheduleSpecToProto(desc.Schedule.Spec),
		NextActionTimes: timestampsToProto(desc.Info.NextActionTimes),
		RecentActions:   actionsToProto(desc.Info.RecentActions),
		NumActions:      int32(desc.Info.NumActions),
	}

	if desc.Schedule.State != nil {
		resp.Paused = desc.Schedule.State.Paused
		resp.Note = desc.Schedule.State.Note
	}

	for _, running := range desc.Info.RunningWorkflows {
		resp.RunningWorkflowIds = append(resp.RunningWorkflowIds, running.WorkflowID)
	}

	return resp, nil
}

func (c *client) ListSchedules(ctx context.Context, _ *temporal101client.ListSchedulesRequest) (*temporal101client.ListSchedulesResponse, error) {
	iter, err := c.cli.ScheduleClient().List(ctx, sdkclient.ScheduleListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}

	resp := &temporal101client.ListSchedulesResponse{}
	for iter.HasNext() {
		item, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("failed to list schedules: %w", err)
		}

		resp.Schedules = append(resp.Schedules, &temporal101client.ScheduleListItem{
			ScheduleId:      item.ID,
			WorkflowName:    workflowcontracts.WorkflowNameFromType(item.WorkflowType.Name),
			WorkflowType:    item.WorkflowType.Name,
			Paused:          item.Paused,
			NextActionTimes: timestampsToProto(item.NextActionTimes),
		})
	}

	return resp, nil
}

func (c *client) TriggerSchedule(ctx context.Context, req *temporal101client.TriggerScheduleRequest) (*temporal101client.TriggerScheduleResponse, error) {
	if req.GetScheduleId() == "" {
		return nil, fmt.Errorf("schedule_id is required")
	}

	err := c.cli.ScheduleClient().GetHandle(ctx, req.GetScheduleId()).Trigger(ctx, sdkclient.ScheduleTriggerOptions{
		Overlap: overlapFromProto(req.GetOverlap()),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to trigger schedule: %w", err)
	}

	return &temporal101client.TriggerScheduleResponse{}, nil
}

func (c *client) PauseSchedule(ctx context.Context, req *temporal101client.PauseScheduleRequest) (*temporal101client.PauseScheduleResponse, error) {
	if req.GetScheduleId() == "" {
		return nil, fmt.Errorf("schedule_id is required")
	}

	err := c.cli.ScheduleClient().GetHandle(ctx, req.GetScheduleId()).Pause(ctx, sdkclient.SchedulePauseOptions{
		Note: req.GetNote(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to pause schedule: %w", err)
	}

	return &temporal101client.PauseScheduleResponse{}, nil
}

func (c *client) UnpauseSchedule(ctx context.Context, req *temporal101client.UnpauseScheduleRequest) (*temporal101client.UnpauseScheduleResponse, error) {
	if req.GetScheduleId() == "" {
		return nil, fmt.Errorf("schedule_id is required")
	}

	err := c.cli.ScheduleClient().GetHandle(ctx, req.GetScheduleId()).Unpause(ctx, sdkclient.ScheduleUnpauseOptions{
		Note: req.GetNote(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to unpause schedule: %w", err)
	}

	return &temporal101client.UnpauseScheduleResponse{}, nil
}

func (c *client) DeleteSchedule(ctx context.Context, req *temporal101client.DeleteScheduleRequest) (*temporal101client.DeleteScheduleResponse, error) {
	if req.GetScheduleId() == "" {
		return nil, fmt.Errorf("schedule_id is required")
	}

	err := c.cli.ScheduleClient().GetHandle(ctx, req.GetScheduleId()).Delete(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to delete schedule: %w", err)
	}

	return &temporal101client.DeleteScheduleResponse{}, nil
}

func hasScheduleUpdate(req *temporal101client.UpdateScheduleRequest) bool {
	return req.GetWorkflowName() != temporal101client.WorkflowName_WORKFLOW_NAME_UNSPECIFIED ||
		req.GetInput() != nil ||
		req.GetSpec() != nil ||
		req.GetOverlap() != temporal101client.ScheduleOverlapPolicy_SCHEDULE_OVERLAP_POLICY_UNSPECIFIED ||
		req.Paused != nil ||
		req.Note != nil ||
		req.WorkflowId != nil
}

func applyScheduleUpdate(schedule *sdkclient.Schedule, req *temporal101client.UpdateScheduleRequest) error {
	if req.GetSpec() != nil {
		spec, err := scheduleSpecFromProto(req.GetSpec())
		if err != nil {
			return err
		}
		schedule.Spec = &spec
	}

	if req.GetOverlap() != temporal101client.ScheduleOverlapPolicy_SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		if schedule.Policy == nil {
			schedule.Policy = &sdkclient.SchedulePolicies{}
		}
		schedule.Policy.Overlap = overlapFromProto(req.GetOverlap())
	}

	if req.Paused != nil || req.Note != nil {
		if schedule.State == nil {
			schedule.State = &sdkclient.ScheduleState{}
		}
		if req.Paused != nil {
			schedule.State.Paused = req.GetPaused()
		}
		if req.Note != nil {
			schedule.State.Note = req.GetNote()
		}
	}

	return applyActionUpdate(schedule, req)
}

func applyActionUpdate(schedule *sdkclient.Schedule, req *temporal101client.UpdateScheduleRequest) error {
	current, _ := schedule.Action.(*sdkclient.ScheduleWorkflowAction)

	if req.GetWorkflowName() != temporal101client.WorkflowName_WORKFLOW_NAME_UNSPECIFIED {
		def, err := workflowcontracts.WorkflowFromProto(req.GetWorkflowName())
		if err != nil {
			return fmt.Errorf("workflow not found: %w", err)
		}

		input := def.NewInput()
		if err := protox.FromStructPB(req.GetInput(), input); err != nil {
			return fmt.Errorf("failed to unmarshal input: %w", err)
		}

		id := ""
		if current != nil {
			id = current.ID
		}
		if req.WorkflowId != nil {
			id = req.GetWorkflowId()
		}

		schedule.Action = &sdkclient.ScheduleWorkflowAction{
			ID:        id,
			Workflow:  def.Name(),
			TaskQueue: def.TaskQueue().String(),
			Args:      []any{input},
		}
		return nil
	}

	if req.GetInput() == nil && req.WorkflowId == nil {
		return nil
	}
	if current == nil {
		return fmt.Errorf("schedule has no workflow action")
	}

	if req.GetInput() != nil {
		name, _ := current.Workflow.(string)
		def, ok := workflowcontracts.Registry.Resolve(name)
		if !ok {
			return fmt.Errorf("unknown workflow type: %s", name)
		}

		input := def.NewInput()
		if err := protox.FromStructPB(req.GetInput(), input); err != nil {
			return fmt.Errorf("failed to unmarshal input: %w", err)
		}
		current.Args = []any{input}
	}

	if req.WorkflowId != nil {
		current.ID = req.GetWorkflowId()
	}

	schedule.Action = current
	return nil
}

func scheduleSpecFromProto(spec *temporal101client.ScheduleSpec) (sdkclient.ScheduleSpec, error) {
	if spec == nil {
		return sdkclient.ScheduleSpec{}, fmt.Errorf("spec is required")
	}

	out := sdkclient.ScheduleSpec{
		CronExpressions: spec.GetCronExpressions(),
		TimeZoneName:    spec.GetTimeZone(),
	}

	if interval := spec.GetInterval(); interval != nil {
		if d := interval.AsDuration(); d > 0 {
			out.Intervals = []sdkclient.ScheduleIntervalSpec{{Every: d}}
		}
	}

	if len(out.CronExpressions) == 0 && len(out.Intervals) == 0 {
		return sdkclient.ScheduleSpec{}, fmt.Errorf("spec requires cron_expressions or interval")
	}

	return out, nil
}

func scheduleSpecToProto(spec *sdkclient.ScheduleSpec) *temporal101client.ScheduleSpec {
	if spec == nil {
		return nil
	}

	out := &temporal101client.ScheduleSpec{
		CronExpressions: spec.CronExpressions,
		TimeZone:        spec.TimeZoneName,
	}
	if len(spec.Intervals) > 0 {
		out.Interval = durationpb.New(spec.Intervals[0].Every)
	}
	return out
}

func overlapFromProto(policy temporal101client.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	switch policy {
	case temporal101client.ScheduleOverlapPolicy_SCHEDULE_OVERLAP_POLICY_SKIP:
		return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	case temporal101client.ScheduleOverlapPolicy_SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
		return enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE
	case temporal101client.ScheduleOverlapPolicy_SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
		return enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	case temporal101client.ScheduleOverlapPolicy_SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
		return enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER
	case temporal101client.ScheduleOverlapPolicy_SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		return enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
	case temporal101client.ScheduleOverlapPolicy_SCHEDULE_OVERLAP_POLICY_ALLOW_ALL:
		return enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	default:
		return enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED
	}
}

func workflowTypeFromAction(action sdkclient.ScheduleAction) string {
	wf, ok := action.(*sdkclient.ScheduleWorkflowAction)
	if !ok {
		return ""
	}
	name, _ := wf.Workflow.(string)
	return name
}

func timestampsToProto(times []time.Time) []*timestamppb.Timestamp {
	out := make([]*timestamppb.Timestamp, 0, len(times))
	for _, t := range times {
		if t.IsZero() {
			continue
		}
		out = append(out, timestamppb.New(t))
	}
	return out
}

func actionsToProto(actions []sdkclient.ScheduleActionResult) []*temporal101client.ScheduleAction {
	out := make([]*temporal101client.ScheduleAction, 0, len(actions))
	for _, action := range actions {
		item := &temporal101client.ScheduleAction{}
		if !action.ScheduleTime.IsZero() {
			item.ScheduledAt = timestamppb.New(action.ScheduleTime)
		}
		if !action.ActualTime.IsZero() {
			item.StartedAt = timestamppb.New(action.ActualTime)
		}
		if action.StartWorkflowResult != nil {
			item.WorkflowId = action.StartWorkflowResult.WorkflowID
			item.RunId = action.StartWorkflowResult.FirstExecutionRunID
		}
		out = append(out, item)
	}
	return out
}
