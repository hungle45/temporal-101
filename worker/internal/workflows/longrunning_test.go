package workflows

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.101.temporal/common/temporalx"
	workflowcontracts "go.101.temporal/contract/workflows"
	"go.101.temporal/worker/resources/histories"
	sdkworker "go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
)

func TestLongRunningWorkflowReplay(t *testing.T) {
	replayer := sdkworker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(LongRunning,
		sdkworkflow.RegisterOptions{Name: workflowcontracts.LongRunning.Name()})

	tests := []string{
		"longrunning-default.json",
		"longrunning-1.json",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			history, err := temporalx.LoadHistory(histories.FS, name)
			require.NoError(t, err)

			err = replayer.ReplayWorkflowHistory(slog.Default(), history)
			require.NoError(t, err)
		})
	}
}
