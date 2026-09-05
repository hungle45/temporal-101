package temporalx

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

func Versioned[I, O any](changeID string, m map[workflow.Version]WorkflowFn[I, O]) WorkflowFn[I, O] {
	if len(m) == 0 {
		panic("workflow versioning: empty version map")
	}

	minVersion, maxVersion := versionRange(m)

	return func(ctx workflow.Context, in I) (O, error) {
		version := workflow.GetVersion(ctx, changeID, minVersion, maxVersion)
		fn, ok := m[version]
		if !ok {
			var zero O
			return zero, fmt.Errorf("workflow versioning: unsupported version %d for change %q", version, changeID)
		}
		return fn(ctx, in)
	}
}

func versionRange[I, O any](m map[workflow.Version]WorkflowFn[I, O]) (minVersion, maxVersion workflow.Version) {
	first := true
	for version := range m {
		if first {
			minVersion, maxVersion, first = version, version, false
			continue
		}
		minVersion = min(minVersion, version)
		maxVersion = max(maxVersion, version)
	}
	return

}
