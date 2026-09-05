package workflows

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	workflowcontracts "go.101.temporal/contract/workflows"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ----------
// Workflow
// ----------

func Order(ctx workflow.Context, input workflowcontracts.OrderInput) (workflowcontracts.OrderOutput, error) {
	// 1. Get the current Workflow time.
	// workflow.Now(ctx) is deterministic.
	createdAt := workflow.Now(ctx)

	// 2. Configure Activity options.
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}

	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// 3. Generate an order ID using an Activity.
	// Randomness should not use math/rand directly in Workflow code.
	var orderID string
	err := workflow.ExecuteActivity(ctx, GenerateOrderID).Get(ctx, &orderID)
	if err != nil {
		return workflowcontracts.OrderOutput{}, err
	}

	// 4. Loop through all items.
	for _, item := range input.Items {
		var result string
		err := workflow.ExecuteActivity(ctx, ProcessItem, input.Customer, item).Get(ctx, &result)
		if err != nil {
			return workflowcontracts.OrderOutput{}, err
		}
	}

	// 5. Another Activity using the Workflow's deterministic time.
	err = workflow.ExecuteActivity(ctx, SaveOrder, orderID, input.Customer, createdAt).Get(ctx, nil)
	if err != nil {
		return workflowcontracts.OrderOutput{}, err
	}

	return workflowcontracts.OrderOutput{
		OrderID:   orderID,
		CreatedAt: createdAt,
	}, nil
}

// ----------
// Activities
// ----------

func GenerateOrderID(ctx context.Context) (string, error) {
	// Activities can use normal Go randomness.
	n := rand.Intn(1_000_000)

	return fmt.Sprintf("ORD-%06d", n), nil
}

func ProcessItem(ctx context.Context, customer string, item string) (string, error) {
	fmt.Printf("processing item=%s customer=%s\n", item, customer)

	// Imagine this is a database/API call.
	return "processed", nil
}

func SaveOrder(ctx context.Context, orderID string, customer string, createdAt time.Time) error {
	info := activity.GetInfo(ctx)
	if info.Attempt < 2 {
		return fmt.Errorf("simulated failure for order=%s customer=%s attempt=%d", orderID, customer, info.Attempt)
	}

	fmt.Printf("save order=%s customer=%s createdAt=%s\n", orderID, customer, createdAt)
	return nil
}
