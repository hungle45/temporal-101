package workflowcontracts

import (
	"time"

	"go.101.temporal/common/temporalx"
)

type OrderInput struct {
	Customer string   `json:"customer"`
	Items    []string `json:"items"`
}

type OrderOutput struct {
	OrderID   string    `json:"orderId"`
	CreatedAt time.Time `json:"createdAt"`
}

var Order = temporalx.NewWorkflow[OrderInput, OrderOutput]("order", HelloQueue)
