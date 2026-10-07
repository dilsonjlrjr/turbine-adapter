package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/YakirOren/turbine"
)

// errPaymentDeclined is returned randomly by the flaky charge step.
var errPaymentDeclined = errors.New("payment provider timeout")

// failRate is the chance of the charge step failing on each attempt.
const failRate = 0.3

// ProcessOrder is the order-fulfillment pipeline:
// validate -> reserve stock -> charge (flaky, retried) -> payment gateway
// (child workflow + Send/Recv) -> human approval for big orders -> ship
// (durable sleep) -> invoice product -> notify (child workflow).
func ProcessOrder(ctx turbine.Context, in OrderInput) (OrderResult, error) {
	id, err := ctx.WorkflowID()
	if err != nil {
		return OrderResult{}, err
	}

	stage(ctx, "validating", "yellow")
	if _, err := turbine.Do(ctx, func(context.Context) (string, error) {
		return validate(in)
	}, turbine.WithStepName("validate")); err != nil {
		stage(ctx, "invalid", "red")
		return OrderResult{}, err
	}

	stage(ctx, "reserving stock", "orange")
	if _, err := turbine.Do(ctx, func(context.Context) (bool, error) {
		time.Sleep(time.Second)
		return true, nil
	}, turbine.WithStepName("reserve-stock")); err != nil {
		return OrderResult{}, err
	}

	stage(ctx, "charging", "blue")
	chargeID, err := turbine.Do(ctx, chargePayment,
		turbine.WithStepName("charge-payment"),
		turbine.WithStepMaxRetries(6),
		turbine.WithBaseInterval(500*time.Millisecond),
		turbine.WithBackoffFactor(2),
		turbine.WithMaxInterval(5*time.Second),
	)
	if err != nil {
		stage(ctx, "payment failed", "red")
		return OrderResult{}, err
	}

	receipt, err := settleWithGateway(ctx, id, in, chargeID)
	if err != nil {
		stage(ctx, "payment failed", "red")
		return OrderResult{}, err
	}

	if err := approveIfNeeded(ctx, in); err != nil {
		return OrderResult{}, err
	}

	stage(ctx, "shipping", "purple")
	if err := turbine.Pause(ctx, 5*time.Second); err != nil { // durable sleep
		return OrderResult{}, err
	}
	tracking, err := turbine.Do(ctx, func(context.Context) (string, error) {
		return fmt.Sprintf("BR%09d", rand.IntN(1_000_000_000)), nil //nolint:gosec // demo data, not security-sensitive
	}, turbine.WithStepName("ship"))
	if err != nil {
		return OrderResult{}, err
	}

	stage(ctx, "invoicing", "cyan")
	invoice, err := turbine.Do(ctx, func(c context.Context) (string, error) {
		return generateInvoice(c, in, receipt)
	}, turbine.WithStepName("invoice"))
	if err != nil {
		return OrderResult{}, err
	}
	if err := turbine.SetValue(ctx, eventOrderInvoice, invoice); err != nil {
		return OrderResult{}, err
	}

	if err := notifyCustomer(ctx, id, in, tracking); err != nil {
		return OrderResult{}, err
	}
	if err := countProcessed(ctx); err != nil {
		return OrderResult{}, err
	}

	stage(ctx, "fulfilled", "green")
	return OrderResult{OrderID: in.OrderID, Tracking: tracking, Invoice: invoice, ChargeID: chargeID}, nil
}

// stage updates the dashboard app status and publishes it as a workflow event.
func stage(ctx turbine.Context, label, color string) {
	ctx.SetAppStatus(label, color)
	if err := turbine.SetValue(ctx, eventOrderStage, label); err != nil {
		ctx.Logger().Warn("set stage event", "error", err)
	}
}

func validate(in OrderInput) (string, error) {
	if in.OrderID == "" || in.Customer == "" {
		return "", errors.New("order_id and customer are required")
	}
	if in.Amount <= 0 {
		return "", fmt.Errorf("invalid amount %.2f", in.Amount)
	}
	return "valid", nil
}

// chargePayment fails randomly; Turbine retries it with exponential backoff.
func chargePayment(ctx context.Context) (string, error) {
	if rand.Float64() < failRate { //nolint:gosec // demo flakiness
		turbine.LoggerFrom(ctx).Warn("charge attempt failed")
		return "", errPaymentDeclined
	}
	return fmt.Sprintf("ch_%08d", rand.IntN(100_000_000)), nil //nolint:gosec // demo data
}

// settleWithGateway starts the PaymentGateway workflow as a child and waits
// for its reply with Recv. Using a deterministic child ID keeps replays
// idempotent.
func settleWithGateway(ctx turbine.Context, orderWF string, in OrderInput, chargeID string) (string, error) {
	gwID := orderWF + "-gateway"
	if _, err := turbine.Do(ctx, func(context.Context) (string, error) {
		_, err := turbine.Run(rt, PaymentGateway, gwID, turbine.WithID(gwID))
		return gwID, err
	}, turbine.WithStepName("start-gateway")); err != nil {
		return "", err
	}

	req := PaymentRequest{OrderID: in.OrderID, Amount: in.Amount, ReplyTo: orderWF, ChargeID: chargeID}
	if err := turbine.Send(ctx, gwID, req, topicPaymentRequest); err != nil {
		return "", err
	}
	res, err := turbine.Recv[*PaymentResult](ctx, topicPaymentResult, time.Minute)
	if err != nil {
		return "", err
	}
	if res == nil || !res.Settled {
		return "", errors.New("payment gateway did not settle the order")
	}
	return res.Receipt, nil
}

// approveIfNeeded pauses the order until a human approves it from the
// dashboard when the amount exceeds the approval_threshold KV value.
func approveIfNeeded(ctx turbine.Context, in OrderInput) error {
	threshold, err := turbine.Do(ctx, func(c context.Context) (float64, error) {
		v, ok, err := turbine.KVGet[float64](rt, c, kvApprovalThreshold)
		if err != nil || !ok {
			return defaultApprovalThreshold, err
		}
		return v, nil
	}, turbine.WithStepName("read-threshold"))
	if err != nil {
		return err
	}
	if in.Amount <= threshold {
		return nil
	}

	// WaitForApproval sets the "waiting for approval" status itself.
	res, err := turbine.WaitForApproval(ctx, turbine.WithApprovalTimeout(2*time.Hour))
	if err != nil {
		stage(ctx, "approval timed out", "red")
		return err
	}
	if !res.Approved {
		stage(ctx, "rejected", "red")
		return fmt.Errorf("order rejected: %s", res.Comment)
	}
	return nil
}

// notifyCustomer runs NotifyCustomer as a child workflow and waits for it.
func notifyCustomer(ctx turbine.Context, orderWF string, in OrderInput, tracking string) error {
	childID := orderWF + "-notify"
	_, err := turbine.Do(ctx, func(context.Context) (string, error) {
		h, err := turbine.Run(rt, NotifyCustomer, NotifyInput{
			OrderID:  in.OrderID,
			Customer: in.Customer,
			Message:  "seu pedido foi enviado, rastreio " + tracking,
		}, turbine.WithID(childID))
		if err != nil {
			return "", err
		}
		return h.GetResult()
	}, turbine.WithStepName("notify-customer"))
	return err
}

// countProcessed bumps the orders_processed KV counter. The read-modify-write
// is not atomic across executors; fine for a demo counter.
func countProcessed(ctx turbine.Context) error {
	_, err := turbine.Do(ctx, func(c context.Context) (int, error) {
		n, _, err := turbine.KVGet[float64](rt, c, kvOrdersProcessed)
		if err != nil {
			return 0, err
		}
		next := int(n) + 1
		return next, rt.KVSet(c, kvOrdersProcessed, next)
	}, turbine.WithStepName("count-processed"))
	return err
}
