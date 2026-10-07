package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/YakirOren/turbine"
)

// PaymentGateway simulates an external payment provider as a workflow. It
// waits for a request from the order, then replies with Send.
func PaymentGateway(ctx turbine.Context, orderID string) (string, error) {
	req, err := turbine.Recv[*PaymentRequest](ctx, topicPaymentRequest, time.Minute)
	if err != nil {
		return "", err
	}
	if req == nil {
		return "", fmt.Errorf("no payment request received for %s", orderID)
	}

	receipt, err := turbine.Do(ctx, func(context.Context) (string, error) {
		time.Sleep(time.Second)
		return fmt.Sprintf("rcpt_%s_%s", req.OrderID, req.ChargeID), nil
	}, turbine.WithStepName("settle"))
	if err != nil {
		return "", err
	}

	reply := PaymentResult{Settled: true, Receipt: receipt}
	if err := turbine.Send(ctx, req.ReplyTo, reply, topicPaymentResult); err != nil {
		return "", err
	}
	return receipt, nil
}

// NotifyCustomer is the child workflow that "sends" the customer message.
func NotifyCustomer(ctx turbine.Context, in NotifyInput) (string, error) {
	ctx.SetAppStatus("notifying", "cyan")
	return turbine.Do(ctx, func(c context.Context) (string, error) {
		turbine.LoggerFrom(c).Info("customer notified", "customer", in.Customer, "message", in.Message)
		return "notified " + in.Customer, nil
	}, turbine.WithStepName("send-message"))
}

// generateInvoice stores a text invoice as a product linked to the workflow.
func generateInvoice(ctx context.Context, in OrderInput, receipt string) (string, error) {
	invoiceID := "INV-" + in.OrderID
	body := fmt.Sprintf("Fatura %s\nCliente: %s\nItens: %d\nTotal: R$ %.2f\nRecibo: %s\nData: %s\n",
		invoiceID, in.Customer, in.Items, in.Amount, receipt, time.Now().Format(time.RFC3339))
	err := turbine.SendProduct(ctx, invoiceID+".txt", bytes.NewReader([]byte(body)), map[string]any{
		"type":     "invoice",
		"order_id": in.OrderID,
		"customer": in.Customer,
	})
	if err != nil {
		return "", fmt.Errorf("send invoice product: %w", err)
	}
	return invoiceID, nil
}

// logSender is the ProductSender: it only logs, standing in for S3/e-mail.
type logSender struct{ log *slog.Logger }

func (s logSender) Send(_ context.Context, p turbine.ProductRecord, _ io.Reader) error {
	s.log.Info("product delivered", "file", p.FileName, "size", p.Size, "metadata", p.Metadata)
	return nil
}

var (
	customers = []string{"acme", "globex", "initech", "umbrella", "hooli"}
	// demoAmounts mixes small orders with ones above the approval threshold.
	demoAmounts = []float64{89.9, 149, 320, 499, 750, 1200}
)

func randomOrder() OrderInput {
	return OrderInput{
		OrderID:  fmt.Sprintf("ORD-%06d", rand.IntN(1_000_000)), //nolint:gosec // demo data
		Customer: customers[rand.IntN(len(customers))],          //nolint:gosec // demo data
		Amount:   demoAmounts[rand.IntN(len(demoAmounts))],      //nolint:gosec // demo data
		Items:    1 + rand.IntN(5),                              //nolint:gosec // demo data
	}
}

// GenerateDemoOrders is scheduled by cron; it enqueues a few random orders
// so the dashboard always has something moving. Big ones wait for approval.
func GenerateDemoOrders(ctx turbine.Context, scheduledAt time.Time) (int, error) {
	// One step so a recovery does not enqueue a second batch.
	n, err := turbine.Do(ctx, func(context.Context) (int, error) {
		n := 2 + rand.IntN(3) //nolint:gosec // demo data
		for range n {
			in := randomOrder()
			_, err := turbine.Run(rt, ProcessOrder, in,
				turbine.WithQueue(ordersQueue),
				turbine.WithQueuePartitionKey(in.Customer),
			)
			if err != nil {
				return 0, err
			}
		}
		return n, nil
	}, turbine.WithStepName("enqueue-orders"))
	if err != nil {
		return 0, err
	}
	ctx.Logger().Info("demo orders enqueued", "count", n, "scheduled_at", scheduledAt)
	return n, nil
}
