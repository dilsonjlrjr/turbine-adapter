package main

import "fmt"

// Topics and event keys shared between the order and its helper workflows.
const (
	topicPaymentRequest = "payment.request"
	topicPaymentResult  = "payment.result"
	eventOrderStage     = "stage"
	eventOrderInvoice   = "invoice"

	// KV keys.
	kvApprovalThreshold = "approval_threshold"
	kvOrdersProcessed   = "orders_processed"

	defaultApprovalThreshold = 500.0

	// ordersQueue runs every ProcessOrder; it is partitioned by customer.
	ordersQueue = "orders"
)

// OrderInput is the ProcessOrder input (also the dashboard trigger form).
type OrderInput struct {
	OrderID  string  `json:"order_id"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
	Items    int     `json:"items"`
}

// OrderResult is what ProcessOrder returns.
type OrderResult struct {
	OrderID  string `json:"order_id"`
	Tracking string `json:"tracking"`
	Invoice  string `json:"invoice"`
	ChargeID string `json:"charge_id"`
}

// PaymentRequest is sent by ProcessOrder to the PaymentGateway workflow.
type PaymentRequest struct {
	OrderID  string  `json:"order_id"`
	Amount   float64 `json:"amount"`
	ReplyTo  string  `json:"reply_to"`
	ChargeID string  `json:"charge_id"`
}

// PaymentResult is the gateway reply.
type PaymentResult struct {
	Settled bool   `json:"settled"`
	Receipt string `json:"receipt"`
}

// NotifyInput is the NotifyCustomer (child workflow) input.
type NotifyInput struct {
	OrderID  string `json:"order_id"`
	Customer string `json:"customer"`
	Message  string `json:"message"`
}

func (in OrderInput) summary() string {
	return fmt.Sprintf("Pedido %s de %s (R$ %.2f)", in.OrderID, in.Customer, in.Amount)
}
