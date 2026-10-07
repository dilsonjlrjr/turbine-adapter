package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/jackc/pgx/v5"
)

// notifyBatch runs stmt and one pg_notify per key in a single round trip and
// implicit transaction, so the notifications are delivered on commit. The
// local waiters are woken after the batch succeeded.
func (d *DB) notifyBatch(ctx context.Context, stmt string, args []any, keys ...string) error {
	batch := &pgx.Batch{}
	batch.Queue(d.q(stmt), args...)
	for _, k := range keys {
		if len(k) <= maxNotifyPayload {
			batch.Queue("SELECT pg_notify($1, $2)", d.channel, k)
		}
	}
	br := d.pool.SendBatch(ctx, batch)
	var firstErr error
	for range batch.Len() {
		if _, err := br.Exec(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr != nil {
		return firstErr
	}
	for _, k := range keys {
		d.bus.notify(k)
	}
	return nil
}

// Send implements turbine.SystemDatabase.
func (d *DB) Send(ctx context.Context, input turbine.SendInput) error {
	id := randomID()
	if input.ProducerWorkflow != "" {
		id = fmt.Sprintf("snd_%s_%d", input.ProducerWorkflow, input.ProducerStepID)
	}
	err := d.notifyBatch(ctx, `INSERT INTO {nt} (id, destination_id, topic, message, created_at_epoch_ms, consumed)
		VALUES ($1, $2, $3, $4, $5, FALSE) ON CONFLICT (id) DO NOTHING`,
		[]any{id, input.DestinationUUID, input.Topic, deref(input.Message), time.Now().UnixMilli()},
		input.DestinationUUID+"::"+input.Topic)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}
	return nil
}

// consume atomically takes the oldest unconsumed message; SKIP LOCKED makes
// concurrent receivers (in any process) pick different rows.
func (d *DB) consume(ctx context.Context, workflowID, topic string) (*string, error) {
	var msg string
	err := d.pool.QueryRow(ctx, d.q(`UPDATE {nt} SET consumed = TRUE
		WHERE id = (
			SELECT id FROM {nt}
			WHERE destination_id = $1 AND topic = $2 AND consumed = FALSE
			ORDER BY created_at_epoch_ms ASC, seq ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED)
		RETURNING message`), workflowID, topic).Scan(&msg)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // no message available.
	}
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// Recv implements turbine.SystemDatabase.
func (d *DB) Recv(ctx context.Context, input turbine.RecvInput) (*string, error) {
	if input.Timeout <= 0 {
		msg, err := d.consume(ctx, input.WorkflowUUID, input.Topic)
		if err != nil {
			return nil, d.waitErr(ctx, "failed to consume notification", err)
		}
		return msg, nil
	}

	key := input.WorkflowUUID + "::" + input.Topic
	ch := d.bus.wait(key) // register before the first query
	defer func() { d.bus.remove(key, ch) }()
	deadline := time.NewTimer(input.Timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		msg, err := d.consume(ctx, input.WorkflowUUID, input.Topic)
		if err != nil {
			return nil, d.waitErr(ctx, "failed to consume notification", err)
		}
		if msg != nil {
			return msg, nil
		}
		select {
		case <-ch:
			ch = d.bus.swap(key, ch)
		case <-ticker.C:
		case <-deadline.C:
			return nil, nil //nolint:nilnil // timeout without a message is (nil, nil).
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// waitErr returns ctx.Err() when the context ended, otherwise a wrapped error.
func (d *DB) waitErr(ctx context.Context, what string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("%s: %w", what, err)
}

// SetEvent implements turbine.SystemDatabase.
func (d *DB) SetEvent(ctx context.Context, input turbine.SetEventInput) error {
	err := d.notifyBatch(ctx, `INSERT INTO {ev} (id, workflow_id, key, value) VALUES ($1, $2, $3, $4)
		ON CONFLICT (workflow_id, key) DO UPDATE SET value = EXCLUDED.value`,
		[]any{input.WorkflowUUID + "_" + input.Key, input.WorkflowUUID, input.Key, deref(input.Value)},
		input.WorkflowUUID+"::"+input.Key)
	if err != nil {
		return fmt.Errorf("failed to set event: %w", err)
	}
	return nil
}

func (d *DB) readEvent(ctx context.Context, workflowID, key string) (*string, error) {
	var value string
	err := d.pool.QueryRow(ctx, d.q(`SELECT value FROM {ev} WHERE workflow_id = $1 AND key = $2`), workflowID, key).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // the event does not exist.
	}
	if err != nil {
		return nil, err
	}
	return &value, nil
}

// GetEvent implements turbine.SystemDatabase.
func (d *DB) GetEvent(ctx context.Context, input turbine.GetEventInput) (*string, error) {
	if input.Timeout <= 0 {
		v, err := d.readEvent(ctx, input.TargetWorkflowUUID, input.Key)
		if err != nil {
			return nil, d.waitErr(ctx, "failed to get event", err)
		}
		return v, nil
	}

	key := input.TargetWorkflowUUID + "::" + input.Key
	ch := d.bus.wait(key)
	defer func() { d.bus.remove(key, ch) }()
	deadline := time.NewTimer(input.Timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		v, err := d.readEvent(ctx, input.TargetWorkflowUUID, input.Key)
		if err != nil {
			return nil, d.waitErr(ctx, "failed to get event", err)
		}
		if v != nil {
			return v, nil
		}
		select {
		case <-ch:
			ch = d.bus.swap(key, ch)
		case <-ticker.C:
		case <-deadline.C:
			return nil, nil //nolint:nilnil // timeout without the event is (nil, nil).
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
