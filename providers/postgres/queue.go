package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/YakirOren/turbine"
)

// DequeueWorkflows implements turbine.SystemDatabase.
//
// With a worker/global/rate limit configured, the counting and the claim run
// in one transaction guarded by a transaction-level advisory lock keyed on
// (schema, queue, partition), so concurrent executors in any process are
// serialized and the limits are exact. Without limits the claim relies on
// FOR UPDATE SKIP LOCKED alone.
func (d *DB) DequeueWorkflows(ctx context.Context, input turbine.DequeueWorkflowsInput) ([]turbine.DequeuedWorkflow, error) {
	limit := input.Limit
	if limit <= 0 {
		limit = turbine.DefaultDequeueLimit
	}
	hasWorker := input.WorkerConcurrency != nil && *input.WorkerConcurrency > 0
	hasGlobal := input.GlobalConcurrency != nil && *input.GlobalConcurrency > 0
	hasRate := input.RateLimit != nil && input.RateLimit.Limit > 0 && input.RateLimit.Period > 0
	partition := ""
	if input.Partitioned {
		partition = input.PartitionKey
	}

	var out []turbine.DequeuedWorkflow
	err := d.inTx(ctx, func(t *txn) error {
		if hasWorker || hasGlobal || hasRate {
			if _, err := t.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
				d.cfg.Schema+":"+input.QueueName+":"+partition); err != nil {
				return err
			}
			var workerRunning, globalRunning, recent int
			args := []any{
				input.QueueName, string(turbine.StatusPending), input.ExecutorID,
				string(turbine.StatusEnqueued), time.Now().Add(-ratePeriod(input)).UnixMilli(),
			}
			part := ""
			if partition != "" {
				part = " AND queue_partition_key = $6"
				args = append(args, partition)
			}
			if err := t.QueryRow(ctx, d.q(`SELECT
					COALESCE(SUM(CASE WHEN status = $2 AND executor_id = $3 THEN 1 ELSE 0 END), 0),
					COALESCE(SUM(CASE WHEN status = $2 THEN 1 ELSE 0 END), 0),
					COALESCE(SUM(CASE WHEN status <> $4 AND updated_at_epoch_ms >= $5 THEN 1 ELSE 0 END), 0)
				FROM {ws} WHERE queue_name = $1`+part), args...).Scan(&workerRunning, &globalRunning, &recent); err != nil {
				return err
			}
			if hasWorker {
				limit = min(limit, *input.WorkerConcurrency-workerRunning)
			}
			if hasGlobal {
				limit = min(limit, *input.GlobalConcurrency-globalRunning)
			}
			if hasRate {
				limit = min(limit, input.RateLimit.Limit-recent)
			}
			if limit <= 0 {
				return nil
			}
		}

		args := []any{
			input.QueueName, string(turbine.StatusEnqueued), limit,
			string(turbine.StatusPending), input.ExecutorID, input.AppVersion, time.Now().UnixMilli(),
		}
		part := ""
		if partition != "" {
			part = " AND queue_partition_key = $8"
			args = append(args, partition)
		}
		order := "created_at_epoch_ms ASC, id ASC"
		if input.PriorityEnabled {
			order = "priority ASC, " + order
		}
		rows, err := t.Query(ctx, d.q(`WITH candidates AS (
				SELECT id FROM {ws}
				WHERE queue_name = $1 AND status = $2`+part+`
				ORDER BY `+order+`
				LIMIT $3
				FOR UPDATE SKIP LOCKED)
			UPDATE {ws} w SET status = $4, executor_id = $5, application_version = $6, updated_at_epoch_ms = $7
			FROM candidates c WHERE w.id = c.id
			RETURNING w.id, w.queue_name, w.name, w.inputs`), args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var wf turbine.DequeuedWorkflow
			var in string
			if err := rows.Scan(&wf.WorkflowID, &wf.QueueName, &wf.Name, &in); err != nil {
				return err
			}
			wf.Input = &in
			out = append(out, wf)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue workflows: %w", err)
	}
	return out, nil
}

func ratePeriod(input turbine.DequeueWorkflowsInput) time.Duration {
	if input.RateLimit != nil && input.RateLimit.Period > 0 {
		return input.RateLimit.Period
	}
	return 0
}

// ClearQueueAssignment implements turbine.SystemDatabase.
func (d *DB) ClearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
	var changed bool
	err := d.inTx(ctx, func(t *txn) error {
		var status string
		if err := t.QueryRow(ctx, d.q(`SELECT status FROM {ws} WHERE id = $1 FOR UPDATE`), workflowID).Scan(&status); err != nil {
			return err
		}
		if turbine.StatusType(status) != turbine.StatusPending {
			return nil
		}
		if _, err := t.Exec(ctx, d.q(`UPDATE {ws} SET status = $2, queue_name = $3, updated_at_epoch_ms = $4 WHERE id = $1`),
			workflowID, string(turbine.StatusEnqueued), turbine.InternalQueueName, time.Now().UnixMilli()); err != nil {
			return err
		}
		changed = true
		return t.notify(ctx, queuePrefix+turbine.InternalQueueName)
	})
	if err != nil {
		return false, fmt.Errorf("failed to clear queue assignment: %w", err)
	}
	return changed, nil
}

// GetQueuePartitions implements turbine.SystemDatabase.
func (d *DB) GetQueuePartitions(ctx context.Context, queueName string) ([]string, error) {
	rows, err := d.pool.Query(ctx, d.q(`SELECT DISTINCT queue_partition_key FROM {ws}
		WHERE queue_name = $1 AND queue_partition_key <> ''`), queueName)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue partitions: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// WaitForEnqueue implements turbine.SystemDatabase. Besides the local and
// LISTEN/NOTIFY wake-ups, the channel also fires after PollInterval, so an
// expired rate limit or a missed notification cannot stall the queue runner.
func (d *DB) WaitForEnqueue(_ context.Context, queueName string) chan struct{} {
	return d.bus.waitTimed(queuePrefix+queueName, d.cfg.PollInterval)
}

// StopWaitForEnqueue implements turbine.SystemDatabase.
func (d *DB) StopWaitForEnqueue(queueName string, ch chan struct{}) {
	d.bus.remove(queuePrefix+queueName, ch)
}
