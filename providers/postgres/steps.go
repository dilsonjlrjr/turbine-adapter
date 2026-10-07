package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/YakirOren/turbine"
	"github.com/jackc/pgx/v5"
)

func stepID(workflowID string, functionID int) string {
	return fmt.Sprintf("%s_%d", workflowID, functionID)
}

// RecordOperationStart implements turbine.SystemDatabase.
func (d *DB) RecordOperationStart(ctx context.Context, input turbine.RecordOperationStartInput) error {
	_, err := d.pool.Exec(ctx, d.q(`INSERT INTO {oo}
			(id, workflow_id, function_id, function_name, output, error, started_at_epoch_ms, ended_at_epoch_ms)
		VALUES ($1, $2, $3, $4, '', '', $5, 0)
		ON CONFLICT (workflow_id, function_id) DO UPDATE SET
			function_name = EXCLUDED.function_name, output = '', error = '',
			started_at_epoch_ms = EXCLUDED.started_at_epoch_ms, ended_at_epoch_ms = 0`),
		stepID(input.WorkflowUUID, input.FunctionID), input.WorkflowUUID, input.FunctionID, input.FunctionName, input.StartedAt)
	if err != nil {
		return fmt.Errorf("failed to record step start: %w", err)
	}
	return nil
}

// RecordOperationResult implements turbine.SystemDatabase.
func (d *DB) RecordOperationResult(ctx context.Context, input turbine.RecordOperationResultInput) error {
	_, err := d.pool.Exec(ctx, d.q(`UPDATE {oo} SET output = $3, error = $4, function_name = $5,
			started_at_epoch_ms = $6, ended_at_epoch_ms = $7
		WHERE workflow_id = $1 AND function_id = $2`),
		input.WorkflowUUID, input.FunctionID, deref(input.Output), deref(input.ErrorMsg), input.FunctionName,
		input.StartedAt, input.EndedAt)
	if err != nil {
		return fmt.Errorf("failed to record step result: %w", err)
	}
	return nil
}

// CheckOperationExecution implements turbine.SystemDatabase.
func (d *DB) CheckOperationExecution(ctx context.Context, input turbine.CheckOperationExecutionInput) (*turbine.RecordedResult, error) {
	var status string
	err := d.pool.QueryRow(ctx, d.q(`SELECT status FROM {ws} WHERE id = $1`), input.WorkflowUUID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, turbine.NewErrWorkflowNotFound(input.WorkflowUUID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if turbine.StatusType(status) == turbine.StatusCancelled {
		return nil, turbine.NewErrCancelled(input.WorkflowUUID)
	}

	var output, errText, fn string
	var endedAt int64
	err = d.pool.QueryRow(ctx, d.q(`SELECT output, error, function_name, ended_at_epoch_ms FROM {oo}
		WHERE workflow_id = $1 AND function_id = $2`), input.WorkflowUUID, input.FunctionID).Scan(&output, &errText, &fn, &endedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // (nil, nil) means "execute the step" in the SystemDatabase contract.
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get operation outputs: %w", err)
	}
	if endedAt == 0 {
		return nil, nil //nolint:nilnil // started but never completed: execute the step.
	}
	res := &turbine.RecordedResult{Output: &output, FunctionName: &fn}
	if errText != "" {
		res.ErrorMsg = &errText
	}
	return res, nil
}

// GetWorkflowSteps implements turbine.SystemDatabase.
func (d *DB) GetWorkflowSteps(ctx context.Context, input turbine.GetWorkflowStepsInput) ([]turbine.StepRecord, error) {
	rows, err := d.pool.Query(ctx, d.q(`SELECT function_id, function_name, output, error, started_at_epoch_ms,
			ended_at_epoch_ms, child_workflow_id
		FROM {oo} WHERE workflow_id = $1 ORDER BY function_id ASC`), input.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow steps: %w", err)
	}
	defer rows.Close()

	var steps []turbine.StepRecord
	for rows.Next() {
		s := turbine.StepRecord{WorkflowUUID: input.WorkflowID}
		var output, errText, child string
		var started, ended int64
		if err := rows.Scan(&s.FunctionID, &s.FunctionName, &output, &errText, &started, &ended, &child); err != nil {
			return nil, fmt.Errorf("failed to scan step row: %w", err)
		}
		s.Output, s.ErrorMsg, s.ChildWorkflowID = &output, &errText, &child
		s.StartedAt, s.EndedAt = &started, &ended
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

// RecordChildWorkflow implements turbine.SystemDatabase.
func (d *DB) RecordChildWorkflow(ctx context.Context, input turbine.RecordChildWorkflowInput) error {
	_, err := d.pool.Exec(ctx, d.q(`INSERT INTO {oo} (id, workflow_id, function_id, function_name, child_workflow_id, output, error)
		VALUES ($1, $2, $3, 'childWorkflow', $4, '', '')`),
		stepID(input.WorkflowUUID, input.FunctionID), input.WorkflowUUID, input.FunctionID, input.ChildWorkflowID)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("child workflow %s already registered for parent %s (step %d)", input.ChildWorkflowID, input.WorkflowUUID, input.FunctionID)
		}
		return fmt.Errorf("failed to record child workflow: %w", err)
	}
	return nil
}

// CheckChildWorkflow implements turbine.SystemDatabase.
func (d *DB) CheckChildWorkflow(ctx context.Context, workflowUUID string, functionID int) (*string, error) {
	var child string
	err := d.pool.QueryRow(ctx, d.q(`SELECT child_workflow_id FROM {oo} WHERE workflow_id = $1 AND function_id = $2`),
		workflowUUID, functionID).Scan(&child)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // no row: nothing recorded.
	}
	if err != nil {
		return nil, fmt.Errorf("failed to check child workflow: %w", err)
	}
	if child == "" {
		return nil, nil //nolint:nilnil // row without a child workflow ID.
	}
	return &child, nil
}

// RecordChildGetResult implements turbine.SystemDatabase.
func (d *DB) RecordChildGetResult(ctx context.Context, input turbine.RecordChildGetResultInput) error {
	_, err := d.pool.Exec(ctx, d.q(`INSERT INTO {oo} (id, workflow_id, function_id, function_name, output, error, child_workflow_id)
		VALUES ($1, $2, $3, 'pt.getResult', $4, $5, '')
		ON CONFLICT DO NOTHING`),
		stepID(input.WorkflowUUID, input.FunctionID), input.WorkflowUUID, input.FunctionID, deref(input.Output), deref(input.ErrorMsg))
	if err != nil {
		return fmt.Errorf("failed to record get result: %w", err)
	}
	return nil
}
