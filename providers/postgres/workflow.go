package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/jackc/pgx/v5"
)

const (
	queuePrefix    = "queue::"
	workflowPrefix = "workflow::"
)

// InsertStatus implements turbine.SystemDatabase.
func (d *DB) InsertStatus(ctx context.Context, input turbine.InsertStatusInput) (*turbine.InsertWorkflowResult, error) {
	st := input.Status
	attempts := 1
	if st.Status == turbine.StatusEnqueued {
		attempts = 0
	}
	updatedAt := time.Now()
	if !st.UpdatedAt.IsZero() {
		updatedAt = st.UpdatedAt
	}
	var deadline, timeoutMs int64
	if !st.Deadline.IsZero() {
		deadline = st.Deadline.UnixMilli()
	}
	if st.Timeout > 0 {
		timeoutMs = st.Timeout.Round(time.Millisecond).Milliseconds()
	}
	inc := 0
	if input.IncrementAttempts {
		inc = 1
	}
	inputs := ""
	switch v := st.Input.(type) {
	case string:
		inputs = v
	case *string:
		inputs = deref(v)
	}
	tags := "[]"
	if len(st.Tags) > 0 {
		b, _ := json.Marshal(st.Tags)
		tags = string(b)
	}

	var (
		res     turbine.InsertWorkflowResult
		postErr error
	)
	err := d.inTx(ctx, func(t *txn) error {
		var queueName string
		var timeoutRes, deadlineRes int64
		err := t.QueryRow(ctx, d.q(`INSERT INTO {ws} (
				id, status, name, queue_name, executor_id, application_version, application_id,
				created_at_epoch_ms, recovery_attempts, updated_at_epoch_ms,
				workflow_timeout_ms, workflow_deadline_epoch_ms,
				inputs, deduplication_id, priority, queue_partition_key,
				owner_xid, parent_workflow_id, tags, summary
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
			ON CONFLICT (id) DO UPDATE SET
				recovery_attempts = CASE WHEN EXCLUDED.status <> $21 THEN {ws}.recovery_attempts + $22 ELSE {ws}.recovery_attempts END,
				updated_at_epoch_ms = EXCLUDED.updated_at_epoch_ms,
				executor_id = CASE WHEN EXCLUDED.status = $21 THEN {ws}.executor_id ELSE EXCLUDED.executor_id END
			RETURNING recovery_attempts, status, name, queue_name, workflow_timeout_ms,
				workflow_deadline_epoch_ms, owner_xid, app_status, app_status_color,
				EXISTS (SELECT 1 FROM {oo} WHERE workflow_id = {ws}.id)`),
			st.ID, string(st.Status), st.Name, st.QueueName, st.ExecutorID, st.ApplicationVersion, st.ApplicationID,
			st.CreatedAt.Round(time.Millisecond).UnixMilli(), attempts, updatedAt.UnixMilli(),
			timeoutMs, deadline,
			inputs, st.DeduplicationID, st.Priority, st.QueuePartitionKey,
			deref(input.OwnerXID), st.ParentWorkflowID, tags, st.Summary,
			string(turbine.StatusEnqueued), inc,
		).Scan(&res.Attempts, &res.Status, &res.Name, &queueName, &timeoutRes, &deadlineRes,
			&res.OwnerXID, &res.AppStatus, &res.AppStatusColor, &res.HasSteps)
		if err != nil {
			return err
		}
		if queueName != "" {
			res.QueueName = &queueName
		}
		if timeoutRes > 0 {
			res.Timeout = time.Duration(timeoutRes) * time.Millisecond
		}
		res.WorkflowDeadline = msToTime(deadlineRes)

		if res.Status == turbine.StatusEnqueued && queueName != "" {
			if err := t.notify(ctx, queuePrefix+queueName); err != nil {
				return err
			}
		}

		// The upsert is kept even when one of the checks below fails (the
		// built-in implementation autocommits it), so these set postErr
		// instead of aborting the transaction.
		if st.Name != "" && res.Name != st.Name {
			postErr = turbine.NewErrWorkflowConflict(st.ID, fmt.Sprintf("Workflow already exists with a different name: %s, but the provided name is: %s", res.Name, st.Name))
			return nil
		}
		if st.QueueName != "" && res.QueueName != nil && st.QueueName != *res.QueueName {
			postErr = turbine.NewErrWorkflowConflict(st.ID, fmt.Sprintf("Workflow already exists in a different queue: %s, but the provided queue is: %s", *res.QueueName, st.QueueName))
			return nil
		}
		if res.Status != turbine.StatusSuccess && res.Status != turbine.StatusError &&
			input.MaxRetries > 0 && res.Attempts > input.MaxRetries+1 {
			if _, err := t.Exec(ctx, d.q(`UPDATE {ws} SET status = $2, deduplication_id = '', queue_name = ''
				WHERE id = $1 AND status = $3`),
				st.ID, string(turbine.StatusMaxRecoveryAttemptsExceeded), string(turbine.StatusPending)); err != nil {
				return fmt.Errorf("failed to update workflow to %s: %w", turbine.StatusMaxRecoveryAttemptsExceeded, err)
			}
			if err := t.notify(ctx, workflowPrefix+st.ID); err != nil {
				return err
			}
			postErr = turbine.NewErrDeadLetter(st.ID, input.MaxRetries)
		}
		return nil
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, turbine.NewErrDeduplicated(st.ID, st.QueueName, st.DeduplicationID)
		}
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}
	if postErr != nil {
		return nil, postErr
	}
	return &res, nil
}

const listColumns = `id, status, name, executor_id, created_at_epoch_ms, updated_at_epoch_ms,
	application_version, application_id, recovery_attempts, queue_name,
	workflow_timeout_ms, workflow_deadline_epoch_ms, deduplication_id, priority,
	queue_partition_key, forked_from_workflow_id, parent_workflow_id,
	app_status, app_status_color, tags, summary`

// scanStatus scans listColumns followed by dest (extra columns).
func (d *DB) scanStatus(rows pgx.Rows, extra ...any) (turbine.Status, error) {
	var (
		wf                    turbine.Status
		createdMs, updatedMs  int64
		timeoutMs, deadlineMs int64
		status, tags          string
	)
	args := []any{
		&wf.ID, &status, &wf.Name, &wf.ExecutorID, &createdMs, &updatedMs,
		&wf.ApplicationVersion, &wf.ApplicationID, &wf.Attempts, &wf.QueueName,
		&timeoutMs, &deadlineMs, &wf.DeduplicationID, &wf.Priority,
		&wf.QueuePartitionKey, &wf.ForkedFrom, &wf.ParentWorkflowID,
		&wf.AppStatus, &wf.AppStatusColor, &tags, &wf.Summary,
	}
	if err := rows.Scan(append(args, extra...)...); err != nil {
		return wf, fmt.Errorf("failed to scan workflow row: %w", err)
	}
	wf.Status = turbine.StatusType(status)
	wf.CreatedAt = time.Unix(0, createdMs*int64(time.Millisecond))
	wf.UpdatedAt = time.Unix(0, updatedMs*int64(time.Millisecond))
	if timeoutMs > 0 {
		wf.Timeout = time.Duration(timeoutMs) * time.Millisecond
	}
	wf.Deadline = msToTime(deadlineMs)
	if tags != "" {
		if err := json.Unmarshal([]byte(tags), &wf.Tags); err != nil {
			wf.Tags = nil
			d.log.Warn("failed to parse workflow tags", "workflow_id", wf.ID, "error", err)
		}
	}
	return wf, nil
}

// ListWorkflows implements turbine.SystemDatabase.
func (d *DB) ListWorkflows(ctx context.Context, input turbine.ListWorkflowsInput) ([]turbine.Status, error) {
	var w where
	if len(input.Status) > 0 {
		w.add("status = ANY(?)", strs(input.Status))
	}
	if len(input.WorkflowName) > 0 {
		w.add("name = ANY(?)", input.WorkflowName)
	}
	if len(input.ExecutorIDs) > 0 {
		w.add("executor_id = ANY(?)", input.ExecutorIDs)
	}
	if len(input.ApplicationVersion) > 0 {
		w.add("application_version = ANY(?)", input.ApplicationVersion)
	}
	if len(input.WorkflowIDs) > 0 {
		w.add("id = ANY(?)", input.WorkflowIDs)
	}
	if input.CreatedBefore != nil {
		w.add("created_at_epoch_ms <= ?", input.CreatedBefore.UnixMilli())
	}
	if input.CreatedAfter != nil {
		w.add("created_at_epoch_ms >= ?", input.CreatedAfter.UnixMilli())
	}
	cols := listColumns
	if input.LoadInput {
		cols += ", inputs"
	}
	order := "DESC"
	if input.SortAscending {
		order = "ASC"
	}
	sql := "SELECT " + cols + " FROM {ws}" + w.sql() + " ORDER BY created_at_epoch_ms " + order + ", id ASC"
	if input.Limit > 0 {
		sql += " LIMIT " + strconv.Itoa(input.Limit)
	}
	rows, err := d.pool.Query(ctx, d.q(sql), w.args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}
	defer rows.Close()

	var out []turbine.Status
	for rows.Next() {
		var in string
		var wf turbine.Status
		if input.LoadInput {
			wf, err = d.scanStatus(rows, &in)
			wf.Input = &in
		} else {
			wf, err = d.scanStatus(rows)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, wf)
	}
	return out, rows.Err()
}

func (w *where) workflowQuery(q turbine.WorkflowQuery) {
	if len(q.IDs) > 0 {
		w.add("id = ANY(?)", q.IDs)
	}
	if len(q.Statuses) > 0 {
		w.add("status = ANY(?)", strs(q.Statuses))
	}
	if len(q.Names) > 0 {
		w.add("name = ANY(?)", q.Names)
	}
	if q.NameContains != "" {
		w.add("strpos(lower(name), lower(?)) > 0", q.NameContains)
	}
	if len(q.Tags) > 0 {
		encoded := make([]string, len(q.Tags))
		for i, tag := range q.Tags {
			b, _ := json.Marshal(tag)
			encoded[i] = string(b)
		}
		// Whole-element match: the stored tags are a JSON array of strings.
		w.add("EXISTS (SELECT 1 FROM unnest(?::text[]) AS tg WHERE tags::jsonb @> tg::jsonb)", encoded)
	}
	if q.QueueName != "" {
		w.add("queue_name = ?", q.QueueName)
	}
	if q.ParentWorkflowID != "" {
		w.add("parent_workflow_id = ?", q.ParentWorkflowID)
	}
	if q.CreatedFrom > 0 {
		w.add("created_at_epoch_ms >= ?", q.CreatedFrom)
	}
	if q.CreatedTo > 0 {
		w.add("created_at_epoch_ms < ?", q.CreatedTo)
	}
}

// QueryWorkflows implements turbine.SystemDatabase.
func (d *DB) QueryWorkflows(ctx context.Context, query turbine.WorkflowQuery) (turbine.WorkflowPage, error) {
	var w where
	w.workflowQuery(query)

	var total int
	if err := d.pool.QueryRow(ctx, d.q("SELECT COUNT(*) FROM {ws}"+w.sql()), w.args...).Scan(&total); err != nil {
		return turbine.WorkflowPage{}, fmt.Errorf("failed to count workflows: %w", err)
	}

	sortCol := "created_at_epoch_ms"
	switch query.SortField {
	case turbine.WorkflowSortUpdatedAt:
		sortCol = "updated_at_epoch_ms"
	case turbine.WorkflowSortName:
		sortCol = "name"
	case turbine.WorkflowSortStatus:
		sortCol = "status"
	}
	dir := "ASC"
	if query.SortDesc {
		dir = "DESC"
	}
	offset, limit := clampPage(query.Offset, query.Limit, turbine.DefaultQueryLimit, turbine.MaxQueryLimit)

	cols := listColumns + ", error, owner_xid, parent_function_id"
	if query.IncludeInput {
		cols += ", inputs"
	}
	if query.IncludeOutput {
		cols += ", output"
	}
	sql := "SELECT " + cols + " FROM {ws}" + w.sql() +
		" ORDER BY " + sortCol + " " + dir + `, id COLLATE "C" ASC OFFSET ` + strconv.Itoa(offset) + " LIMIT " + strconv.Itoa(limit)
	rows, err := d.pool.Query(ctx, d.q(sql), w.args...)
	if err != nil {
		return turbine.WorkflowPage{}, fmt.Errorf("failed to query workflows: %w", err)
	}
	defer rows.Close()

	page := turbine.WorkflowPage{Total: total}
	for rows.Next() {
		var errText, ownerXID, inputStr, outputStr string
		var parentFn int
		extra := []any{&errText, &ownerXID, &parentFn}
		if query.IncludeInput {
			extra = append(extra, &inputStr)
		}
		if query.IncludeOutput {
			extra = append(extra, &outputStr)
		}
		wf, err := d.scanStatus(rows, extra...)
		if err != nil {
			return turbine.WorkflowPage{}, err
		}
		wf.OwnerXID = ownerXID
		wf.ParentFunctionID = parentFn
		if errText != "" {
			wf.Error = errors.New(errText)
		}
		if inputStr != "" {
			wf.Input = &inputStr
		}
		if outputStr != "" {
			wf.Output = &outputStr
		}
		page.Items = append(page.Items, wf)
	}
	return page, rows.Err()
}

// CountWorkflows implements turbine.SystemDatabase.
func (d *DB) CountWorkflows(ctx context.Context, input turbine.CountWorkflowsInput) ([]turbine.WorkflowCount, error) {
	var w where
	if input.QueueName != "" {
		w.add("queue_name = ?", input.QueueName)
	}
	if len(input.Names) > 0 {
		w.add("name = ANY(?)", input.Names)
	}
	if len(input.Statuses) > 0 {
		w.add("status = ANY(?)", strs(input.Statuses))
	}
	if input.Tag != "" {
		b, _ := json.Marshal(input.Tag)
		w.add("tags::jsonb @> ?::jsonb", string(b))
	}
	if input.CreatedFrom > 0 {
		w.add("created_at_epoch_ms >= ?", input.CreatedFrom)
	}
	if input.CreatedTo > 0 {
		w.add("created_at_epoch_ms < ?", input.CreatedTo)
	}
	bucket := "0::bigint"
	if input.BucketMs > 0 {
		n := strconv.FormatInt(input.BucketMs, 10)
		bucket = "(created_at_epoch_ms / " + n + ") * " + n
	}
	sql := "SELECT " + bucket + " AS bucket, status, COUNT(*) FROM {ws}" + w.sql() +
		` GROUP BY bucket, status ORDER BY bucket ASC, status COLLATE "C" ASC`
	rows, err := d.pool.Query(ctx, d.q(sql), w.args...)
	if err != nil {
		return nil, fmt.Errorf("failed to count workflows: %w", err)
	}
	defer rows.Close()

	var out []turbine.WorkflowCount
	for rows.Next() {
		var c turbine.WorkflowCount
		var status string
		var n int64
		if err := rows.Scan(&c.Bucket, &status, &n); err != nil {
			return nil, fmt.Errorf("failed to scan workflow count: %w", err)
		}
		c.Status, c.Count = turbine.StatusType(status), int(n)
		out = append(out, c)
	}
	return out, rows.Err()
}

// UpdateWorkflowOutcome implements turbine.SystemDatabase.
func (d *DB) UpdateWorkflowOutcome(ctx context.Context, input turbine.UpdateWorkflowOutcomeInput) error {
	err := d.inTx(ctx, func(t *txn) error {
		var queueName string
		err := t.QueryRow(ctx, d.q(`UPDATE {ws}
			SET status = $2::text, output = $3, error = $4, updated_at_epoch_ms = $5, deduplication_id = ''
			WHERE id = $1 AND NOT (status = $6 AND $2::text IN ($7, $8))
			RETURNING queue_name`),
			input.WorkflowID, string(input.Status), deref(input.Output), deref(input.ErrorMsg), time.Now().UnixMilli(),
			string(turbine.StatusCancelled), string(turbine.StatusSuccess), string(turbine.StatusError),
		).Scan(&queueName)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // missing or already cancelled: nothing to update or notify
		}
		if err != nil {
			return err
		}
		if err := t.notify(ctx, workflowPrefix+input.WorkflowID); err != nil {
			return err
		}
		if queueName != "" {
			return t.notify(ctx, queuePrefix+queueName)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

// AwaitWorkflowResult implements turbine.SystemDatabase.
func (d *DB) AwaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*string, error) {
	key := workflowPrefix + workflowID
	ch := d.bus.wait(key)
	defer func() { d.bus.remove(key, ch) }()

	poll := d.cfg.PollInterval
	if pollInterval > 0 && pollInterval < poll {
		poll = pollInterval
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	for {
		var status, output, errText string
		var attempts int
		err := d.pool.QueryRow(ctx, d.q(`SELECT status, output, error, recovery_attempts FROM {ws} WHERE id = $1`), workflowID).
			Scan(&status, &output, &errText, &attempts)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("failed to query workflow status: %w", err)
		}
		if err == nil {
			switch turbine.StatusType(status) {
			case turbine.StatusSuccess, turbine.StatusError:
				if errText == "" {
					return &output, nil
				}
				return &output, errors.New(errText)
			case turbine.StatusCancelled:
				return &output, turbine.NewErrAwaitCancelled(workflowID)
			case turbine.StatusMaxRecoveryAttemptsExceeded:
				return &output, turbine.NewErrDeadLetter(workflowID, attempts-2)
			}
		}
		select {
		case <-ch:
			ch = d.bus.swap(key, ch)
		case <-ticker.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// CancelWorkflow implements turbine.SystemDatabase.
func (d *DB) CancelWorkflow(ctx context.Context, input turbine.CancelWorkflowInput) (bool, error) {
	var changed bool
	err := d.inTx(ctx, func(t *txn) error {
		var status, queueName string
		err := t.QueryRow(ctx, d.q(`SELECT status, queue_name FROM {ws} WHERE id = $1 FOR UPDATE`), input.WorkflowID).Scan(&status, &queueName)
		if errors.Is(err, pgx.ErrNoRows) {
			return turbine.NewErrWorkflowNotFound(input.WorkflowID)
		}
		if err != nil {
			return err
		}
		switch turbine.StatusType(status) {
		case turbine.StatusSuccess, turbine.StatusError, turbine.StatusCancelled:
			return nil
		}
		if _, err := t.Exec(ctx, d.q(`UPDATE {ws} SET status = $2, updated_at_epoch_ms = $3, deduplication_id = '', queue_name = '' WHERE id = $1`),
			input.WorkflowID, string(turbine.StatusCancelled), time.Now().UnixMilli()); err != nil {
			return err
		}
		changed = true
		if err := t.notify(ctx, workflowPrefix+input.WorkflowID); err != nil {
			return err
		}
		if queueName != "" {
			return t.notify(ctx, queuePrefix+queueName)
		}
		return nil
	})
	if err != nil {
		var te *turbine.Error
		if errors.As(err, &te) {
			return false, err
		}
		return false, fmt.Errorf("failed to update workflow status to CANCELLED: %w", err)
	}
	return changed, nil
}

// ResumeWorkflow implements turbine.SystemDatabase.
func (d *DB) ResumeWorkflow(ctx context.Context, input turbine.ResumeWorkflowInput) error {
	err := d.inTx(ctx, func(t *txn) error {
		tag, err := t.Exec(ctx, d.q(`UPDATE {ws}
			SET status = $2, queue_name = $3, recovery_attempts = 0, workflow_deadline_epoch_ms = 0,
			    deduplication_id = '', updated_at_epoch_ms = $4
			WHERE id = $1 AND status NOT IN ($5, $6)`),
			input.WorkflowID, string(turbine.StatusEnqueued), turbine.InternalQueueName, time.Now().UnixMilli(),
			string(turbine.StatusSuccess), string(turbine.StatusError))
		if err != nil {
			return err
		}
		if tag.RowsAffected() > 0 {
			return t.notify(ctx, queuePrefix+turbine.InternalQueueName)
		}
		var one int
		err = t.QueryRow(ctx, d.q(`SELECT 1 FROM {ws} WHERE id = $1`), input.WorkflowID).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return turbine.NewErrWorkflowNotFound(input.WorkflowID)
		}
		return err
	})
	if err != nil {
		var te *turbine.Error
		if errors.As(err, &te) {
			return err
		}
		return fmt.Errorf("failed to update workflow status to ENQUEUED: %w", err)
	}
	return nil
}

// ForkWorkflow implements turbine.SystemDatabase.
func (d *DB) ForkWorkflow(ctx context.Context, input turbine.ForkWorkflowInput) (string, error) {
	if input.StartStepID < 0 {
		return "", fmt.Errorf("startStepID must be >= 0, got %d", input.StartStepID)
	}
	newID := input.NewWorkflowID
	if newID == "" {
		newID = randomID()
	}
	now := time.Now().UnixMilli()
	err := d.inTx(ctx, func(t *txn) error {
		var name, appVersion, appID string
		err := t.QueryRow(ctx, d.q(`SELECT name, application_version, application_id FROM {ws} WHERE id = $1`),
			input.OriginalWorkflowID).Scan(&name, &appVersion, &appID)
		if errors.Is(err, pgx.ErrNoRows) {
			return turbine.NewErrWorkflowNotFound(input.OriginalWorkflowID)
		}
		if err != nil {
			return err
		}
		if _, err := t.Exec(ctx, d.q(`INSERT INTO {ws} (
				id, status, name, application_version, application_id, queue_name, inputs,
				created_at_epoch_ms, updated_at_epoch_ms, recovery_attempts, forked_from_workflow_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, 0, $9)`),
			newID, string(turbine.StatusEnqueued), name, appVersion, appID, turbine.InternalQueueName,
			deref(input.Input), now, input.OriginalWorkflowID); err != nil {
			return fmt.Errorf("failed to insert forked workflow: %w", err)
		}
		if input.StartStepID > 0 {
			if _, err := t.Exec(ctx, d.q(`INSERT INTO {oo}
					(id, workflow_id, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, ended_at_epoch_ms)
				SELECT $1 || '_' || function_id, $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, ended_at_epoch_ms
				FROM {oo} WHERE workflow_id = $2 AND function_id < $3`),
				newID, input.OriginalWorkflowID, input.StartStepID); err != nil {
				return fmt.Errorf("failed to copy operation outputs: %w", err)
			}
			if _, err := t.Exec(ctx, d.q(`INSERT INTO {eh} (id, workflow_id, function_id, key, value)
				SELECT $1 || '_' || function_id || '_' || key, $1, function_id, key, value
				FROM {eh} WHERE workflow_id = $2 AND function_id < $3`),
				newID, input.OriginalWorkflowID, input.StartStepID); err != nil {
				return fmt.Errorf("failed to copy workflow events history: %w", err)
			}
			if _, err := t.Exec(ctx, d.q(`INSERT INTO {ev} (id, workflow_id, key, value)
				SELECT DISTINCT ON (key) $1 || '_' || key, $1, key, value
				FROM {eh} WHERE workflow_id = $2 AND function_id < $3
				ORDER BY key, function_id DESC`),
				newID, input.OriginalWorkflowID, input.StartStepID); err != nil {
				return fmt.Errorf("failed to copy latest workflow events: %w", err)
			}
		}
		return t.notify(ctx, queuePrefix+turbine.InternalQueueName)
	})
	if err != nil {
		return "", err
	}
	return newID, nil
}

// UpdateAppStatus implements turbine.SystemDatabase.
func (d *DB) UpdateAppStatus(ctx context.Context, input turbine.UpdateAppStatusInput) error {
	tag, err := d.pool.Exec(ctx, d.q(`UPDATE {ws} SET app_status = $2, app_status_color = $3, updated_at_epoch_ms = $4 WHERE id = $1`),
		input.WorkflowID, input.AppStatus, input.AppStatusColor, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("failed to update app status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("failed to find workflow status record: %w", pgx.ErrNoRows)
	}
	return nil
}

// deleteDependents removes the rows that have no foreign key to
// workflow_status (messages and events); operation_outputs cascade by FK.
func (d *DB) deleteDependents(ctx context.Context, t *txn, idCond string, args ...any) error {
	for _, stmt := range []string{
		"DELETE FROM {nt} WHERE destination_id " + idCond,
		"DELETE FROM {ev} WHERE workflow_id " + idCond,
		"DELETE FROM {eh} WHERE workflow_id " + idCond,
	} {
		if _, err := t.Exec(ctx, d.q(stmt), args...); err != nil {
			return err
		}
	}
	return nil
}

// GarbageCollectWorkflows implements turbine.SystemDatabase. It runs in one
// transaction; step rows go away through ON DELETE CASCADE.
func (d *DB) GarbageCollectWorkflows(ctx context.Context, input turbine.GarbageCollectInput) error {
	cutoff := input.CutoffTime.UnixMilli()
	var deleted int64
	err := d.inTx(ctx, func(t *txn) error {
		const victims = `IN (SELECT id FROM {ws} WHERE created_at_epoch_ms < $1 AND status NOT IN ($2, $3))`
		args := []any{cutoff, string(turbine.StatusPending), string(turbine.StatusEnqueued)}
		if err := d.deleteDependents(ctx, t, victims, args...); err != nil {
			return fmt.Errorf("failed to gc dependents: %w", err)
		}
		tag, err := t.Exec(ctx, d.q(`DELETE FROM {ws} WHERE created_at_epoch_ms < $1 AND status NOT IN ($2, $3)`), args...)
		if err != nil {
			return fmt.Errorf("failed to gc workflows: %w", err)
		}
		deleted = tag.RowsAffected()
		return nil
	})
	if err != nil {
		return err
	}
	d.log.Info("Garbage collected workflows", "cutoff", cutoff, "deleted", deleted)
	return nil
}

// DeleteWorkflow implements turbine.SystemDatabase.
func (d *DB) DeleteWorkflow(ctx context.Context, input turbine.DeleteWorkflowInput) (bool, error) {
	var deleted bool
	err := d.inTx(ctx, func(t *txn) error {
		var status string
		err := t.QueryRow(ctx, d.q(`SELECT status FROM {ws} WHERE id = $1 FOR UPDATE`), input.WorkflowID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to load workflow: %w", err)
		}
		if st := turbine.StatusType(status); st == turbine.StatusPending || st == turbine.StatusEnqueued {
			return turbine.NewErrWorkflowActive(input.WorkflowID)
		}
		if err := d.deleteDependents(ctx, t, "= $1", input.WorkflowID); err != nil {
			return fmt.Errorf("failed to delete dependents of workflow: %w", err)
		}
		if _, err := t.Exec(ctx, d.q(`DELETE FROM {ws} WHERE id = $1`), input.WorkflowID); err != nil {
			return fmt.Errorf("failed to delete workflow: %w", err)
		}
		deleted = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return deleted, nil
}
