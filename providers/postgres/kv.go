package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/YakirOren/turbine"
	"github.com/jackc/pgx/v5"
)

// SetKV implements turbine.SystemDatabase.
func (d *DB) SetKV(ctx context.Context, input turbine.SetKVInput) error {
	// A nil Schema keeps the stored one, "" clears it (NULL).
	var schema *string
	if input.Schema != nil && *input.Schema != "" {
		schema = input.Schema
	}
	_, err := d.pool.Exec(ctx, d.q(`INSERT INTO {kv} (id, key, value, schema, updated_at_epoch_ms)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value,
			schema = CASE WHEN $6 THEN EXCLUDED.schema ELSE {kv}.schema END,
			updated_at_epoch_ms = EXCLUDED.updated_at_epoch_ms`),
		randomID(), input.Key, deref(input.Value), schema, time.Now().UnixMilli(), input.Schema != nil)
	if err != nil {
		return fmt.Errorf("failed to set KV: %w", err)
	}
	return nil
}

// GetKV implements turbine.SystemDatabase.
func (d *DB) GetKV(ctx context.Context, input turbine.GetKVInput) (*string, error) {
	var value string
	err := d.pool.QueryRow(ctx, d.q(`SELECT value FROM {kv} WHERE key = $1`), input.Key).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // missing key is (nil, nil).
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get KV: %w", err)
	}
	return &value, nil
}

// DeleteKV implements turbine.SystemDatabase.
func (d *DB) DeleteKV(ctx context.Context, input turbine.DeleteKVInput) error {
	if _, err := d.pool.Exec(ctx, d.q(`DELETE FROM {kv} WHERE key = $1`), input.Key); err != nil {
		return fmt.Errorf("failed to delete KV: %w", err)
	}
	return nil
}

// ListKV implements turbine.SystemDatabase.
func (d *DB) ListKV(ctx context.Context, input turbine.ListKVInput) (turbine.KVPage, error) {
	var w where
	if input.Prefix != "" {
		w.add("starts_with(key, ?)", input.Prefix)
	}
	if input.Key != "" {
		w.add("key = ?", input.Key)
	}
	if input.ID != "" {
		w.add("id = ?", input.ID)
	}
	var total int
	if err := d.pool.QueryRow(ctx, d.q("SELECT COUNT(*) FROM {kv}"+w.sql()), w.args...).Scan(&total); err != nil {
		return turbine.KVPage{}, fmt.Errorf("failed to count KV: %w", err)
	}
	offset, limit := clampPage(input.Offset, input.Limit, turbine.DefaultQueryLimit, turbine.MaxQueryLimit)
	rows, err := d.pool.Query(ctx, d.q(fmt.Sprintf(
		"SELECT id, key, value, schema, updated_at_epoch_ms FROM {kv}%s ORDER BY key ASC OFFSET %d LIMIT %d",
		w.sql(), offset, limit)), w.args...)
	if err != nil {
		return turbine.KVPage{}, fmt.Errorf("failed to list KV: %w", err)
	}
	defer rows.Close()

	page := turbine.KVPage{Total: total}
	for rows.Next() {
		var e turbine.KVEntry
		var value string
		var schema *string
		if err := rows.Scan(&e.ID, &e.Key, &value, &schema, &e.UpdatedAt); err != nil {
			return turbine.KVPage{}, fmt.Errorf("failed to scan KV row: %w", err)
		}
		e.Value = &value
		if schema != nil && *schema != "" && *schema != "null" {
			e.Schema = schema
		}
		page.Items = append(page.Items, e)
	}
	return page, rows.Err()
}
