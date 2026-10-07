-- Initial schema of the Turbine execution state.
-- Applied with search_path set to the configured schema, so names are unqualified.
-- Foreign keys: operation_outputs cascades with its workflow. notifications,
-- workflow_events and workflow_events_history deliberately have NO foreign key:
-- the SystemDatabase contract lets Send and SetEvent target a workflow that
-- does not exist (yet), so those rows are removed explicitly by
-- GarbageCollectWorkflows and DeleteWorkflow.
-- Opaque payload columns (inputs, output, message, value, ...) are TEXT: the
-- runtime hands over already-encoded strings and they are stored byte for byte.

CREATE TABLE workflow_status (
    id                         TEXT    PRIMARY KEY,
    status                     TEXT    COLLATE "C" NOT NULL,
    name                       TEXT    COLLATE "C" NOT NULL DEFAULT '',
    inputs                     TEXT    NOT NULL DEFAULT '',
    output                     TEXT    NOT NULL DEFAULT '',
    error                      TEXT    NOT NULL DEFAULT '',
    executor_id                TEXT    NOT NULL DEFAULT '',
    application_version        TEXT    NOT NULL DEFAULT '',
    application_id             TEXT    NOT NULL DEFAULT '',
    recovery_attempts          INTEGER NOT NULL DEFAULT 0,
    created_at_epoch_ms        BIGINT  NOT NULL DEFAULT 0,
    updated_at_epoch_ms        BIGINT  NOT NULL DEFAULT 0,
    queue_name                 TEXT    NOT NULL DEFAULT '',
    queue_partition_key        TEXT    NOT NULL DEFAULT '',
    deduplication_id           TEXT    NOT NULL DEFAULT '',
    priority                   INTEGER NOT NULL DEFAULT 0,
    workflow_timeout_ms        BIGINT  NOT NULL DEFAULT 0,
    workflow_deadline_epoch_ms BIGINT  NOT NULL DEFAULT 0,
    owner_xid                  TEXT    NOT NULL DEFAULT '',
    parent_function_id         INTEGER NOT NULL DEFAULT 0,
    app_status                 TEXT    NOT NULL DEFAULT '',
    app_status_color           TEXT    NOT NULL DEFAULT '',
    forked_from_workflow_id    TEXT    NOT NULL DEFAULT '',
    parent_workflow_id         TEXT    NOT NULL DEFAULT '',
    tags                       TEXT    NOT NULL DEFAULT '[]',
    summary                    TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX idx_workflow_status_executor ON workflow_status (executor_id, status, application_version);
CREATE UNIQUE INDEX idx_workflow_status_dedup ON workflow_status (queue_name, deduplication_id) WHERE deduplication_id <> '';
CREATE INDEX idx_workflow_status_queue_status ON workflow_status (queue_name, status);
CREATE INDEX idx_workflow_status_created ON workflow_status (created_at_epoch_ms);
CREATE INDEX idx_workflow_status_queue_partition ON workflow_status (queue_name, status, queue_partition_key);
CREATE INDEX idx_workflow_status_parent ON workflow_status (parent_workflow_id) WHERE parent_workflow_id <> '';

CREATE TABLE operation_outputs (
    id                  TEXT    PRIMARY KEY,
    workflow_id         TEXT    NOT NULL REFERENCES workflow_status (id) ON DELETE CASCADE,
    function_id         INTEGER NOT NULL,
    output              TEXT    NOT NULL DEFAULT '',
    error               TEXT    NOT NULL DEFAULT '',
    child_workflow_id   TEXT    NOT NULL DEFAULT '',
    function_name       TEXT    NOT NULL DEFAULT '',
    started_at_epoch_ms BIGINT  NOT NULL DEFAULT 0,
    ended_at_epoch_ms   BIGINT  NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX idx_operation_outputs_pk ON operation_outputs (workflow_id, function_id);

CREATE TABLE notifications (
    id                  TEXT    PRIMARY KEY,
    destination_id      TEXT    NOT NULL,
    topic               TEXT    NOT NULL DEFAULT '',
    message             TEXT    NOT NULL DEFAULT '',
    created_at_epoch_ms BIGINT  NOT NULL DEFAULT 0,
    consumed            BOOLEAN NOT NULL DEFAULT FALSE,
    seq                 BIGINT  GENERATED ALWAYS AS IDENTITY
);

CREATE INDEX idx_notifications_dest_topic ON notifications (destination_id, topic, created_at_epoch_ms, seq) WHERE consumed = FALSE;

CREATE TABLE workflow_events (
    id          TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       TEXT NOT NULL DEFAULT ''
);

CREATE UNIQUE INDEX idx_workflow_events_pk ON workflow_events (workflow_id, key);

CREATE TABLE workflow_events_history (
    id          TEXT    PRIMARY KEY,
    workflow_id TEXT    NOT NULL,
    function_id INTEGER NOT NULL,
    key         TEXT    NOT NULL,
    value       TEXT    NOT NULL DEFAULT ''
);

CREATE UNIQUE INDEX idx_workflow_events_history_pk ON workflow_events_history (workflow_id, function_id, key);

CREATE TABLE kv (
    id                  TEXT   PRIMARY KEY,
    key                 TEXT   COLLATE "C" NOT NULL,
    value               TEXT   NOT NULL DEFAULT '',
    schema              TEXT,
    updated_at_epoch_ms BIGINT NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX idx_kv_key ON kv (key);
