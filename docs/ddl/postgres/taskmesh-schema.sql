-- taskmesh reference schema (PostgreSQL)
-- =====================================================================
-- The seven tables owned by each of taskmesh-simple/-pro/-max (D31), mapped
-- from the JPA entities in com.palmyralabs.taskmesh.{simple,pro,max}.entity —
-- identical across tiers except tm_task.promoted_at (taskmesh-max only, see
-- below). taskmesh ships NO DDL of its own and
-- runs Hibernate with ddl-auto=validate — so a consuming application must
-- create these tables from a script like this (kept in its own dbscripts/),
-- then let taskmesh validate against them.
--
-- Notes:
--   * TEXT columns (error_summary/detail/message) are mapped in the entities
--     with @JdbcTypeCode(LONGVARCHAR), NOT @Lob — on Postgres @Lob on a String
--     becomes the `oid` large-object type, which is wrong. Keep them TEXT.
--   * Instant columns are declared `timestamp(6) with time zone` to match
--     Hibernate 6's default mapping for java.time.Instant (TIMESTAMP_UTC). If
--     your Hibernate is configured to map Instant without a zone, switch these
--     to `timestamp(6)`.
--   * tm_task + tm_in_flight are a pair (one row each per task); query/claim/
--     release read only tm_in_flight. See docs/schema.md for the full model.
--   * partition_key (D32, taskmesh-max ONLY) is a caller-supplied, required
--     column on tm_task/tm_in_flight/tm_history_event/tm_incident/
--     tm_attempt_history — omit it entirely for taskmesh-simple/-pro. It's a
--     PLAIN column only on tm_in_flight (never physically partitioned); on
--     the other four it's part of the PRIMARY KEY, because Postgres requires
--     the partition column in every unique constraint on a table you actually
--     partition. taskmesh ships ZERO partitioning opinion — no PARTITION BY
--     clause, no scheme, no modulus below. Add your own PARTITION BY (...)
--     and per-partition CREATE TABLE ... PARTITION OF statements, sized to
--     your own data, if/when you actually partition these tables physically.
--     tm_audit_event is explicitly excluded from this — deferred (D32).
-- =====================================================================

-- ---- tm_task (cold ledger) -------------------------------------------
-- Written only at enqueue (QUEUED) and on the terminal transition, where the
-- final attempt_count/max_attempts are snapshotted from the hot row.
-- This reference script is shared across taskmesh-simple/-pro/-max (D31);
-- promoted_at/partition_key and partition_key's PK membership are
-- taskmesh-max ONLY — omit them for taskmesh-simple/-pro.
CREATE TABLE tm_task (
    task_type       VARCHAR(128) NOT NULL,
    task_id         VARCHAR(255) NOT NULL,
    status          VARCHAR(32)  NOT NULL,          -- QUEUED | COMPLETED | FAILED
    attempt_count   INTEGER      NOT NULL,
    max_attempts    INTEGER      NOT NULL,
    reason_code     VARCHAR(255),
    error_summary   TEXT,
    created_at      TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at      TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    promoted_at     TIMESTAMP(6) WITH TIME ZONE,    -- taskmesh-max only; NULL under -simple/-pro
    partition_key   VARCHAR(255) NOT NULL,          -- taskmesh-max only (D32); omit for -simple/-pro
    CONSTRAINT pk_tm_task PRIMARY KEY (task_type, task_id, partition_key)  -- -simple/-pro: PK is (task_type, task_id) only
);
CREATE INDEX idx_tm_task_type_status ON tm_task (task_type, status);
-- taskmesh-max only — keeps JpaBacklogPromoter's backlog scan cheap:
CREATE INDEX idx_tm_task_unpromoted ON tm_task (created_at) WHERE promoted_at IS NULL AND status = 'QUEUED';

-- ---- tm_in_flight (hot claim state) ----------------------------------
-- Sole source of truth for query/claim/release. Carries the live retry
-- counters and the backoff gate. Deleted at a terminal outcome.
CREATE TABLE tm_in_flight (
    task_type        VARCHAR(128) NOT NULL,
    task_id          VARCHAR(255) NOT NULL,
    partition_key    VARCHAR(255) NOT NULL,         -- taskmesh-max only (D32); plain column, NOT part of the PK — this table is never physically partitioned
    status           VARCHAR(32),                    -- QUEUED (reserved) | NULL (unreserved) | CLAIMED | RELEASED
    node_id          VARCHAR(255),
    attempt_count    INTEGER      NOT NULL,
    max_attempts     INTEGER      NOT NULL,
    next_retry_at    TIMESTAMP(6) WITH TIME ZONE,
    claimed_at       TIMESTAMP(6) WITH TIME ZONE,
    lease_expires_at TIMESTAMP(6) WITH TIME ZONE,
    created_at       TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    updated_at       TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_tm_in_flight PRIMARY KEY (task_type, task_id)
);
-- Discovery scans by task_type + status (+ updated_at/next_retry_at); ordered by created_at.
CREATE INDEX idx_tm_in_flight_discovery ON tm_in_flight (task_type, status, created_at);
-- Under heavy claim/renew churn, consider Postgres-specific tuning here in your
-- own DDL (e.g. fillfactor, autovacuum_vacuum_scale_factor) — deployment-specific.

-- ---- tm_history_event (append-only per-attempt log) ------------------
-- taskmesh-max only: id uses an explicit SEQUENCE, not GENERATED BY DEFAULT
-- AS IDENTITY — IDENTITY columns don't reliably stay globally unique/generate
-- correctly once partition_key joins the PK (Hibernate: "Identity generation
-- isn't supported for composite ids"). taskmesh-simple/-pro keep IDENTITY and
-- omit partition_key entirely (PK stays (id) alone).
CREATE SEQUENCE tm_history_event_id_seq;  -- taskmesh-max only
CREATE TABLE tm_history_event (
    id             BIGINT NOT NULL DEFAULT nextval('tm_history_event_id_seq'),  -- taskmesh-max only form; -simple/-pro: GENERATED BY DEFAULT AS IDENTITY
    partition_key  VARCHAR(255) NOT NULL,           -- taskmesh-max only (D32); omit for -simple/-pro
    task_type      VARCHAR(128) NOT NULL,
    task_id        VARCHAR(255) NOT NULL,
    attempt_number INTEGER      NOT NULL,
    event_type     VARCHAR(32)  NOT NULL,           -- STARTED | COMPLETED | FAILED | RETRIED | OWNERSHIP_LOST | PAUSED | HEARTBEAT
    detail         TEXT,
    occurred_at    TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_tm_history_event PRIMARY KEY (id, partition_key)  -- -simple/-pro: PK is (id) only
);
CREATE INDEX idx_tm_history_event_task ON tm_history_event (task_type, task_id);

-- ---- tm_attempt_history (BATCH profile — one row per attempt) ---------
-- Coarse alternative to tm_history_event, used by AttemptHistoryWriter.
CREATE TABLE tm_attempt_history (
    task_type      VARCHAR(128) NOT NULL,
    task_id        VARCHAR(255) NOT NULL,
    attempt_number INTEGER      NOT NULL,
    partition_key  VARCHAR(255) NOT NULL,           -- taskmesh-max only (D32); omit for -simple/-pro
    status         VARCHAR(32)  NOT NULL,           -- last HistoryEventType for the attempt
    started_at     TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    ended_at       TIMESTAMP(6) WITH TIME ZONE,
    node_id        VARCHAR(255),                     -- node that recorded the attempt's most recent event
    detail         TEXT,
    CONSTRAINT pk_tm_attempt_history PRIMARY KEY (task_type, task_id, attempt_number, partition_key)  -- -simple/-pro: omit partition_key
);

-- ---- tm_audit_event (operator/cluster actions only) ------------------
-- NOT touched by D32 — no partition_key here in any tier (deferred).
CREATE TABLE tm_audit_event (
    id           BIGINT GENERATED BY DEFAULT AS IDENTITY,
    event_type   VARCHAR(64)  NOT NULL,             -- e.g. PAUSE_REQUESTED, TASK_RERUN, INCIDENT_RESOLVED
    task_type    VARCHAR(128),                       -- nullable: cluster-wide events have no single task
    task_id      VARCHAR(255),
    actor_type   VARCHAR(32)  NOT NULL,             -- APPLICATION_USER | SYSTEM | ENGINE_MODULE
    performed_by VARCHAR(255),
    message      TEXT,
    occurred_at  TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_tm_audit_event PRIMARY KEY (id)
);

-- ---- tm_incident (operational "what's broken" worklist) --------------
-- taskmesh-max only: id uses an explicit SEQUENCE — same IDENTITY-vs-composite-key
-- reason as tm_history_event above.
CREATE SEQUENCE tm_incident_id_seq;  -- taskmesh-max only
CREATE TABLE tm_incident (
    id           BIGINT NOT NULL DEFAULT nextval('tm_incident_id_seq'),  -- taskmesh-max only form; -simple/-pro: GENERATED BY DEFAULT AS IDENTITY
    partition_key VARCHAR(255) NOT NULL,            -- taskmesh-max only (D32); omit for -simple/-pro
    task_type    VARCHAR(128) NOT NULL,
    task_id      VARCHAR(255) NOT NULL,
    reason_code  VARCHAR(64)  NOT NULL,
    message      TEXT,
    created_at   TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    resolved_at  TIMESTAMP(6) WITH TIME ZONE,
    resolved_by  VARCHAR(255),
    CONSTRAINT pk_tm_incident PRIMARY KEY (id, partition_key)  -- -simple/-pro: PK is (id) only
);
-- Keeps the "currently open incidents" query cheap regardless of resolved history.
CREATE INDEX idx_tm_incident_open ON tm_incident (task_type, task_id) WHERE resolved_at IS NULL;

-- ---- tm_control_flag (single-row cluster pause flag) -----------------
CREATE TABLE tm_control_flag (
    id              INTEGER NOT NULL,               -- always 1
    pause_requested BOOLEAN NOT NULL,
    CONSTRAINT pk_tm_control_flag PRIMARY KEY (id)
);
-- Seed the single row (JpaPauseController will also self-seed if absent).
INSERT INTO tm_control_flag (id, pause_requested) VALUES (1, false);
