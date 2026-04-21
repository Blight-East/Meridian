-- 007_recovery_enforcement.sql
-- Idempotent migration for the recovery enforcement layer.
-- Safe to re-run. Never drops or truncates data.

BEGIN;

-- 1. Dual-lock mode + feature flags
CREATE TABLE IF NOT EXISTS system_mode (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL,
    reason  TEXT,
    set_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    set_by  TEXT NOT NULL
);
INSERT INTO system_mode(key, value, reason, set_by) VALUES
    ('mode', 'recovery', 'migration_install', 'sre'),
    ('learning_enabled', 'false', 'default_off_until_validated', 'sre')
ON CONFLICT (key) DO NOTHING;

-- 2. Stripe webhook idempotency
CREATE TABLE IF NOT EXISTS processed_stripe_events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Canonical event identity columns + indices
ALTER TABLE events ADD COLUMN IF NOT EXISTS payload_signature TEXT;
CREATE INDEX IF NOT EXISTS events_event_type_sig_idx
    ON events (event_type, payload_signature, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS events_idem_key_uniq
    ON events ((data->>'idempotency_key'))
    WHERE data ? 'idempotency_key';

-- 4. Recovery window table (referenced by learning_guard and validation queries)
CREATE TABLE IF NOT EXISTS recovery_window (
    key   TEXT PRIMARY KEY,
    value TIMESTAMPTZ NOT NULL,
    note  TEXT
);

-- 5. Trust columns on deal_stage_transitions
ALTER TABLE deal_stage_transitions
    ADD COLUMN IF NOT EXISTS trust_status TEXT DEFAULT 'trusted';

-- Rename base table -> _raw, create filtered view.
-- Wrapped in DO block so it is idempotent (skips if already renamed).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'deal_stage_transitions'
          AND table_type = 'BASE TABLE'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'deal_stage_transitions_raw'
    ) THEN
        EXECUTE 'ALTER TABLE deal_stage_transitions RENAME TO deal_stage_transitions_raw';
        EXECUTE 'CREATE VIEW deal_stage_transitions AS
                 SELECT * FROM deal_stage_transitions_raw WHERE trust_status = ''trusted''';
        EXECUTE 'CREATE VIEW deal_stage_transitions_all AS
                 SELECT * FROM deal_stage_transitions_raw';
    END IF;
END $$;

-- 6. Trust column on learning_feedback_ledger
ALTER TABLE IF EXISTS learning_feedback_ledger
    ADD COLUMN IF NOT EXISTS trust_status TEXT DEFAULT 'trusted';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'learning_feedback_ledger'
          AND table_type = 'BASE TABLE'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'learning_feedback_ledger_raw'
    ) THEN
        EXECUTE 'ALTER TABLE learning_feedback_ledger RENAME TO learning_feedback_ledger_raw';
        EXECUTE 'CREATE VIEW learning_feedback_ledger AS
                 SELECT * FROM learning_feedback_ledger_raw
                 WHERE trust_status IN (''trusted'', ''recovered'')';
    END IF;
END $$;

-- 6b. View comments — warn writers that these names are filtered views, not tables.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.views WHERE table_name = 'deal_stage_transitions') THEN
        EXECUTE $cmt$COMMENT ON VIEW deal_stage_transitions IS
            'Filtered view (trust_status=trusted) over deal_stage_transitions_raw. WRITERS MUST TARGET deal_stage_transitions_raw — INSERT/UPDATE on this view will fail.'$cmt$;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.views WHERE table_name = 'learning_feedback_ledger') THEN
        EXECUTE $cmt$COMMENT ON VIEW learning_feedback_ledger IS
            'Filtered view (trust_status IN trusted/recovered) over learning_feedback_ledger_raw. WRITERS MUST TARGET learning_feedback_ledger_raw — INSERT/UPDATE on this view will fail.'$cmt$;
    END IF;
END $$;

-- 7. Task reconciliation table (idempotent)
CREATE TABLE IF NOT EXISTS task_reconciliation (
    task_event_id BIGINT PRIMARY KEY,
    task_payload  JSONB NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL,
    resolution    TEXT NOT NULL CHECK (resolution IN
                  ('executed', 'failed', 'lost', 'replay_pending', 'replayed')),
    evidence      JSONB
);

COMMIT;
