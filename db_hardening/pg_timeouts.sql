-- db_hardening/pg_timeouts.sql
-- Phase 0 — cluster-level Postgres hardening for Agent Flux.
--
-- Idempotent.  Every statement below is an ALTER SYSTEM SET with a concrete
-- value, followed by a single `pg_reload_conf()`.  Rollback is
-- `ALTER SYSTEM RESET <name>; SELECT pg_reload_conf();` for each line.
--
-- Apply once per cluster as the `postgres` superuser:
--   sudo -u postgres psql -f db_hardening/pg_timeouts.sql
--
-- Nothing below requires a Postgres restart.

-- ---------------------------------------------------------------------------
-- Session-level timeouts
-- ---------------------------------------------------------------------------

-- 10 min hard cap on any session holding a transaction open without activity.
-- Would have auto-terminated the 19-hour stale session observed on
-- 2026-04-22 that wedged every ALTER on merchant_opportunities.
ALTER SYSTEM SET idle_in_transaction_session_timeout = '10min';

-- 30 s bounded wait for lock acquisition at the cluster default.  A blocked
-- ALTER / SELECT waiting behind a lock hog now errors loudly instead of
-- queueing forever.  Long-running transactions that legitimately need more
-- should SET LOCAL lock_timeout inline.
ALTER SYSTEM SET lock_timeout = '30s';

-- statement_timeout stays 0 at the cluster level — scheduled
-- reports/backfills can legitimately run for minutes.  Per-query caps
-- already exist in runtime/main.py, operator_briefings.py, chat_engine.py,
-- operator_commands.py.  A future Phase 4 will set a per-role default on
-- the `af_app` role so request-path queries get a 30 s ceiling without
-- killing reports.

-- ---------------------------------------------------------------------------
-- Observability — make lock / slow-query events visible in the PG log
-- without requiring an exporter.
-- ---------------------------------------------------------------------------

-- Emit a log line whenever a session waits longer than `deadlock_timeout`
-- (default 1 s) for a lock.  This is the cheapest possible
-- lock-contention alarm.
ALTER SYSTEM SET log_lock_waits = 'on';

-- Any statement >500 ms gets logged.  Tune up if log volume is an issue.
ALTER SYSTEM SET log_min_duration_statement = '500ms';

-- Sample 1 % of transactions end-to-end.  Catches patterns without
-- blowing up log volume.
ALTER SYSTEM SET log_transaction_sample_rate = 0.01;

-- Capture the full statement (up to 4 KiB) in pg_stat_activity so the
-- watchdog in Phase 2 can classify what a stuck session was doing.
ALTER SYSTEM SET track_activity_query_size = 4096;

-- Apply all of the above without a restart.
SELECT pg_reload_conf();

-- ---------------------------------------------------------------------------
-- Verification — run after reload.
-- ---------------------------------------------------------------------------
SELECT name, setting, unit
FROM pg_settings
WHERE name IN (
    'idle_in_transaction_session_timeout',
    'lock_timeout',
    'statement_timeout',
    'log_lock_waits',
    'log_min_duration_statement',
    'log_transaction_sample_rate',
    'track_activity_query_size'
)
ORDER BY name;
