# DB Hardening

Five-phase additive, feature-flagged upgrade of Agent Flux's Postgres
posture.  Every phase is independently reversible and no phase
requires downtime.

The upgrade exists because of the 2026-04-22 incident: a session left
`idle in transaction` for 19 hours blocked an `ALTER TABLE
merchant_opportunities ADD COLUMN retry_after` initiated by an
engineer ~01:27 UTC, which in turn blocked four stacked retries of a
separate `ALTER TABLE merchants ADD COLUMN urgency_score` during
deploy of PR #2.  Every existing safeguard (PM2 restart, schema
validator, `pool_pre_ping`, deadlock timeout) was blind to this class
of failure.

Coverage of the original audit findings:

| Audit finding | Addressed in |
|---|---|
| idle-in-transaction session timeout | Phase 0 |
| detection/alerting for long-running or idle transactions | Phase 0 (logs), Phase 2 (watchdog + alerts) |
| advisory locks / concurrent migration guard | Phase 1 |
| centralized migration runner / serialized DDL | Phase 1 + Phase 3 |
| `pg_stat_activity` observability | Phase 2 |
| PM2 / scheduler / worker safeguards against overlapping schema changes | Phase 1 (advisory locks) + Phase 4 (role split) |


## Phase 0 — cluster-level timeouts + log observability

File: [`db_hardening/pg_timeouts.sql`](../db_hardening/pg_timeouts.sql)

One-shot `ALTER SYSTEM SET` batch followed by `pg_reload_conf()`.  No
restart needed.  Sets:

- `idle_in_transaction_session_timeout = 10min`
- `lock_timeout = 30s`
- `log_lock_waits = on`
- `log_min_duration_statement = 500ms`
- `log_transaction_sample_rate = 0.01`
- `track_activity_query_size = 4096`

### Apply

```bash
scp db_hardening/pg_timeouts.sql root@159.65.45.64:/tmp/
ssh root@159.65.45.64 "sudo -u postgres psql -f /tmp/pg_timeouts.sql"
```

### Rollback

```sql
ALTER SYSTEM RESET idle_in_transaction_session_timeout;
ALTER SYSTEM RESET lock_timeout;
ALTER SYSTEM RESET log_lock_waits;
ALTER SYSTEM RESET log_min_duration_statement;
ALTER SYSTEM RESET log_transaction_sample_rate;
ALTER SYSTEM RESET track_activity_query_size;
SELECT pg_reload_conf();
```


## Phase 1 — advisory-lock-guarded migration runner

File: [`runtime/ops/schema_migrations.py`](../runtime/ops/schema_migrations.py)

Single source of truth for DDL.  Two affordances:

1. **Registry + runner.** Modules register DDL closures via
   `@register_migration("name_v1")`.  `run_pending()` acquires
   `pg_advisory_lock(MIGRATION_LOCK_KEY)` (fleet-wide), applies
   unrecorded migrations inside transactions with `SET LOCAL
   lock_timeout = 60s`, records them in the `schema_migrations` tracking
   table, and releases.
2. **`with_advisory_lock(name)`** context manager.  Wrap any legacy
   `_ensure_*_schema()` function without rewriting it.  Two sites are
   wrapped in this PR (`contact_discovery._ensure_contact_discovery_schema`
   and `opportunity_scoring._init_opportunity_score_column`); the
   remaining 26 are idempotent and can be migrated later.

### Feature flag

- `MERIDIAN_SCHEMA_MIGRATIONS_ENABLED` (default `true`).  When `false`,
  the runner and the advisory-lock helper are pass-throughs.  Provides
  a one-switch rollback.

### CLI

```bash
python3 -m runtime.ops.schema_migrations --list
python3 -m runtime.ops.schema_migrations --apply
```


## Phase 2 — DB session watchdog

File: [`runtime/ops/db_session_watchdog.py`](../runtime/ops/db_session_watchdog.py)

Runs every 60 s from the scheduler (see
`runtime/scheduler/cron_tasks.py`).  Queries `pg_stat_activity` for
idle-in-transaction sessions and lock waits, classifies overall status
(`healthy` / `attention` / `degraded` / `critical`), writes snapshots
to Redis, emits `save_event("db_session_watchdog_checked", ...)`, and
sends a deduped operator alert on transitions into `degraded` /
`critical`.

### Feature flags

- `MERIDIAN_DB_WATCHDOG_ENABLED` (default `true`) — visibility-only
  when auto-terminate is off.
- `MERIDIAN_DB_WATCHDOG_AUTO_TERMINATE` (default `false`) — when both
  flags are `true`, sessions idle-in-transaction longer than
  `MERIDIAN_DB_WATCHDOG_IDLE_KILL_SECONDS` (default `900`) are
  `pg_terminate_backend`'d.  Leave off until Phase 0 has been live for
  at least a week.

### Thresholds (all env-configurable)

- `MERIDIAN_DB_WATCHDOG_IDLE_WARN_SECONDS` (default `300`)
- `MERIDIAN_DB_WATCHDOG_IDLE_KILL_SECONDS` (default `900`)
- `MERIDIAN_DB_WATCHDOG_LOCK_WARN_SECONDS` (default `120`)


## Phase 3 — deploy.sh migration-runner invocation

File: [`deploy.sh`](../deploy.sh)

Before `pm2 start`, when `MERIDIAN_RUN_MIGRATIONS_ON_DEPLOY=true` is
set in the deploy shell, `deploy.sh` SSHes to the VPS and runs
`python3 -m runtime.ops.schema_migrations --apply` under the advisory
lock.  Default is OFF so current deploys are byte-compatible.

### Enable once confidence is built

```bash
MERIDIAN_RUN_MIGRATIONS_ON_DEPLOY=true ./deploy.sh
```


## Phase 4 — per-service Postgres roles

File: [`db_hardening/roles.sql`](../db_hardening/roles.sql)

Splits DB access into three non-superuser roles:

| Role | Used by | DDL? | statement_timeout | lock_timeout | idle_in_transaction_session_timeout |
|---|---|---|---|---|---|
| `af_app` | api / worker / telegram / mcp | no | 30 s | 5 s | 5 min |
| `af_scheduler` | scheduler (owns DDL via migration runner) | yes | 5 min | 60 s | 10 min |
| `af_readonly` | operator queries, future dashboards | no | 60 s | 5 s | 2 min |

### Applying — intentionally manual, not wired into this PR

1. Generate passwords for each role (one-liner: `openssl rand -base64 24`).
2. Apply `db_hardening/roles.sql` as `postgres` superuser.
3. Set passwords: `ALTER ROLE af_app WITH PASSWORD '...'` etc.
4. Add three new secrets: `DATABASE_URL_APP`, `DATABASE_URL_SCHEDULER`,
   `DATABASE_URL_READONLY`.
5. Update `ecosystem.config.js` so each PM2 service reads the right
   URL (out of scope for this PR — requires explicit operator approval
   before flipping).

### Rollback

See the comment block at the top of `db_hardening/roles.sql`.
