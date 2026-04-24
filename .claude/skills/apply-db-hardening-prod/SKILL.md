---
name: apply-db-hardening-prod
description: Apply the opt-in parts of b0eed03 to production — PG timeouts, migration runner in deploy, per-service roles. Use to finish the DB hardening rollout that's already in the tree.
phase: 0
status: draft
blast_radius: medium (prod DB config + deploy script flag)
parallelizable: true
---

# Apply DB hardening to prod

## Why

Commit [b0eed03](https://github.com/Blight-East/Meridian/commit/b0eed03) added
significant DB hardening — advisory-locked migration runner, session
watchdog, PG timeouts, per-service roles — but several parts are
**opt-in and not yet applied to prod**:

1. `db_hardening/pg_timeouts.sql` — cluster-level settings. Not
   applied. Without this, the idle-in-transaction incident (2026-04-22
   root cause) can recur.
2. `deploy.sh` has `MERIDIAN_RUN_MIGRATIONS_ON_DEPLOY=true` step — flag
   is **off by default**. Migration runner is only used ad-hoc.
3. `db_hardening/roles.sql` — per-service roles (`af_app`,
   `af_scheduler`, `af_readonly`) defined but not wired to
   `ecosystem.config.js`. Currently all services run as `postgres`
   superuser, which is the wrong shape for production.

This skill closes those three gaps.

## Blast radius

- **Prod DB config change** (pg_timeouts.sql) — requires `pg_reload_conf`.
  Non-destructive, but affects every connection.
- **`deploy.sh` flag flip** — future deploys run migrations automatically.
- **`ecosystem.config.js`** — service env vars switch to per-role
  credentials.
- Does not touch application code.

## Preconditions

1. Backup verified < 24 hours old. `pg_dump` or managed snapshot.
2. A maintenance window scheduled (15 minutes — most of this is
   reversible but connection-disruptive).
3. Existing `.env` values for `af_app`, `af_scheduler`, `af_readonly`
   passwords generated and stored in the password manager.

## Steps

1. **Apply pg_timeouts.sql.** SSH to prod, run:
   ```
   psql $DATABASE_URL -f db_hardening/pg_timeouts.sql
   SELECT pg_reload_conf();
   ```
   → **verify**: `SHOW idle_in_transaction_session_timeout` returns
   `10min`; `SHOW lock_timeout` returns `30s`.

2. **Apply schema_migrations bookkeeping table.** Run migration runner
   in `--apply` mode under the advisory lock:
   ```
   cd /opt/agent-flux
   python3 -m runtime.ops.schema_migrations --apply
   ```
   → **verify**: `SELECT count(*) FROM schema_migrations` returns rows
   corresponding to all migrations already applied to this DB (they
   should be pre-registered as historical).

3. **Apply roles.sql.** Run as superuser:
   ```
   psql $DATABASE_URL -f db_hardening/roles.sql
   ```
   Set passwords:
   ```sql
   ALTER ROLE af_app PASSWORD '...';
   ALTER ROLE af_scheduler PASSWORD '...';
   ALTER ROLE af_readonly PASSWORD '...';
   ```
   → **verify**: `\du` shows the three roles with the expected
   `statement_timeout`, `lock_timeout`,
   `idle_in_transaction_session_timeout` attributes (per
   db_hardening/roles.sql).

4. **Flip `deploy.sh` flag.**
   `MERIDIAN_RUN_MIGRATIONS_ON_DEPLOY=true` in the prod `.env`. The
   next deploy will run migrations under the advisory lock
   automatically.
   → **verify**: next deploy logs show
   `[schema_migrations] acquired advisory lock` and
   `[schema_migrations] applied N migrations`.

5. **Wire per-service roles in `ecosystem.config.js`.** Split the
   `DATABASE_URL` env var by service:
   - `agent-flux-api` → `af_app` creds
   - `agent-flux-worker` → `af_app` creds
   - `agent-flux-scheduler` → `af_scheduler` creds
   - `agentflux-telegram` → `af_app` creds (reads; writes via
     operator_commands)
   - `meridian-mcp` → `af_readonly` creds
   Reload: `pm2 reload ecosystem.config.js`.
   → **verify**: `pg_stat_activity` `usename` column shows the
   per-service roles during live traffic.

6. **Confirm db_session_watchdog is running.** It should be already
   (default `MERIDIAN_DB_WATCHDOG_ENABLED=true`), reporting every 60s.
   Check Redis: `HGETALL agent_flux:health:db_watchdog`.
   → **verify**: `classification` field updates every 60s; is
   `healthy` in steady state.

7. **Optional: turn on auto-terminate.** Once confident, set
   `MERIDIAN_DB_WATCHDOG_AUTO_TERMINATE=true` so idle-in-tx sessions
   beyond `IDLE_KILL_SECONDS` are killed automatically. Leave off for
   first 7 days to observe.
   → **verify**: when enabled, a forced long idle-in-tx session in
   staging is terminated by the watchdog within 60s.

## Rollback

Ordered reverse of steps:

- `ecosystem.config.js`: revert env vars to previous DATABASE_URL.
  `pm2 reload`.
- `deploy.sh` flag: `MERIDIAN_RUN_MIGRATIONS_ON_DEPLOY=false`.
- `roles.sql`: `DROP ROLE af_app; DROP ROLE af_scheduler; DROP ROLE
  af_readonly;` (only if no connections using them).
- `pg_timeouts.sql`: `ALTER SYSTEM RESET idle_in_transaction_session_timeout;
  ALTER SYSTEM RESET lock_timeout; ...; SELECT pg_reload_conf();`.
- `schema_migrations` table can stay; it's inert on revert.

## Exit criterion

- All three opt-in parts of b0eed03 live in prod.
- `pg_stat_activity` shows per-service role assignment during normal
  traffic.
- db_session_watchdog reports `healthy` for 7 consecutive days.
- No idle-in-tx session older than 10 minutes in `pg_stat_activity`
  snapshot at any time.
- `deploy.sh` automatically runs pending migrations under advisory lock
  on next deploy.

## Notes

- **Why wasn't this done in b0eed03 itself?** The PR was intentionally
  additive + flag-gated so nothing would break on merge. The actual
  prod application is a separate, operator-initiated step. That
  separation is correct — don't change it. This skill is the
  "operator-initiated step."
- **Per-service roles are the biggest win** for blast-radius
  containment. If an app connection is compromised, it can't drop
  tables; the scheduler can't read operator PII; the MCP is
  read-only. Worth the setup cost.
- **Connection pooling.** If prod uses pgbouncer, per-service roles
  imply per-service pools. Config change in pgbouncer.ini.
