-- db_hardening/roles.sql
-- Phase 4 — per-service Postgres roles for Agent Flux.
--
-- DO NOT APPLY BLIND.  Intended to be rolled out after:
--   1. Phase 0 is live (pg_timeouts.sql applied).
--   2. Phase 1 (schema_migrations.py) is in production.
--   3. A maintenance window exists to cycle DATABASE_URL per PM2
--      service (requires an ecosystem.config.js change to point each
--      service at its role-specific URL).
--
-- Roles
-- -----
--
--  af_app        — api / worker / telegram / mcp request-path DB user.
--                  Read-write to data tables.  NO DDL.
--                  statement_timeout = 30s, lock_timeout = 5s.
--
--  af_scheduler  — scheduler-only DB user.  Required to own DDL via
--                  the migration runner.  CREATE / ALTER permitted on
--                  schema `public`.  statement_timeout = 5min,
--                  lock_timeout = 60s.
--
--  af_readonly   — ad-hoc operator queries + future dashboards.
--                  Read-only on schema `public`.
--                  statement_timeout = 60s.
--
-- All three roles are non-superuser, non-replication.
--
-- Application credentials are NOT set here — generate random passwords
-- per environment and store them in Devin secrets + .env.
--
-- Rollback
-- --------
--
--   REASSIGN OWNED BY af_app        TO postgres;  DROP OWNED BY af_app;        DROP ROLE af_app;
--   REASSIGN OWNED BY af_scheduler  TO postgres;  DROP OWNED BY af_scheduler;  DROP ROLE af_scheduler;
--   REASSIGN OWNED BY af_readonly   TO postgres;  DROP OWNED BY af_readonly;   DROP ROLE af_readonly;
--
-- Apply as the `postgres` superuser against the `agent_flux` database.

-- Guard — refuse to run outside the agent_flux database.
SELECT CASE
    WHEN current_database() = 'agent_flux' THEN 1
    ELSE 1 / 0
END;

-- ---------------------------------------------------------------------------
-- af_app  (request-path: api / worker / telegram / mcp)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'af_app') THEN
        CREATE ROLE af_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
END$$;

ALTER ROLE af_app SET statement_timeout = '30s';
ALTER ROLE af_app SET lock_timeout = '5s';
ALTER ROLE af_app SET idle_in_transaction_session_timeout = '5min';

GRANT CONNECT ON DATABASE agent_flux TO af_app;
GRANT USAGE ON SCHEMA public TO af_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO af_app;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO af_app;
-- No DDL — intentionally omit CREATE on schema public.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO af_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO af_app;


-- ---------------------------------------------------------------------------
-- af_scheduler (scheduler only — owns DDL via migration runner)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'af_scheduler') THEN
        CREATE ROLE af_scheduler LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
END$$;

ALTER ROLE af_scheduler SET statement_timeout = '5min';
ALTER ROLE af_scheduler SET lock_timeout = '60s';
ALTER ROLE af_scheduler SET idle_in_transaction_session_timeout = '10min';

GRANT CONNECT ON DATABASE agent_flux TO af_scheduler;
GRANT USAGE, CREATE ON SCHEMA public TO af_scheduler;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO af_scheduler;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO af_scheduler;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO af_scheduler;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO af_scheduler;


-- ---------------------------------------------------------------------------
-- af_readonly (operator ad-hoc queries, future dashboards)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'af_readonly') THEN
        CREATE ROLE af_readonly LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
END$$;

ALTER ROLE af_readonly SET statement_timeout = '60s';
ALTER ROLE af_readonly SET lock_timeout = '5s';
ALTER ROLE af_readonly SET idle_in_transaction_session_timeout = '2min';

GRANT CONNECT ON DATABASE agent_flux TO af_readonly;
GRANT USAGE ON SCHEMA public TO af_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO af_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO af_readonly;

-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
SELECT rolname, rolcanlogin, rolconfig
FROM pg_roles
WHERE rolname IN ('af_app', 'af_scheduler', 'af_readonly')
ORDER BY rolname;
