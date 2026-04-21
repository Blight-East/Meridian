import json
import datetime
import logging
import redis as _redis_lib
from sqlalchemy import create_engine, text

# ----------------------------------------------------------------------------
# IMPORTANT — DO NOT OPEN NETWORK CONNECTIONS AT MODULE LOAD HERE.
#
# Both `redis.Redis(...)` and `sqlalchemy.create_engine(...)` are LAZY by
# design: they construct client objects but do not open sockets until the
# first command / first .connect() call. That property is load-bearing —
# `runtime/main.py`, `runtime/worker.py`, and `runtime/scheduler/cron_tasks.py`
# all import this module AFTER the boot integrity gate
# (`runtime.safety.fingerprint_check`) has run, but other tooling may
# import it earlier. If you add a `pool.connect()`, an `engine.connect()`,
# a `r.ping()`, or any other call that forces a real socket open at
# module load, you risk:
#   1. Opening DB / Redis connections on hosts that should have been
#      crashed by the integrity gate (in enforce mode).
#   2. Slowing every module that imports this module by a network round
#      trip — including unit tests and `--help` shortcuts.
# If you need eager connectivity validation, add it as an explicit
# function (e.g. `assert_db_reachable()`) that callers invoke AFTER
# the boot gate has run. See deploy/fingerprint/TRUST_MODEL.md.
# ----------------------------------------------------------------------------

_reward_failure_logger = logging.getLogger("agent_flux.reward_hooks")
_reward_redis = _redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
_REWARD_FAILURE_KEY = "learning_reward_write_failures_24h"
_REWARD_FAILURE_TTL = 86400

DATABASE_URL = "postgresql://postgres@127.0.0.1/agent_flux"
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)


def _serialize(val):
    if isinstance(val, (datetime.datetime, datetime.date)):
        return val.isoformat()
    return val


def _row_to_dict(row):
    d = dict(row._mapping)
    return {k: _serialize(v) for k, v in d.items()}


def get_session():
    return engine.connect()


CRITICAL_TABLES = (
    "system_mode",
    "processed_stripe_events",
    "customers",
    "conversion_opportunities",
    "deal_stage_transitions_raw",
    "learning_feedback_ledger_raw",
    "events",
)


def verify_critical_tables() -> None:
    """Fail-fast at startup if any critical table or load-bearing column is missing.
    Run AFTER the boot integrity gate but BEFORE the FastAPI app starts serving."""
    with engine.connect() as conn:
        existing = {
            r[0] for r in conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public'"
            ))
        }
        missing = [t for t in CRITICAL_TABLES if t not in existing]
        if missing:
            raise RuntimeError(
                f"Critical tables missing: {missing}. "
                f"Run migrations 001-007 (deploy/migrations/)."
            )
        cols = {
            r[0] for r in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='events'"
            ))
        }
        if "payload_signature" not in cols:
            raise RuntimeError(
                "events.payload_signature column missing. "
                "Run migration 007_recovery_enforcement.sql."
            )


def save_event(event_type, data=None):
    with engine.connect() as c:
        c.execute(
            text("INSERT INTO events (event_type, data, created_at) VALUES (:t, :d, NOW())"),
            {"t": event_type, "d": json.dumps(data or {})},
        )
        c.commit()


def save_task(payload):
    with engine.connect() as c:
        c.execute(
            text("INSERT INTO tasks (payload, created_at) VALUES (:p, NOW())"),
            {"p": json.dumps(payload)},
        )
        c.commit()


def save_message(role, content, conversation_id=None):
    with engine.connect() as c:
        c.execute(
            text("INSERT INTO conversations (role, content, conversation_id, created_at) VALUES (:r, :c, :cid, NOW())"),
            {"r": role, "c": content, "cid": conversation_id},
        )
        c.commit()


def load_conversation(conversation_id=None, limit=20):
    with engine.connect() as c:
        if conversation_id:
            rows = c.execute(
                text("SELECT role, content, created_at FROM conversations WHERE conversation_id = :cid ORDER BY created_at DESC LIMIT :l"),
                {"cid": conversation_id, "l": limit},
            ).fetchall()
        else:
            rows = c.execute(
                text("SELECT role, content, created_at FROM conversations ORDER BY created_at DESC LIMIT :l"),
                {"l": limit},
            ).fetchall()
        return [_row_to_dict(r) for r in reversed(rows)]


def get_all_tasks():
    with engine.connect() as c:
        rows = c.execute(text("SELECT * FROM tasks ORDER BY created_at DESC LIMIT 50")).fetchall()
        return [_row_to_dict(r) for r in rows]


def get_all_signals():
    with engine.connect() as c:
        rows = c.execute(text("SELECT * FROM signals ORDER BY detected_at DESC LIMIT 50")).fetchall()
        return [_row_to_dict(r) for r in rows]


def get_all_events():
    with engine.connect() as c:
        rows = c.execute(text("SELECT * FROM events ORDER BY created_at DESC LIMIT 50")).fetchall()
        return [_row_to_dict(r) for r in rows]


def _increment_reward_failure_counter():
    """Increment the 24h reward-write failure counter in Redis."""
    try:
        pipe = _reward_redis.pipeline()
        pipe.incr(_REWARD_FAILURE_KEY)
        pipe.expire(_REWARD_FAILURE_KEY, _REWARD_FAILURE_TTL)
        pipe.execute()
    except Exception:
        pass  # Redis counter is best-effort; the structured log is the primary signal


def get_reward_write_failures_24h() -> int:
    """Read the 24h reward-write failure count from Redis."""
    try:
        return int(_reward_redis.get(_REWARD_FAILURE_KEY) or 0)
    except Exception:
        return 0


def _learning_disabled() -> bool:
    """True if system_mode.learning_enabled is set to a falsy value.
    Returns False on any read failure (pre-migration / outage) to avoid
    silently dropping writes during transient issues.
    """
    try:
        with engine.connect() as _c:
            _flag = _c.execute(
                text("SELECT value FROM system_mode WHERE key = 'learning_enabled'")
            ).scalar()
        if _flag and str(_flag).strip().lower() in ("false", "0", "off", "no"):
            return True
    except Exception:
        pass
    return False


def save_learning_feedback(data: dict, hook_name: str = "unknown"):
    """
    Append a row to the learning_feedback_ledger safely.
    On failure: logs a structured error, increments Redis failure counter, and re-raises.
    Gated by system_mode.learning_enabled — suppressed during recovery.
    """
    if _learning_disabled():
        _reward_failure_logger.info(
            "Learning suppressed | hook=%s | signal_id=%s | opportunity_id=%s",
            hook_name, data.get("signal_id"), data.get("opportunity_id"),
        )
        return
    fields = [
        "signal_id", "source", "merchant_id_candidate", "merchant_domain_candidate",
        "opportunity_id", "contact_id", "decision_type", "decision_payload_json",
        "decision_score", "decision_model_version", "outcome_type", "outcome_value",
        "outcome_confidence", "outcome_at", "reward_score", "notes"
    ]
    vals = {f: data.get(f) for f in fields}
    
    # Handle JSON serialization for decision_payload_json if it's a dict
    if isinstance(vals.get("decision_payload_json"), dict):
        vals["decision_payload_json"] = json.dumps(vals["decision_payload_json"])

    # Ensure outcome_at is serialized if it's a datetime
    if isinstance(vals.get("outcome_at"), (datetime.datetime, datetime.date)):
        vals["outcome_at"] = vals["outcome_at"].isoformat()

    cols = ", ".join(vals.keys())
    placeholders = ", ".join([f":{f}" for f in vals.keys()])
    
    try:
        with engine.connect() as c:
            c.execute(
                text(f"INSERT INTO learning_feedback_ledger_raw ({cols}) VALUES ({placeholders})"),
                vals
            )
            c.commit()
    except Exception as exc:
        _increment_reward_failure_counter()
        _reward_failure_logger.error(
            "Reward write failed | "
            f"hook={hook_name} | "
            f"signal_id={data.get('signal_id')} | "
            f"merchant_id={data.get('merchant_id_candidate')} | "
            f"opportunity_id={data.get('opportunity_id')} | "
            f"outcome_type={data.get('outcome_type')} | "
            f"exc_class={type(exc).__name__} | "
            f"exc_msg={exc}"
        )
        raise


def record_outcome(
    *,
    opportunity_id: int | None = None,
    signal_id: int | None = None,
    decision_type: str | None = None,
    outcome_type: str,
    outcome_value: float,
    reward_score: float,
    outcome_confidence: float | None = None,
    notes: str | None = None,
    hook_name: str = "unknown",
) -> int:
    """Update the most recent matching decision row in learning_feedback_ledger_raw
    with outcome data. Idempotent: only fills rows where outcome_at IS NULL, so
    re-firing the same outcome event won't double-write.

    Returns the count of rows updated (0 if no matching open decision row found —
    logged but not raised, since outcomes can fire before decisions in race cases).

    At least one of opportunity_id, signal_id, or decision_type must be supplied
    to constrain the match.
    """
    if _learning_disabled():
        _reward_failure_logger.info(
            "Outcome write suppressed | hook=%s | opportunity_id=%s | signal_id=%s | outcome_type=%s",
            hook_name, opportunity_id, signal_id, outcome_type,
        )
        return 0

    where_clauses = ["outcome_at IS NULL"]
    params: dict = {
        "outcome_type": outcome_type,
        "outcome_value": outcome_value,
        "outcome_confidence": outcome_confidence,
        "reward_score": reward_score,
        "notes": notes,
    }
    if opportunity_id is not None:
        where_clauses.append("opportunity_id = :opportunity_id")
        params["opportunity_id"] = opportunity_id
    if signal_id is not None:
        where_clauses.append("signal_id = :signal_id")
        params["signal_id"] = signal_id
    if decision_type is not None:
        where_clauses.append("decision_type = :decision_type")
        params["decision_type"] = decision_type

    if len(where_clauses) == 1:
        raise ValueError("record_outcome requires at least one of: opportunity_id, signal_id, decision_type")

    where_sql = " AND ".join(where_clauses)
    sql = f"""
        UPDATE learning_feedback_ledger_raw
           SET outcome_type = :outcome_type,
               outcome_value = :outcome_value,
               outcome_confidence = :outcome_confidence,
               outcome_at = NOW(),
               reward_score = :reward_score,
               notes = COALESCE(notes || ' | ', '') || COALESCE(:notes, '')
         WHERE id = (
             SELECT id FROM learning_feedback_ledger_raw
              WHERE {where_sql}
              ORDER BY created_at DESC
              LIMIT 1
         )
    """
    try:
        with engine.connect() as c:
            result = c.execute(text(sql), params)
            c.commit()
            rowcount = result.rowcount or 0
    except Exception as exc:
        _increment_reward_failure_counter()
        _reward_failure_logger.error(
            "Outcome write failed | hook=%s | opportunity_id=%s | signal_id=%s | "
            "outcome_type=%s | exc_class=%s | exc_msg=%s",
            hook_name, opportunity_id, signal_id, outcome_type,
            type(exc).__name__, exc,
        )
        raise

    if rowcount == 0:
        _reward_failure_logger.info(
            "record_outcome: no matching open decision row | hook=%s | "
            "opportunity_id=%s | signal_id=%s | decision_type=%s | outcome_type=%s",
            hook_name, opportunity_id, signal_id, decision_type, outcome_type,
        )
    return rowcount


def ensure_learning_ledger_exists():
    """Ensure the learning_feedback_ledger table exists."""
    with engine.connect() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS learning_feedback_ledger_raw (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                signal_id INTEGER,
                source TEXT,
                merchant_id_candidate INTEGER,
                merchant_domain_candidate TEXT,
                opportunity_id INTEGER,
                contact_id INTEGER,
                decision_type TEXT,
                decision_payload_json JSONB,
                decision_score FLOAT,
                decision_model_version TEXT,
                outcome_type TEXT,
                outcome_value FLOAT,
                outcome_confidence FLOAT,
                outcome_at TIMESTAMP WITH TIME ZONE,
                reward_score FLOAT,
                notes TEXT
            );
        """))
        # Using a separate block for indices to avoid issues if they already exist (though CREATE INDEX IF NOT EXISTS is safe)
        c.execute(text("CREATE INDEX IF NOT EXISTS idx_lfl_signal_id ON learning_feedback_ledger_raw(signal_id);"))
        c.execute(text("CREATE INDEX IF NOT EXISTS idx_lfl_merchant_candidate ON learning_feedback_ledger_raw(merchant_id_candidate);"))
        c.execute(text("CREATE INDEX IF NOT EXISTS idx_lfl_decision_type ON learning_feedback_ledger_raw(decision_type);"))
        c.execute(text("CREATE INDEX IF NOT EXISTS idx_lfl_outcome_type ON learning_feedback_ledger_raw(outcome_type);"))
        c.commit()


# ── Canonical idempotent event sink (added by release 007) ─────────────────

def save_event_canonical(event_type, payload, *,
                         idempotency_key=None, source_system=None,
                         subsystem="default"):
    """Idempotent insert into events. Requires migration 007 (unique index on
    (data->>'idempotency_key')). Returns {'inserted': bool, 'idempotency_key': str}.
    """
    from runtime.safety.write_guard import assert_write_allowed
    from runtime.core.canonical_event import build_event
    import json as _json
    assert_write_allowed(subsystem)
    ev = build_event(event_type=event_type, payload=payload,
                     idempotency_key=idempotency_key,
                     source_system=source_system)
    with engine.connect() as c:
        row = c.execute(
            text("""
                INSERT INTO events (event_type, data, payload_signature, created_at)
                VALUES (:t, CAST(:d AS JSONB), :s, NOW())
                ON CONFLICT ((data->>'idempotency_key'))
                  WHERE data ? 'idempotency_key'
                DO NOTHING
                RETURNING id
            """),
            {"t": ev["event_type"], "d": _json.dumps(ev["data"]),
             "s": ev["payload_signature"]},
        ).fetchone()
        c.commit()
    return {"inserted": row is not None,
            "idempotency_key": ev["idempotency_key"]}
