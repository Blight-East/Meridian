from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text

from memory.structured.db import engine


def _json(value):
    return json.dumps({} if value is None else value)


def _parse_json(value, fallback):
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _row_to_dict(row):
    if not row:
        return None
    data = dict(row._mapping)
    for key in ("raw_model_output_json", "validated_output_json", "final_decision_json"):
        data[key] = _parse_json(data.get(key), {})
    if "created_at" in data and isinstance(data["created_at"], datetime):
        data["created_at"] = data["created_at"].isoformat()
    return data


def ensure_reasoning_tables():
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS reasoning_decision_log (
                    id BIGSERIAL PRIMARY KEY,
                    item_type TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    channel TEXT NOT NULL DEFAULT 'gmail',
                    task_type TEXT NOT NULL,
                    routing_decision TEXT NOT NULL,
                    deterministic_confidence DOUBLE PRECISION,
                    provider TEXT,
                    model TEXT,
                    raw_model_output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    validated_output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    final_decision_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    success BOOLEAN NOT NULL DEFAULT FALSE,
                    error TEXT,
                    fallback_used BOOLEAN NOT NULL DEFAULT FALSE,
                    latency_ms DOUBLE PRECISION,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                ALTER TABLE reasoning_decision_log
                ADD COLUMN IF NOT EXISTS fallback_used BOOLEAN NOT NULL DEFAULT FALSE
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS reasoning_decision_log_item_idx
                ON reasoning_decision_log (item_type, item_id, created_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS reasoning_decision_log_task_idx
                ON reasoning_decision_log (task_type, created_at DESC)
                """
            )
        )
        conn.commit()


def log_reasoning_decision(
    *,
    item_type: str,
    item_id: str,
    channel: str,
    task_type: str,
    routing_decision: str,
    deterministic_confidence: float | None,
    provider: str | None,
    model: str | None,
    raw_model_output,
    validated_output,
    final_decision,
    success: bool,
    error: str | None,
    fallback_used: bool = False,
    latency_ms: float | None,
) -> int:
    ensure_reasoning_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO reasoning_decision_log (
                    item_type, item_id, channel, task_type, routing_decision,
                    deterministic_confidence, provider, model,
                    raw_model_output_json, validated_output_json, final_decision_json,
                    success, error, fallback_used, latency_ms
                )
                VALUES (
                    :item_type, :item_id, :channel, :task_type, :routing_decision,
                    :deterministic_confidence, :provider, :model,
                    CAST(:raw_model_output AS JSONB),
                    CAST(:validated_output AS JSONB),
                    CAST(:final_decision AS JSONB),
                    :success, :error, :fallback_used, :latency_ms
                )
                RETURNING id
                """
            ),
            {
                "item_type": item_type,
                "item_id": str(item_id),
                "channel": channel,
                "task_type": task_type,
                "routing_decision": routing_decision,
                "deterministic_confidence": deterministic_confidence,
                "provider": provider,
                "model": model,
                "raw_model_output": _json(raw_model_output),
                "validated_output": _json(validated_output),
                "final_decision": _json(final_decision),
                "success": bool(success),
                "error": error,
                "fallback_used": bool(fallback_used),
                "latency_ms": latency_ms,
            },
        ).fetchone()
        conn.commit()
    return int(row[0])


def list_reasoning_decisions(*, limit: int = 25, task_type: str | None = None, item_id: str | None = None) -> list[dict]:
    ensure_reasoning_tables()
    where = []
    params = {"limit": int(limit)}
    if task_type:
        where.append("task_type = :task_type")
        params["task_type"] = task_type
    if item_id:
        where.append("item_id = :item_id")
        params["item_id"] = str(item_id)
    query = "SELECT * FROM reasoning_decision_log"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY id DESC LIMIT :limit"
    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_reasoning_decision(decision_id: int) -> dict | None:
    ensure_reasoning_tables()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM reasoning_decision_log WHERE id = :decision_id"),
            {"decision_id": int(decision_id)},
        ).fetchone()
    return _row_to_dict(row)


def get_reasoning_metrics_snapshot() -> dict:
    ensure_reasoning_tables()
    with engine.connect() as conn:
        tier1_calls = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE routing_decision = 'tier_1_cheap_model'
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        tier2_calls = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE routing_decision = 'tier_2_strong_model'
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        rule_only = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE routing_decision = 'tier_0_rules_only'
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        failures = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE success = FALSE
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        average_latency = conn.execute(
            text(
                """
                SELECT AVG(latency_ms)
                FROM reasoning_decision_log
                WHERE latency_ms IS NOT NULL
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar()
        last_error = conn.execute(
            text(
                """
                SELECT error
                FROM reasoning_decision_log
                WHERE error IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """
            )
        ).scalar() or ""
        last_error_at = conn.execute(
            text(
                """
                SELECT created_at
                FROM reasoning_decision_log
                WHERE error IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """
            )
        ).scalar()
        last_success_at = conn.execute(
            text(
                """
                SELECT created_at
                FROM reasoning_decision_log
                WHERE success = TRUE
                ORDER BY id DESC
                LIMIT 1
                """
            )
        ).scalar()
        budget_exceeded = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE error = 'budget_exceeded'
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        gmail_classification = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE task_type = 'gmail_thread_classification_refinement'
                  AND success = TRUE
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        merchant_identity = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE task_type = 'merchant_identity_refinement'
                  AND success = TRUE
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        opportunity_refinements = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE task_type = 'opportunity_scoring_refinement'
                  AND success = TRUE
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        draft_refinements = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE task_type = 'reply_draft_refinement'
                  AND success = TRUE
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        fallback_count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM reasoning_decision_log
                WHERE fallback_used = TRUE
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        provider_rows = conn.execute(
            text(
                """
                SELECT provider,
                       model,
                       COUNT(*) AS total_calls,
                       COUNT(*) FILTER (WHERE success = TRUE) AS success_count,
                       COUNT(*) FILTER (WHERE success = FALSE) AS failure_count,
                       COUNT(*) FILTER (WHERE fallback_used = TRUE) AS fallback_count,
                       AVG(latency_ms) AS avg_latency_ms
                FROM reasoning_decision_log
                WHERE provider IS NOT NULL
                  AND created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY provider, model
                ORDER BY total_calls DESC, provider, model
                """
            )
        ).fetchall()

    total_model_calls = tier1_calls + tier2_calls
    if last_error_at and (last_success_at is None or last_error_at > last_success_at):
        status = "degraded"
    else:
        status = "healthy"
    current_error = last_error if status == "degraded" else ""
    provider_performance = []
    for row in provider_rows:
        data = dict(row._mapping)
        total_calls = int(data["total_calls"] or 0)
        success_count = int(data["success_count"] or 0)
        failure_count = int(data["failure_count"] or 0)
        provider_performance.append(
            {
                "provider": data["provider"],
                "model": data["model"],
                "total_calls": total_calls,
                "success_count": success_count,
                "failure_count": failure_count,
                "fallback_count": int(data["fallback_count"] or 0),
                "success_rate": round(success_count / total_calls, 4) if total_calls else 0.0,
                "failure_rate": round(failure_count / total_calls, 4) if total_calls else 0.0,
                "fallback_rate": round(float(data["fallback_count"] or 0) / total_calls, 4) if total_calls else 0.0,
                "avg_latency_ms": round(float(data["avg_latency_ms"]), 2) if data["avg_latency_ms"] is not None else 0.0,
            }
        )

    return {
        "reasoning_status": status,
        "reasoning_tier1_calls_24h": tier1_calls,
        "reasoning_tier2_calls_24h": tier2_calls,
        "reasoning_rule_only_decisions_24h": rule_only,
        "reasoning_route_distribution_24h": {
            "tier_0_rules_only": rule_only,
            "tier_1_cheap_model": tier1_calls,
            "tier_2_strong_model": tier2_calls,
        },
        "reasoning_failures_24h": failures,
        "reasoning_fallbacks_24h": fallback_count,
        "reasoning_fallback_rate_24h": round(fallback_count / total_model_calls, 4) if total_model_calls else 0.0,
        "last_reasoning_error": current_error,
        "avg_reasoning_latency_ms": round(float(average_latency), 2) if average_latency is not None else 0.0,
        "reasoning_budget_exceeded_24h": budget_exceeded,
        "gmail_classification_refinements_24h": gmail_classification,
        "merchant_identity_refinements_24h": merchant_identity,
        "opportunity_refinements_24h": opportunity_refinements,
        "draft_refinements_24h": draft_refinements,
        "reasoning_provider_performance_24h": provider_performance,
    }
