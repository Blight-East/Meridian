"""
Deal Lifecycle — Unified Deal State Machine
=============================================
Single source of truth for where any deal sits in the sales lifecycle.

Replaces the scattered state assembly across merchant_opportunities,
opportunity_outreach_actions, merchant_contacts, signals, and
opportunity_operator_actions with one table and enforced transitions.

Usage:
    from runtime.ops.deal_lifecycle import (
        DealStage, transition_deal, get_deal,
        ensure_deal_lifecycle_table, backfill_deal_lifecycle,
    )
"""
from __future__ import annotations

import enum
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from memory.structured.db import engine, save_event, record_outcome
from runtime.health.telemetry import record_component_state, utc_now_iso

logger = logging.getLogger("meridian.deal_lifecycle")


# ── Deal Stages ──────────────────────────────────────────────────────────────

class DealStage(str, enum.Enum):
    """Canonical lifecycle stages for a deal."""
    signal_detected = "signal_detected"
    lead_qualified = "lead_qualified"
    opportunity_created = "opportunity_created"
    outreach_drafted = "outreach_drafted"
    outreach_approved = "outreach_approved"
    outreach_sent = "outreach_sent"
    reply_received = "reply_received"
    follow_up_due = "follow_up_due"
    outcome_won = "outcome_won"
    outcome_lost = "outcome_lost"
    outcome_ignored = "outcome_ignored"


class InvalidTransitionError(Exception):
    """Raised when a stage transition violates the lifecycle rules."""
    pass


# ── Allowed Transitions ─────────────────────────────────────────────────────

ALLOWED_TRANSITIONS: dict[DealStage, set[DealStage]] = {
    DealStage.signal_detected: {
        DealStage.lead_qualified,
        DealStage.outcome_ignored,      # suppress junk signals
    },
    DealStage.lead_qualified: {
        DealStage.opportunity_created,
        DealStage.outcome_ignored,
    },
    DealStage.opportunity_created: {
        DealStage.outreach_drafted,
        DealStage.outcome_ignored,
        DealStage.outcome_lost,
    },
    DealStage.outreach_drafted: {
        DealStage.outreach_approved,
        DealStage.outreach_drafted,     # rewrite cycle
        DealStage.outcome_ignored,
        DealStage.outcome_lost,
    },
    DealStage.outreach_approved: {
        DealStage.outreach_sent,
        DealStage.outreach_drafted,     # revoke approval for rewrite
        DealStage.outcome_ignored,
        DealStage.outcome_lost,
    },
    DealStage.outreach_sent: {
        DealStage.reply_received,
        DealStage.follow_up_due,
        DealStage.outcome_won,
        DealStage.outcome_lost,
        DealStage.outcome_ignored,
    },
    DealStage.reply_received: {
        DealStage.outreach_drafted,     # reply-follow-up draft
        DealStage.outreach_sent,        # reply-follow-up sent
        DealStage.follow_up_due,
        DealStage.outcome_won,
        DealStage.outcome_lost,
        DealStage.outcome_ignored,
    },
    DealStage.follow_up_due: {
        DealStage.outreach_drafted,     # draft follow-up
        DealStage.outreach_sent,        # send follow-up directly
        DealStage.reply_received,
        DealStage.outcome_won,
        DealStage.outcome_lost,
        DealStage.outcome_ignored,
    },
    # Terminal stages — no further transitions
    DealStage.outcome_won: set(),
    DealStage.outcome_lost: set(),
    DealStage.outcome_ignored: set(),
}

# ── Stage Urgency Weights (used in priority scoring) ─────────────────────────

_STAGE_URGENCY: dict[DealStage, int] = {
    DealStage.reply_received: 100,      # hottest — merchant replied
    DealStage.follow_up_due: 90,        # time-sensitive
    DealStage.outreach_approved: 80,    # ready to send, blocked on operator
    DealStage.outreach_drafted: 60,     # needs review
    DealStage.outreach_sent: 40,        # waiting
    DealStage.opportunity_created: 30,  # needs drafting
    DealStage.lead_qualified: 20,       # needs review
    DealStage.signal_detected: 10,      # raw signal
    DealStage.outcome_won: 0,
    DealStage.outcome_lost: 0,
    DealStage.outcome_ignored: 0,
}

_DISTRESS_SEVERITY: dict[str, int] = {
    "account_terminated": 25,
    "account_frozen": 20,
    "payouts_delayed": 18,
    "reserve_hold": 15,
    "chargeback_issue": 10,
    "verification_review": 8,
    "processor_switch_intent": 5,
    "onboarding_rejected": 5,
    "unknown": 0,
}


# ── Table Management ─────────────────────────────────────────────────────────

def ensure_deal_lifecycle_table() -> None:
    """Create the deal_lifecycle table and indices if they don't exist."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS deal_lifecycle (
                id BIGSERIAL PRIMARY KEY,
                opportunity_id BIGINT UNIQUE,
                merchant_id BIGINT,
                merchant_domain TEXT NOT NULL DEFAULT '',
                signal_id BIGINT,
                current_stage TEXT NOT NULL DEFAULT 'signal_detected',
                previous_stage TEXT,
                stage_entered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                stage_actor TEXT DEFAULT 'system',
                processor TEXT DEFAULT 'unknown',
                distress_type TEXT DEFAULT 'unknown',
                contact_email TEXT DEFAULT '',
                contact_trust_score INT DEFAULT 0,
                outreach_status TEXT DEFAULT 'no_outreach',
                gmail_thread_id TEXT DEFAULT '',
                outcome_status TEXT DEFAULT 'pending',
                icp_fit_score INT DEFAULT 0,
                commercial_readiness_score INT DEFAULT 0,
                priority_score FLOAT DEFAULT 0.0,
                metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS deal_lifecycle_stage_priority_idx
            ON deal_lifecycle (current_stage, priority_score DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS deal_lifecycle_domain_idx
            ON deal_lifecycle (merchant_domain)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS deal_lifecycle_updated_idx
            ON deal_lifecycle (updated_at DESC)
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS deal_stage_transitions_raw (
                id BIGSERIAL PRIMARY KEY,
                opportunity_id BIGINT NOT NULL,
                from_stage TEXT,
                to_stage TEXT NOT NULL,
                actor TEXT DEFAULT 'system',
                metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                transitioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                trust_status TEXT DEFAULT 'trusted'
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS deal_stage_transitions_opp_idx
            ON deal_stage_transitions_raw (opportunity_id, transitioned_at DESC)
        """))
        conn.commit()
    logger.info("deal_lifecycle and deal_stage_transitions tables ensured")


# ── Core State Machine ───────────────────────────────────────────────────────

def transition_deal(
    opportunity_id: int,
    new_stage: DealStage | str,
    *,
    actor: str = "system",
    metadata: dict[str, Any] | None = None,
    update_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Advance a deal to a new stage, enforcing transition rules.

    Args:
        opportunity_id: The opportunity ID to transition.
        new_stage: Target stage (DealStage enum or string).
        actor: Who/what triggered this transition.
        metadata: Optional metadata to merge into the deal.
        update_fields: Optional dict of denormalized fields to update
                       (e.g., contact_email, gmail_thread_id).

    Returns:
        The updated deal record dict.

    Raises:
        InvalidTransitionError: If the transition is not allowed.
        ValueError: If the deal doesn't exist.
    """
    if isinstance(new_stage, str):
        try:
            new_stage = DealStage(new_stage)
        except ValueError:
            raise InvalidTransitionError(f"Unknown stage: {new_stage}")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM deal_lifecycle WHERE opportunity_id = :opp_id LIMIT 1"),
            {"opp_id": int(opportunity_id)},
        ).mappings().first()

        if not row:
            raise ValueError(f"No deal found for opportunity_id={opportunity_id}")

        current = dict(row)
        current_stage_str = current.get("current_stage") or "signal_detected"
        try:
            current_stage = DealStage(current_stage_str)
        except ValueError:
            current_stage = DealStage.signal_detected

        # Enforce transition rules
        allowed = ALLOWED_TRANSITIONS.get(current_stage, set())
        if new_stage not in allowed:
            raise InvalidTransitionError(
                f"Cannot transition deal {opportunity_id} from "
                f"{current_stage.value} → {new_stage.value}. "
                f"Allowed: {', '.join(s.value for s in allowed) or 'none (terminal stage)'}"
            )

        # Build the update
        merged_metadata = _json_dict(current.get("metadata_json"))
        if metadata:
            merged_metadata.update(metadata)

        now = datetime.now(timezone.utc)
        set_clauses = [
            "current_stage = :new_stage",
            "previous_stage = :previous_stage",
            "stage_entered_at = :now",
            "stage_actor = :actor",
            "metadata_json = :metadata",
            "updated_at = :now",
        ]
        params: dict[str, Any] = {
            "opp_id": int(opportunity_id),
            "new_stage": new_stage.value,
            "previous_stage": current_stage.value,
            "now": now,
            "actor": actor or "system",
            "metadata": json.dumps(merged_metadata, default=str),
        }

        # Apply optional field updates
        if update_fields:
            for field_name, field_value in update_fields.items():
                if field_name in _UPDATABLE_FIELDS:
                    set_clauses.append(f"{field_name} = :{field_name}")
                    params[field_name] = field_value

        # Recompute priority score
        priority = _compute_priority(
            stage=new_stage,
            icp_fit=int(update_fields.get("icp_fit_score", current.get("icp_fit_score", 0)) if update_fields else current.get("icp_fit_score", 0)),
            commercial=int(update_fields.get("commercial_readiness_score", current.get("commercial_readiness_score", 0)) if update_fields else current.get("commercial_readiness_score", 0)),
            contact_trust=int(update_fields.get("contact_trust_score", current.get("contact_trust_score", 0)) if update_fields else current.get("contact_trust_score", 0)),
            distress_type=str(update_fields.get("distress_type", current.get("distress_type", "unknown")) if update_fields else current.get("distress_type", "unknown")),
            merchant_domain=str(update_fields.get("merchant_domain", current.get("merchant_domain", "")) if update_fields else current.get("merchant_domain", "")),
            processor=str(update_fields.get("processor", current.get("processor", "unknown")) if update_fields else current.get("processor", "unknown")),
        )
        set_clauses.append("priority_score = :priority_score")
        params["priority_score"] = priority

        # Map outcome stages to outcome_status
        if new_stage in {DealStage.outcome_won, DealStage.outcome_lost, DealStage.outcome_ignored}:
            outcome_map = {
                DealStage.outcome_won: "won",
                DealStage.outcome_lost: "lost",
                DealStage.outcome_ignored: "ignored",
            }
            set_clauses.append("outcome_status = :outcome_status")
            params["outcome_status"] = outcome_map[new_stage]

        query = f"UPDATE deal_lifecycle SET {', '.join(set_clauses)} WHERE opportunity_id = :opp_id"
        conn.execute(text(query), params)

        # Record the transition in the audit trail
        conn.execute(
            text("""
                INSERT INTO deal_stage_transitions_raw
                    (opportunity_id, from_stage, to_stage, actor, metadata_json, transitioned_at)
                VALUES (:opp_id, :from_stage, :to_stage, :actor, :meta, :now)
            """),
            {
                "opp_id": int(opportunity_id),
                "from_stage": current_stage.value,
                "to_stage": new_stage.value,
                "actor": actor or "system",
                "meta": json.dumps(metadata or {}, default=str),
                "now": now,
            },
        )

        conn.commit()

        # Re-read the updated deal
        updated = conn.execute(
            text("SELECT * FROM deal_lifecycle WHERE opportunity_id = :opp_id LIMIT 1"),
            {"opp_id": int(opportunity_id)},
        ).mappings().first()
        result = dict(updated) if updated else {}

    # Record the event
    save_event("deal_stage_transition", {
        "opportunity_id": int(opportunity_id),
        "from_stage": current_stage.value,
        "to_stage": new_stage.value,
        "actor": actor or "system",
        "priority_score": priority,
    })

    # Close the learning loop on terminal stages.
    _TERMINAL_REWARDS = {
        DealStage.outcome_won:     ("deal_won",     1.0),
        DealStage.outcome_lost:    ("deal_lost",   -0.5),
        DealStage.outcome_ignored: ("deal_ignored", -0.1),
    }
    if new_stage in _TERMINAL_REWARDS:
        outcome_type, reward = _TERMINAL_REWARDS[new_stage]
        try:
            record_outcome(
                opportunity_id=int(opportunity_id),
                outcome_type=outcome_type,
                outcome_value=reward,
                outcome_confidence=1.0,
                reward_score=reward,
                notes=f"transition {current_stage.value}->{new_stage.value} actor={actor or 'system'}",
                hook_name="deal_lifecycle_transition",
            )
        except Exception as outcome_err:
            logger.warning("record_outcome (deal terminal) failed: %s", outcome_err)

    logger.info(
        "deal_transition: opp=%d %s→%s actor=%s priority=%.1f",
        opportunity_id, current_stage.value, new_stage.value, actor, priority,
    )

    return result


def create_deal(
    *,
    opportunity_id: int,
    merchant_id: int | None = None,
    merchant_domain: str = "",
    signal_id: int | None = None,
    initial_stage: DealStage = DealStage.signal_detected,
    actor: str = "system",
    processor: str = "unknown",
    distress_type: str = "unknown",
    contact_email: str = "",
    contact_trust_score: int = 0,
    icp_fit_score: int = 0,
    commercial_readiness_score: int = 0,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a new deal in the lifecycle table. Idempotent via ON CONFLICT."""
    priority = _compute_priority(
        stage=initial_stage,
        icp_fit=icp_fit_score,
        commercial=commercial_readiness_score,
        contact_trust=contact_trust_score,
        distress_type=distress_type,
        merchant_domain=merchant_domain,
        processor=processor,
    )
    now = datetime.now(timezone.utc)
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO deal_lifecycle (
                    opportunity_id, merchant_id, merchant_domain, signal_id,
                    current_stage, stage_entered_at, stage_actor,
                    processor, distress_type, contact_email, contact_trust_score,
                    icp_fit_score, commercial_readiness_score, priority_score,
                    metadata_json, created_at, updated_at
                ) VALUES (
                    :opportunity_id, :merchant_id, :merchant_domain, :signal_id,
                    :stage, :now, :actor,
                    :processor, :distress_type, :contact_email, :contact_trust_score,
                    :icp_fit_score, :commercial_readiness_score, :priority_score,
                    :metadata, :now, :now
                )
                ON CONFLICT (opportunity_id) DO UPDATE SET
                    merchant_domain = COALESCE(NULLIF(EXCLUDED.merchant_domain, ''), deal_lifecycle.merchant_domain),
                    processor = CASE WHEN EXCLUDED.processor != 'unknown' THEN EXCLUDED.processor ELSE deal_lifecycle.processor END,
                    distress_type = CASE WHEN EXCLUDED.distress_type != 'unknown' THEN EXCLUDED.distress_type ELSE deal_lifecycle.distress_type END,
                    contact_email = COALESCE(NULLIF(EXCLUDED.contact_email, ''), deal_lifecycle.contact_email),
                    contact_trust_score = GREATEST(EXCLUDED.contact_trust_score, deal_lifecycle.contact_trust_score),
                    icp_fit_score = GREATEST(EXCLUDED.icp_fit_score, deal_lifecycle.icp_fit_score),
                    commercial_readiness_score = GREATEST(EXCLUDED.commercial_readiness_score, deal_lifecycle.commercial_readiness_score),
                    priority_score = GREATEST(EXCLUDED.priority_score, deal_lifecycle.priority_score),
                    updated_at = NOW()
            """),
            {
                "opportunity_id": int(opportunity_id),
                "merchant_id": int(merchant_id) if merchant_id else None,
                "merchant_domain": merchant_domain or "",
                "signal_id": int(signal_id) if signal_id else None,
                "stage": initial_stage.value,
                "now": now,
                "actor": actor,
                "processor": processor or "unknown",
                "distress_type": distress_type or "unknown",
                "contact_email": contact_email or "",
                "contact_trust_score": int(contact_trust_score),
                "icp_fit_score": int(icp_fit_score),
                "commercial_readiness_score": int(commercial_readiness_score),
                "priority_score": priority,
                "metadata": json.dumps(metadata or {}, default=str),
            },
        )
        conn.commit()

        row = conn.execute(
            text("SELECT * FROM deal_lifecycle WHERE opportunity_id = :opp_id LIMIT 1"),
            {"opp_id": int(opportunity_id)},
        ).mappings().first()

    return dict(row) if row else {}


def refresh_deal_details(
    opportunity_id: int,
    *,
    update_fields: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    actor: str = "system_refresh",
) -> dict[str, Any]:
    """Refresh denormalized lifecycle fields without changing stage."""
    current = get_deal(opportunity_id)
    if not current:
        raise ValueError(f"No deal found for opportunity_id={opportunity_id}")

    merged_metadata = _json_dict(current.get("metadata_json"))
    if metadata:
        merged_metadata.update(metadata)

    update_fields = dict(update_fields or {})
    current_stage_str = current.get("current_stage") or DealStage.signal_detected.value
    try:
        stage = DealStage(current_stage_str)
    except ValueError:
        stage = DealStage.signal_detected

    icp_fit = int(update_fields.get("icp_fit_score", current.get("icp_fit_score", 0)) or 0)
    commercial = int(update_fields.get("commercial_readiness_score", current.get("commercial_readiness_score", 0)) or 0)
    contact_trust = int(update_fields.get("contact_trust_score", current.get("contact_trust_score", 0)) or 0)
    distress_type = str(update_fields.get("distress_type", current.get("distress_type", "unknown")) or "unknown")
    priority = _compute_priority(
        stage=stage,
        icp_fit=icp_fit,
        commercial=commercial,
        contact_trust=contact_trust,
        distress_type=distress_type,
        merchant_domain=str(update_fields.get("merchant_domain", current.get("merchant_domain", "")) or ""),
        processor=str(update_fields.get("processor", current.get("processor", "unknown")) or "unknown"),
    )

    set_clauses = ["metadata_json = :metadata", "updated_at = NOW()", "priority_score = :priority_score"]
    params: dict[str, Any] = {
        "opp_id": int(opportunity_id),
        "metadata": json.dumps(merged_metadata, default=str),
        "priority_score": priority,
    }
    for field_name, field_value in update_fields.items():
        if field_name in _UPDATABLE_FIELDS:
            set_clauses.append(f"{field_name} = :{field_name}")
            params[field_name] = field_value

    with engine.connect() as conn:
        conn.execute(
            text(f"UPDATE deal_lifecycle SET {', '.join(set_clauses)} WHERE opportunity_id = :opp_id"),
            params,
        )
        conn.commit()
        row = conn.execute(
            text("SELECT * FROM deal_lifecycle WHERE opportunity_id = :opp_id LIMIT 1"),
            {"opp_id": int(opportunity_id)},
        ).mappings().first()

    save_event(
        "deal_lifecycle_refreshed",
        {
            "opportunity_id": int(opportunity_id),
            "actor": actor,
            "updated_fields": sorted([k for k in update_fields.keys() if k in _UPDATABLE_FIELDS]),
            "priority_score": priority,
        },
    )
    return dict(row) if row else {}


# ── Read Accessors ───────────────────────────────────────────────────────────

def get_deal(opportunity_id: int) -> dict[str, Any] | None:
    """Get a deal by opportunity ID. Returns None if not found."""
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM deal_lifecycle WHERE opportunity_id = :opp_id LIMIT 1"),
            {"opp_id": int(opportunity_id)},
        ).mappings().first()
    return dict(row) if row else None


def get_deal_by_domain(merchant_domain: str) -> dict[str, Any] | None:
    """Get the most recent deal for a merchant domain."""
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT * FROM deal_lifecycle
                WHERE merchant_domain = :domain
                ORDER BY priority_score DESC, updated_at DESC
                LIMIT 1
            """),
            {"domain": (merchant_domain or "").strip().lower()},
        ).mappings().first()
    return dict(row) if row else None


def list_active_deals(*, limit: int = 20) -> list[dict[str, Any]]:
    """List active (non-terminal) deals ordered by priority."""
    terminal = {DealStage.outcome_won.value, DealStage.outcome_lost.value, DealStage.outcome_ignored.value}
    placeholders = ", ".join(f":t{i}" for i in range(len(terminal)))
    params: dict[str, Any] = {"limit": int(limit)}
    params.update({f"t{i}": v for i, v in enumerate(terminal)})
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT * FROM deal_lifecycle
                WHERE current_stage NOT IN ({placeholders})
                ORDER BY priority_score DESC, updated_at DESC
                LIMIT :limit
            """),
            params,
        ).mappings().all()
    return [dict(row) for row in rows]


def hydrate_deal_lifecycle(*, limit: int = 250) -> dict[str, Any]:
    """Refresh sparse active lifecycle rows from live outreach context."""
    ensure_deal_lifecycle_table()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT opportunity_id, current_stage
                FROM deal_lifecycle
                WHERE current_stage NOT IN ('outcome_won', 'outcome_lost', 'outcome_ignored')
                  AND (
                      processor = 'unknown'
                      OR distress_type = 'unknown'
                      OR contact_trust_score = 0
                      OR icp_fit_score = 0
                      OR commercial_readiness_score = 0
                  )
                ORDER BY priority_score DESC, updated_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()

    from runtime.ops.outreach_execution import get_outreach_context_snapshot

    hydrated = 0
    skipped = 0
    errors = 0
    examples: list[dict[str, Any]] = []
    for row in rows:
        opp_id = int(row["opportunity_id"])
        try:
            context = get_outreach_context_snapshot(opportunity_id=opp_id)
            if not context:
                skipped += 1
                continue
            refreshed = refresh_deal_details(
                opp_id,
                actor="hydration",
                update_fields={
                    "merchant_id": context.get("merchant_id"),
                    "merchant_domain": context.get("merchant_domain") or "",
                    "processor": context.get("processor") or "unknown",
                    "distress_type": context.get("distress_type") or "unknown",
                    "contact_email": context.get("contact_email") or "",
                    "contact_trust_score": int(context.get("contact_trust_score") or 0),
                    "icp_fit_score": int(context.get("icp_fit_score") or 0),
                    "commercial_readiness_score": int(context.get("commercial_readiness_score") or 0),
                    "outreach_status": context.get("outreach_status") or "no_outreach",
                    "gmail_thread_id": context.get("gmail_thread_id") or "",
                    "outcome_status": context.get("outcome_status") or "pending",
                },
                metadata={"hydrated_at": utc_now_iso()},
            )
            hydrated += 1
            if len(examples) < 10:
                examples.append(
                    {
                        "opportunity_id": opp_id,
                        "merchant_domain": refreshed.get("merchant_domain") or "",
                        "processor": refreshed.get("processor") or "unknown",
                        "distress_type": refreshed.get("distress_type") or "unknown",
                        "contact_trust_score": int(refreshed.get("contact_trust_score") or 0),
                        "icp_fit_score": int(refreshed.get("icp_fit_score") or 0),
                        "commercial_readiness_score": int(refreshed.get("commercial_readiness_score") or 0),
                    }
                )
        except Exception as exc:
            errors += 1
            logger.warning("hydrate error for opp %s: %s", opp_id, exc)

    result = {
        "status": "ok" if errors == 0 else "warning",
        "scanned": len(rows),
        "hydrated": hydrated,
        "skipped": skipped,
        "errors": errors,
        "examples": examples,
        "hydrated_at": utc_now_iso(),
    }
    record_component_state(
        "deal_lifecycle",
        ttl=3600,
        last_hydration_at=result["hydrated_at"],
        last_hydration_scanned=len(rows),
        last_hydration_hydrated=hydrated,
        last_hydration_errors=errors,
    )
    save_event("deal_lifecycle_hydrated", result)
    return result


def get_deal_history(opportunity_id: int, *, limit: int = 20) -> list[dict[str, Any]]:
    """Get the transition history for a deal."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT * FROM deal_stage_transitions
                WHERE opportunity_id = :opp_id
                ORDER BY transitioned_at DESC
                LIMIT :limit
            """),
            {"opp_id": int(opportunity_id), "limit": int(limit)},
        ).mappings().all()
    return [dict(row) for row in rows]


# ── Priority Scoring ─────────────────────────────────────────────────────────

def _compute_priority(
    *,
    stage: DealStage,
    icp_fit: int = 0,
    commercial: int = 0,
    contact_trust: int = 0,
    distress_type: str = "unknown",
    merchant_domain: str = "",
    processor: str = "unknown",
) -> float:
    """
    Compute a priority score for ranking deals in the action queue.

    Weights:
    - Stage urgency: 0-100 (replies and follow-ups are hottest)
    - ICP fit: 0-100         → ×0.25
    - Commercial readiness: 0-100 → ×0.30
    - Contact trust: 0-100   → ×0.15
    - Distress severity: 0-25 → ×1.0
    """
    stage_score = _STAGE_URGENCY.get(stage, 0)
    normalized_domain = str(merchant_domain or "").strip().lower()
    normalized_distress = str(distress_type or "unknown").strip().lower()
    normalized_processor = str(processor or "unknown").strip().lower()
    distress_score = _DISTRESS_SEVERITY.get(normalized_distress or "unknown", 0)

    priority = (
        stage_score
        + (icp_fit * 0.25)
        + (commercial * 0.30)
        + (contact_trust * 0.15)
        + distress_score
    )

    # Penalize legacy ghost rows so they stop crowding real merchant work.
    if not normalized_domain:
        priority -= 45
    if normalized_distress in {"unknown", ""}:
        priority -= 18
    if normalized_processor in {"unknown", ""}:
        priority -= 6
    if stage in {DealStage.signal_detected, DealStage.lead_qualified, DealStage.opportunity_created}:
        if contact_trust <= 0:
            priority -= 12
        if icp_fit <= 0:
            priority -= 8

    return round(max(priority, 0.0), 2)


def recompute_priority(opportunity_id: int) -> float:
    """Recompute and save the priority score for a deal."""
    deal = get_deal(opportunity_id)
    if not deal:
        return 0.0
    try:
        stage = DealStage(deal.get("current_stage", "signal_detected"))
    except ValueError:
        stage = DealStage.signal_detected

    priority = _compute_priority(
        stage=stage,
        icp_fit=int(deal.get("icp_fit_score", 0) or 0),
        commercial=int(deal.get("commercial_readiness_score", 0) or 0),
        contact_trust=int(deal.get("contact_trust_score", 0) or 0),
        distress_type=str(deal.get("distress_type", "unknown")),
        merchant_domain=str(deal.get("merchant_domain", "") or ""),
        processor=str(deal.get("processor", "unknown") or "unknown"),
    )
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE deal_lifecycle SET priority_score = :p, updated_at = NOW() WHERE opportunity_id = :opp_id"),
            {"p": priority, "opp_id": int(opportunity_id)},
        )
        conn.commit()
    return priority


# ── Backfill Migration ───────────────────────────────────────────────────────

def _infer_stage_from_existing(
    opp_status: str,
    outreach_status: str | None,
    outreach_approval: str | None,
    outcome_status: str | None,
) -> DealStage:
    """Map existing scattered state to a canonical deal stage."""
    # Check outcome first — terminal states take priority
    outcome = (outcome_status or "").strip().lower()
    if outcome == "won":
        return DealStage.outcome_won
    if outcome == "lost":
        return DealStage.outcome_lost
    if outcome == "ignored":
        return DealStage.outcome_ignored

    # Check opportunity-level status for terminal/rejected
    status = (opp_status or "").strip().lower()
    if status in {"rejected", "revoked", "revoked_non_aligned"}:
        return DealStage.outcome_ignored
    if status == "converted":
        return DealStage.outcome_won

    # Check outreach status — most specific state
    outreach = (outreach_status or "").strip().lower()
    if outreach == "replied":
        return DealStage.reply_received
    if outreach == "follow_up_needed":
        return DealStage.follow_up_due
    if outreach == "sent":
        return DealStage.outreach_sent
    if outreach == "draft_ready" and (outreach_approval or "").strip().lower() == "approved":
        return DealStage.outreach_approved
    if outreach in {"awaiting_approval", "draft_ready"}:
        return DealStage.outreach_drafted

    # Fall back to opportunity status
    if status == "outreach_sent":
        return DealStage.outreach_sent
    if status in {"approved", "outreach_pending", "pending_review"}:
        return DealStage.opportunity_created

    return DealStage.opportunity_created


def backfill_deal_lifecycle(*, dry_run: bool = False, limit: int = 5000) -> dict[str, Any]:
    """
    One-time migration: populate deal_lifecycle from existing tables.

    Reads from merchant_opportunities joined with opportunity_outreach_actions,
    merchant_contacts, and signals. Uses ON CONFLICT DO NOTHING for idempotency.

    Args:
        dry_run: If True, log what would be written without actually writing.
        limit: Max rows to process.

    Returns:
        Summary dict with counts.
    """
    ensure_deal_lifecycle_table()
    started = time.time()

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    mo.id AS opportunity_id,
                    mo.merchant_id,
                    COALESCE(mo.merchant_domain, m.domain, '') AS merchant_domain,
                    mo.status AS opp_status,
                    mo.processor,
                    mo.distress_topic,
                    mo.created_at AS opp_created_at,
                    oa.status AS outreach_status,
                    oa.approval_state AS outreach_approval,
                    oa.outcome_status,
                    oa.contact_email,
                    oa.gmail_thread_id,
                    oa.selected_play,
                    COALESCE((oa.metadata_json->>'icp_fit_score')::int, 0) AS icp_fit_score,
                    COALESCE((oa.metadata_json->>'contact_trust_score')::int, 0) AS contact_trust_score,
                    COALESCE((oa.metadata_json->>'commercial_readiness_score')::int, 0) AS commercial_readiness_score,
                    s.id AS signal_id
                FROM merchant_opportunities mo
                LEFT JOIN opportunity_outreach_actions oa ON oa.opportunity_id = mo.id
                LEFT JOIN merchants m ON m.id = mo.merchant_id
                LEFT JOIN LATERAL (
                    SELECT s2.id
                    FROM signals s2
                    WHERE s2.merchant_id = mo.merchant_id
                    ORDER BY s2.detected_at DESC
                    LIMIT 1
                ) s ON TRUE
                ORDER BY mo.created_at DESC
                LIMIT :limit
            """),
            {"limit": int(limit)},
        ).mappings().all()

    created = 0
    skipped = 0
    errors = 0
    dry_run_items: list[dict] = []

    for row in rows:
        try:
            opp_id = int(row["opportunity_id"])
            stage = _infer_stage_from_existing(
                opp_status=row.get("opp_status") or "",
                outreach_status=row.get("outreach_status"),
                outreach_approval=row.get("outreach_approval"),
                outcome_status=row.get("outcome_status"),
            )
            item = {
                "opportunity_id": opp_id,
                "merchant_id": int(row["merchant_id"]) if row.get("merchant_id") else None,
                "merchant_domain": (row.get("merchant_domain") or "").strip().lower(),
                "signal_id": int(row["signal_id"]) if row.get("signal_id") else None,
                "stage": stage.value,
                "processor": row.get("processor") or "unknown",
                "distress_type": row.get("distress_topic") or "unknown",
                "contact_email": row.get("contact_email") or "",
                "contact_trust_score": int(row.get("contact_trust_score") or 0),
                "icp_fit_score": int(row.get("icp_fit_score") or 0),
                "commercial_readiness_score": int(row.get("commercial_readiness_score") or 0),
                "gmail_thread_id": row.get("gmail_thread_id") or "",
                "outreach_status": row.get("outreach_status") or "no_outreach",
            }

            if dry_run:
                dry_run_items.append(item)
                skipped += 1
                continue

            create_deal(
                opportunity_id=opp_id,
                merchant_id=item["merchant_id"],
                merchant_domain=item["merchant_domain"],
                signal_id=item["signal_id"],
                initial_stage=stage,
                actor="backfill",
                processor=item["processor"],
                distress_type=item["distress_type"],
                contact_email=item["contact_email"],
                contact_trust_score=item["contact_trust_score"],
                icp_fit_score=item["icp_fit_score"],
                commercial_readiness_score=item["commercial_readiness_score"],
                metadata={
                    "backfilled_from": "merchant_opportunities",
                    "original_opp_status": row.get("opp_status") or "",
                    "original_outreach_status": row.get("outreach_status") or "",
                    "gmail_thread_id": item["gmail_thread_id"],
                    "outreach_status": item["outreach_status"],
                },
            )
            created += 1
        except Exception as exc:
            errors += 1
            logger.warning("backfill error for opp %s: %s", row.get("opportunity_id"), exc)

    duration = round(time.time() - started, 2)
    result = {
        "status": "dry_run" if dry_run else "completed",
        "rows_scanned": len(rows),
        "created": created,
        "skipped": skipped,
        "errors": errors,
        "duration_seconds": duration,
    }
    if dry_run:
        result["preview"] = dry_run_items[:10]

    record_component_state(
        "deal_lifecycle",
        ttl=3600,
        backfill_status=result["status"],
        backfill_created=created,
        backfill_errors=errors,
        backfill_at=utc_now_iso(),
    )
    save_event("deal_lifecycle_backfill", result)
    logger.info("deal_lifecycle backfill: %s", result)
    return result


# ── Helpers ──────────────────────────────────────────────────────────────────

_UPDATABLE_FIELDS = {
    "processor", "distress_type", "contact_email", "contact_trust_score",
    "outreach_status", "gmail_thread_id", "outcome_status",
    "icp_fit_score", "commercial_readiness_score", "merchant_domain",
    "merchant_id", "signal_id",
}


def _json_dict(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}
