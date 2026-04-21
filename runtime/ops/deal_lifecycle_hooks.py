"""
Deal Lifecycle Hooks — Side Effects on Stage Transitions
=========================================================
Lightweight hook layer that fires after deal stage transitions.
Keeps the core deal_lifecycle module clean from side effects.

These hooks are called by the action queue and outreach execution
layers after successful transitions. They are NOT called
automatically by transition_deal() — the caller decides which
hooks to fire. This keeps the state machine pure.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from memory.structured.db import save_event
from runtime.health.telemetry import record_component_state, utc_now_iso

logger = logging.getLogger("meridian.deal_lifecycle.hooks")

# Default follow-up window after outreach is sent
_DEFAULT_FOLLOW_UP_DAYS = 3


def on_outreach_drafted(deal: dict[str, Any]) -> None:
    """Fires when a deal transitions to outreach_drafted.

    This is the auto-drafting completion step. The draft is ready
    for the operator to review before sending.
    """
    save_event("deal_hook_outreach_drafted", {
        "opportunity_id": deal.get("opportunity_id"),
        "merchant_domain": deal.get("merchant_domain"),
        "processor": deal.get("processor"),
        "distress_type": deal.get("distress_type"),
    })
    logger.info(
        "hook: outreach drafted for opp=%s domain=%s",
        deal.get("opportunity_id"), deal.get("merchant_domain"),
    )


def on_outreach_approved(deal: dict[str, Any]) -> None:
    """Fires when a deal transitions to outreach_approved.

    The operator approved the draft. System can now auto-advance
    to send if the auto-execution mode allows it — but currently
    per operator preference, send requires explicit sign-off.
    """
    save_event("deal_hook_outreach_approved", {
        "opportunity_id": deal.get("opportunity_id"),
        "merchant_domain": deal.get("merchant_domain"),
    })
    logger.info(
        "hook: outreach approved for opp=%s, waiting for operator send",
        deal.get("opportunity_id"),
    )


def on_outreach_sent(deal: dict[str, Any]) -> None:
    """Fires when outreach is sent.

    Sets the follow-up timer and records telemetry.
    """
    opportunity_id = deal.get("opportunity_id")
    follow_up_at = datetime.now(timezone.utc) + timedelta(days=_DEFAULT_FOLLOW_UP_DAYS)

    save_event("deal_hook_outreach_sent", {
        "opportunity_id": opportunity_id,
        "merchant_domain": deal.get("merchant_domain"),
        "follow_up_due_at": follow_up_at.isoformat(),
    })

    record_component_state(
        "deal_lifecycle",
        ttl=3600,
        last_outreach_sent_opp=opportunity_id,
        last_outreach_sent_at=utc_now_iso(),
    )

    logger.info(
        "hook: outreach sent for opp=%s, follow-up due at %s",
        opportunity_id, follow_up_at.isoformat(),
    )


def on_reply_received(deal: dict[str, Any]) -> None:
    """Fires when a merchant reply is detected.

    This is the hottest state — a real human responded.
    Triggers telemetry and could trigger reply classification.
    """
    opportunity_id = deal.get("opportunity_id")
    save_event("deal_hook_reply_received", {
        "opportunity_id": opportunity_id,
        "merchant_domain": deal.get("merchant_domain"),
        "gmail_thread_id": deal.get("gmail_thread_id"),
    })

    record_component_state(
        "deal_lifecycle",
        ttl=3600,
        last_reply_received_opp=opportunity_id,
        last_reply_received_at=utc_now_iso(),
    )

    logger.info(
        "hook: reply received for opp=%s domain=%s",
        opportunity_id, deal.get("merchant_domain"),
    )


def on_follow_up_due(deal: dict[str, Any]) -> None:
    """Fires when a follow-up becomes due.

    Records telemetry for the operator briefing system.
    """
    save_event("deal_hook_follow_up_due", {
        "opportunity_id": deal.get("opportunity_id"),
        "merchant_domain": deal.get("merchant_domain"),
    })
    logger.info(
        "hook: follow-up due for opp=%s domain=%s",
        deal.get("opportunity_id"), deal.get("merchant_domain"),
    )


def on_outcome_set(deal: dict[str, Any], outcome: str) -> None:
    """Fires when a deal reaches a terminal outcome (won/lost/ignored).

    Triggers outreach learning so the system can learn from the outcome.
    """
    opportunity_id = deal.get("opportunity_id")
    save_event("deal_hook_outcome_set", {
        "opportunity_id": opportunity_id,
        "merchant_domain": deal.get("merchant_domain"),
        "processor": deal.get("processor"),
        "distress_type": deal.get("distress_type"),
        "outcome": outcome,
    })

    record_component_state(
        "deal_lifecycle",
        ttl=3600,
        last_outcome_set_opp=opportunity_id,
        last_outcome_set_at=utc_now_iso(),
        last_outcome_value=outcome,
    )

    logger.info(
        "hook: outcome %s set for opp=%s domain=%s",
        outcome, opportunity_id, deal.get("merchant_domain"),
    )


# ── Hook Dispatcher ──────────────────────────────────────────────────────────

_STAGE_HOOKS: dict[str, Any] = {
    "outreach_drafted": on_outreach_drafted,
    "outreach_approved": on_outreach_approved,
    "outreach_sent": on_outreach_sent,
    "reply_received": on_reply_received,
    "follow_up_due": on_follow_up_due,
}

_OUTCOME_STAGES = {"outcome_won", "outcome_lost", "outcome_ignored"}


def dispatch_hook(deal: dict[str, Any], new_stage: str) -> None:
    """Dispatch the appropriate hook for a stage transition."""
    if new_stage in _OUTCOME_STAGES:
        outcome = new_stage.replace("outcome_", "")
        on_outcome_set(deal, outcome)
        return

    hook_fn = _STAGE_HOOKS.get(new_stage)
    if hook_fn:
        try:
            hook_fn(deal)
        except Exception as exc:
            logger.error(
                "hook error for stage %s opp=%s: %s",
                new_stage, deal.get("opportunity_id"), exc,
            )
