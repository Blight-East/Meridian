from __future__ import annotations

import base64
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from memory.structured.db import engine, save_event
from runtime.channels.gmail_adapter import GmailAdapter, _headers_map, _payload_body
from runtime.channels.store import (
    get_channel_settings,
    get_gmail_thread_intelligence,
    log_action,
    record_journal_entry,
)
from runtime.intelligence.commercial_qualification import assess_commercial_readiness
from runtime.intelligence.distress_normalization import (
    doctrine_priority_reason,
    normalize_distress_topic,
    normalize_operator_action,
    operator_action_for_distress,
    operator_action_label,
    strategy_for_distress,
)
from runtime.intelligence.opportunity_queue_quality import (
    ELIGIBILITY_OUTREACH,
    evaluate_opportunity_queue_quality,
    summarize_quality_counts,
)
from runtime.ops.outreach_learning import ensure_outreach_learning_table, record_outreach_learning

_lifecycle_logger = logging.getLogger("meridian.outreach_execution.lifecycle")


def _sync_deal_lifecycle_transition(
    opportunity_id: int,
    stage: str,
    *,
    actor: str = "outreach_execution",
    update_fields: dict | None = None,
    metadata: dict | None = None,
) -> None:
    """Best-effort deal lifecycle transition. Never blocks the main path."""
    try:
        from runtime.ops.deal_lifecycle import transition_deal, get_deal, create_deal, DealStage
        from runtime.ops.deal_lifecycle_hooks import dispatch_hook

        deal = get_deal(int(opportunity_id))
        if not deal:
            # Extract only the kwargs that create_deal() actually accepts.
            # update_fields may contain keys like 'outreach_status'
            # that are valid for transition_deal but not for create_deal.
            _CREATE_DEAL_FIELDS = {
                "merchant_id", "merchant_domain", "signal_id", "processor",
                "distress_type", "contact_email", "contact_trust_score",
                "icp_fit_score", "commercial_readiness_score",
            }
            create_kwargs = {}
            if update_fields:
                for k, v in update_fields.items():
                    if k in _CREATE_DEAL_FIELDS:
                        create_kwargs[k] = v

            create_deal(
                opportunity_id=int(opportunity_id),
                initial_stage=DealStage(stage),
                actor=actor,
                **create_kwargs,
            )
            deal = get_deal(int(opportunity_id))
            if deal:
                dispatch_hook(deal, stage)
            return

        deal = transition_deal(
            int(opportunity_id),
            stage,
            actor=actor,
            update_fields=update_fields,
            metadata=metadata,
        )
        dispatch_hook(deal, stage)
    except Exception as exc:
        _lifecycle_logger.debug("deal lifecycle sync skipped for opp %s: %s", opportunity_id, exc)


def _lifecycle_update_fields_from_context(context: dict | None, **overrides) -> dict:
    """Project the richest available deal fields from an outreach context."""
    context = dict(context or {})
    fields = {
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
    }
    fields.update(overrides)
    return {key: value for key, value in fields.items() if value is not None}


def get_current_outreach_mode(channel: str = "gmail") -> str:
    try:
        settings = get_channel_settings(channel)
        mode = str((settings or {}).get("mode") or "").strip().lower()
        if mode:
            return mode
    except Exception:
        pass
    return OUTREACH_MODE


OUTREACH_MODE = os.getenv("OUTREACH_EXECUTION_MODE", "approval_required").strip().lower() or "approval_required"
FOLLOW_UP_DAYS = int(os.getenv("OUTREACH_FOLLOW_UP_DAYS", "3"))
AUTO_SEND_CONFIDENCE_THRESHOLD = float(os.getenv("GMAIL_CHANNEL_AUTO_SEND_CONFIDENCE", "0.90"))
AUTO_SEND_CONTACT_TRUST_MIN = int(os.getenv("MERIDIAN_AUTO_SEND_CONTACT_TRUST_MIN", "90"))
AUTO_SEND_COMMERCIAL_MIN = int(os.getenv("MERIDIAN_AUTO_SEND_COMMERCIAL_MIN", "80"))
AUTO_SEND_QUEUE_QUALITY_MIN = int(os.getenv("MERIDIAN_AUTO_SEND_QUEUE_QUALITY_MIN", "70"))
AUTO_SEND_ICP_MIN = int(os.getenv("MERIDIAN_AUTO_SEND_ICP_MIN", "45"))
ROLE_ADDRESS_PATTERNS = ("noreply", "no-reply", "do-not-reply", "mailer-daemon", "postmaster")
INTERNAL_CONTACT_DOMAINS = {"payflux.dev", "reach.payflux.dev"}
FREE_EMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
}
TRUSTED_ROLE_LOCAL_PARTS = {"hello", "contact", "team", "info", "support", "sales"}
DECISION_ROLE_LOCAL_PARTS = {
    "founder",
    "cofounder",
    "ceo",
    "owner",
    "finance",
    "payments",
    "ops",
    "operations",
    "risk",
    "compliance",
}
GENERIC_BLOCKED_LOCAL_PARTS = {
    "admin",
    "billing",
    "example",
    "hr",
    "jobs",
    "legal",
    "mailer-daemon",
    "marketing",
    "media",
    "no-reply",
    "noreply",
    "office",
    "postmaster",
    "press",
    "test",
    "user",
}
HIGH_CONFIDENCE_CONTACT_THRESHOLD = float(os.getenv("AGENT_FLUX_HIGH_CONFIDENCE_CONTACT_THRESHOLD", "0.85"))
USABLE_FALLBACK_CONTACT_THRESHOLD = float(os.getenv("AGENT_FLUX_USABLE_CONTACT_THRESHOLD", "0.64"))
GENERIC_FALLBACK_PAGE_TYPES = {"contact_page", "about_page", "team_page", "support_page", "finance_page", "payment_page"}
SOURCE_QUALITY_SCORES = {
    "merchant_website": 90,
    "verified_same_domain": 95,
    "role_inbox_same_domain": 82,
    "public_directory": 55,
    "pattern_enumeration": 35,
    "guessed_address": 20,
    "operator_validation": 0,
    "unknown_source": 25,
}
PLAY_TARGET_ROLES = {
    "urgent_processor_migration": (["finance", "payments", "operations", "founder"], "this is a processor-liquidity case"),
    "payout_acceleration": (["finance", "payments", "operations", "founder"], "this is a processor-liquidity case"),
    "reserve_negotiation": (["finance", "risk", "compliance", "operations"], "this looks like reserve or review pressure"),
    "compliance_remediation": (["compliance", "risk", "operations", "finance"], "this looks like a compliance or verification case"),
    "chargeback_mitigation": (["operations", "support leadership", "risk"], "chargeback-heavy cases usually sit with operations and risk owners"),
    "onboarding_assistance": (["founder", "operations", "finance"], "placement decisions usually sit with founders or operators"),
    "clarify_distress": (["finance", "operations"], "we still need a merchant operator who can confirm the payment issue"),
}

URGENCY_MAP = {
    "account_frozen": "high",
    "account_terminated": "high",
    "payouts_delayed": "high",
    "reserve_hold": "high",
    "verification_review": "medium",
    "processor_switch_intent": "medium",
    "chargeback_issue": "medium",
    "onboarding_rejected": "medium",
    "unknown": "low",
}

OUTREACH_STATE_ORDER = {
    "no_outreach": 0,
    "draft_ready": 1,
    "awaiting_approval": 2,
    "sent": 3,
    "replied": 4,
    "follow_up_needed": 5,
    "won": 6,
    "lost": 7,
    "ignored": 8,
}
ACTIVE_OUTREACH_OPPORTUNITY_STATUSES = ("pending_review", "approved", "outreach_pending", "outreach_sent")
PAGE_TYPE_PRIORITY = {
    "contact_page": 1,
    "about_page": 2,
    "team_page": 2,
    "footer_page": 3,
    "legal_page": 3,
    "support_page": 4,
    "finance_page": 4,
    "payment_page": 4,
}
PAGE_TYPE_LABELS = {
    "contact_page": "contact page",
    "about_page": "about page",
    "team_page": "team page",
    "footer_page": "footer",
    "legal_page": "legal page",
    "support_page": "support page",
    "finance_page": "finance page",
    "payment_page": "payment page",
}
DEEPENING_SEQUENCE_LABEL = "contact, about/team, footer/legal, and support/finance/payment pages"


def _list_env(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def _blocked_outreach_recipients(sender_email: str | None = None) -> set[str]:
    blocked = set(_list_env("GMAIL_DENYLIST"))
    blocked.update(_list_env("OPERATOR_EMAIL"))
    blocked.update(_list_env("PERSONAL_EMAIL"))
    if sender_email and "@" in str(sender_email):
        blocked.add(str(sender_email).strip().lower())
    return blocked


def _recipient_is_denied(email: str | None, *, sender_email: str | None = None) -> bool:
    lowered = str(email or "").strip().lower()
    return bool(lowered and lowered in _blocked_outreach_recipients(sender_email=sender_email))


def ensure_outreach_execution_tables() -> None:
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS opportunity_outreach_actions (
                    id BIGSERIAL PRIMARY KEY,
                    opportunity_id BIGINT NOT NULL UNIQUE,
                    merchant_id BIGINT,
                    merchant_domain TEXT,
                    contact_email TEXT,
                    contact_name TEXT,
                    channel TEXT NOT NULL DEFAULT 'gmail',
                    selected_play TEXT NOT NULL DEFAULT 'clarify_distress',
                    outreach_type TEXT NOT NULL DEFAULT 'initial_outreach',
                    subject TEXT,
                    body TEXT,
                    status TEXT NOT NULL DEFAULT 'no_outreach',
                    approval_state TEXT NOT NULL DEFAULT 'approval_required',
                    gmail_thread_id TEXT,
                    draft_message_id TEXT,
                    sent_message_id TEXT,
                    sent_at TIMESTAMPTZ,
                    replied_at TIMESTAMPTZ,
                    follow_up_due_at TIMESTAMPTZ,
                    outcome_status TEXT NOT NULL DEFAULT 'pending',
                    notes TEXT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS opportunity_outreach_actions_status_idx
                ON opportunity_outreach_actions (status, approval_state, updated_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS opportunity_outreach_actions_follow_up_idx
                ON opportunity_outreach_actions (follow_up_due_at, status)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS opportunity_outreach_actions_play_idx
                ON opportunity_outreach_actions (selected_play, outcome_status, updated_at DESC)
                """
            )
        )
        conn.commit()
    ensure_outreach_learning_table()


def recommend_outreach_for_opportunity(
    *,
    opportunity_id: int | None = None,
    merchant_domain: str | None = None,
) -> dict:
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        context = _get_outreach_context(conn, opportunity_id=opportunity_id, merchant_domain=merchant_domain)
    if not context:
        return {"error": "Opportunity not found"}
    recommendation = _build_outreach_recommendation(context)
    return {**context, **recommendation}


def get_contact_intelligence_for_opportunity(
    *,
    opportunity_id: int | None = None,
    merchant_domain: str | None = None,
) -> dict:
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        context = _get_outreach_context(conn, opportunity_id=opportunity_id, merchant_domain=merchant_domain)
    if not context:
        return {"error": "Opportunity not found"}
    recommendation = _build_outreach_recommendation(context)
    return {
        **context,
        **recommendation,
        "contact_summary": _contact_summary_line(context),
        "next_contact_move": context.get("next_contact_move") or "",
    }


def draft_outreach_for_opportunity(
    *,
    opportunity_id: int | None = None,
    merchant_domain: str | None = None,
    outreach_type: str | None = None,
    allow_best_effort: bool = False,
) -> dict:
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        context = _get_outreach_context(conn, opportunity_id=opportunity_id, merchant_domain=merchant_domain)
        if not context:
            return {"error": "Opportunity not found"}
        recommendation = _build_outreach_recommendation(context)
        final_outreach_type = outreach_type or _default_outreach_type(context)
        allow_reply_follow_up = (
            final_outreach_type == "reply_follow_up"
            and str(context.get("outreach_status") or "").strip().lower() == "replied"
            and recommendation.get("best_channel") == "gmail"
            and context.get("outcome_status") not in {"won", "lost", "ignored"}
        )
        if allow_reply_follow_up:
            recommendation = {
                **recommendation,
                "should_proceed_now": True,
                "wait_reason": "",
                "readiness_state": "reply_follow_up_ready",
            }
        allow_review_only_draft = bool(
            allow_best_effort
            and final_outreach_type == "initial_outreach"
            and context.get("outcome_status") not in {"won", "lost", "ignored"}
            and context.get("opportunity_status") in ACTIVE_OUTREACH_OPPORTUNITY_STATUSES
            and not (
                recommendation.get("best_play") == "clarify_distress"
                and normalize_distress_topic(context.get("distress_type")) == "unknown"
                and int(context.get("queue_quality_score") or 0) < 55
            )
        )
        if not recommendation.get("should_proceed_now") and not allow_review_only_draft:
            return {
                "error": recommendation.get("wait_reason") or "Outreach should wait",
                "recommendation": recommendation,
            }

        mode = recommendation.get("mode") or get_current_outreach_mode("gmail")
        draft = _build_outreach_artifact(context, recommendation, final_outreach_type)
        gmail_draft = None
        gmail_draft_error = ""
        draft_channel = recommendation.get("best_channel") or "gmail"
        if draft_channel == "gmail" and context.get("contact_email"):
            try:
                gmail_draft = _create_gmail_draft(
                    to_email=context["contact_email"],
                    subject=draft["subject"],
                    body=draft["body"],
                    thread_id=context.get("gmail_thread_id") if final_outreach_type != "initial_outreach" else "",
                )
            except Exception as exc:
                gmail_draft_error = str(exc)

        status = "awaiting_approval" if mode == "approval_required" else "draft_ready"
        approval_state = "approval_required" if mode == "approval_required" else "approved"
        metadata = {
            "urgency": recommendation.get("urgency"),
            "why_now": recommendation.get("why_now"),
            "strategy": recommendation.get("strategy"),
            "recommended_operator_action": recommendation.get("best_play"),
            "draft_version": 1,
            "gmail_draft_error": gmail_draft_error,
        }
        if allow_review_only_draft and not recommendation.get("should_proceed_now"):
            draft_channel = "review_only"
            status = "awaiting_approval"
            approval_state = "approval_required"
            metadata.update(
                {
                    "review_only_draft": True,
                    "draft_blocker": recommendation.get("wait_reason") or "",
                    "draft_blocker_codes": list(recommendation.get("contact_reason_codes") or []),
                }
            )
        record = _upsert_outreach_row(
            conn,
            opportunity_id=int(context["opportunity_id"]),
            merchant_id=context.get("merchant_id"),
            merchant_domain=context.get("merchant_domain"),
            contact_email=context.get("contact_email"),
            contact_name=context.get("contact_name"),
            channel=draft_channel,
            selected_play=recommendation.get("best_play") or "clarify_distress",
            outreach_type=final_outreach_type,
            subject=draft["subject"],
            body=draft["body"],
            status=status,
            approval_state=approval_state,
            gmail_thread_id=(gmail_draft or {}).get("thread_id") or context.get("gmail_thread_id") or "",
            draft_message_id=(gmail_draft or {}).get("draft_id") or "",
            sent_message_id=context.get("sent_message_id") or "",
            sent_at=context.get("sent_at"),
            replied_at=context.get("replied_at"),
            follow_up_due_at=context.get("follow_up_due_at"),
            outcome_status=context.get("outcome_status") or "pending",
            notes=draft.get("notes", ""),
            metadata=metadata,
        )
        conn.commit()

    log_action(
        channel=draft_channel if draft_channel in {"gmail", "review_only"} else "gmail",
        action_type="outreach_draft",
        approval_state=approval_state,
        mode=mode,
        result="drafted",
        account_identity=GmailAdapter().sender_email,
        merchant_id=context.get("merchant_id"),
        merchant_name=context.get("merchant_name") or "",
        thread_id=record.get("gmail_thread_id") or "",
        target_id=context.get("contact_email") or "",
        confidence=None,
        rationale=recommendation.get("why_now") or "",
        draft_text=draft["body"],
        metadata={
            "opportunity_id": context["opportunity_id"],
            "outreach_type": final_outreach_type,
            "selected_play": recommendation.get("best_play"),
            "subject": draft["subject"],
            "draft_message_id": record.get("draft_message_id"),
        },
    )
    save_event(
        "opportunity_outreach_drafted",
        {
            "opportunity_id": int(context["opportunity_id"]),
            "channel": recommendation.get("best_channel"),
            "selected_play": recommendation.get("best_play"),
            "outreach_type": final_outreach_type,
            "approval_state": approval_state,
        },
    )
    record_journal_entry(
        channel="gmail",
        external_id=str(context["opportunity_id"]),
        last_action_type="outreach_draft",
        account_identity=GmailAdapter().sender_email,
        thread_id=record.get("gmail_thread_id") or "",
        merchant_id=context.get("merchant_id"),
        status=status,
        metadata={"contact_email": context.get("contact_email"), "selected_play": recommendation.get("best_play")},
    )
    # Sync to deal lifecycle
    _sync_deal_lifecycle_transition(
        int(context["opportunity_id"]),
        "outreach_drafted",
        actor="outreach_execution",
        update_fields=_lifecycle_update_fields_from_context(
            context,
            gmail_thread_id=record.get("gmail_thread_id") or "",
            outreach_status=status,
        ),
    )

    return {
        "status": "ok",
        "opportunity_id": int(context["opportunity_id"]),
        "merchant_domain": context.get("merchant_domain") or "",
        "contact_email": context.get("contact_email") or "",
        "best_channel": recommendation.get("best_channel"),
        "best_play": recommendation.get("best_play"),
        "best_play_label": operator_action_label(recommendation.get("best_play")),
        "urgency": recommendation.get("urgency"),
        "why_now": recommendation.get("why_now"),
        "should_proceed_now": recommendation.get("should_proceed_now"),
        "outreach_type": final_outreach_type,
        "subject": draft["subject"],
        "body": draft["body"],
        "approval_state": approval_state,
        "status_label": status,
        "draft_message_id": record.get("draft_message_id") or "",
        "gmail_thread_id": record.get("gmail_thread_id") or "",
        "gmail_draft_error": gmail_draft_error,
        "review_only_draft": bool(metadata.get("review_only_draft")),
        "draft_blocker": metadata.get("draft_blocker") or "",
    }


def approve_outreach_for_opportunity(
    opportunity_id: int,
    notes: str = "",
    approved_by: str = "",
    approval_source: str = "operator_command",
) -> dict:
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        context = _get_outreach_context(conn, opportunity_id=opportunity_id)
        row = conn.execute(
            text(
                """
                SELECT *
                FROM opportunity_outreach_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        if not row:
            return {"error": "Draft outreach first before approving it"}
        metadata = _json_dict(row.get("metadata_json"))
        metadata.update(
            {
                "approved_by": (approved_by or "").strip(),
                "approval_source": (approval_source or "operator_command").strip(),
                "approved_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        record = _upsert_outreach_row(
            conn,
            opportunity_id=int(opportunity_id),
            merchant_id=row.get("merchant_id"),
            merchant_domain=row.get("merchant_domain"),
            contact_email=row.get("contact_email"),
            contact_name=row.get("contact_name"),
            channel=row.get("channel") or "gmail",
            selected_play=row.get("selected_play") or "clarify_distress",
            outreach_type=row.get("outreach_type") or "initial_outreach",
            subject=row.get("subject") or "",
            body=row.get("body") or "",
            status="draft_ready",
            approval_state="approved",
            gmail_thread_id=row.get("gmail_thread_id") or "",
            draft_message_id=row.get("draft_message_id") or "",
            sent_message_id=row.get("sent_message_id") or "",
            sent_at=row.get("sent_at"),
            replied_at=row.get("replied_at"),
            follow_up_due_at=row.get("follow_up_due_at"),
            outcome_status=row.get("outcome_status") or "pending",
            notes=notes or row.get("notes") or "",
            metadata=metadata,
        )
        conn.commit()
    save_event(
        "opportunity_outreach_approved",
        {
            "opportunity_id": int(opportunity_id),
            "notes": notes or "",
            "approved_by": (approved_by or "").strip(),
            "approval_source": (approval_source or "operator_command").strip(),
        },
    )
    # Sync to deal lifecycle
    _sync_deal_lifecycle_transition(
        int(opportunity_id),
        "outreach_approved",
        actor=approved_by or "operator",
        update_fields=_lifecycle_update_fields_from_context(
            context,
            outreach_status="draft_ready",
        ),
        metadata={"approval_source": approval_source},
    )

    return {
        "status": "ok",
        "opportunity_id": int(opportunity_id),
        "approval_state": "approved",
        "outreach_status": record.get("status"),
        "selected_play": record.get("selected_play"),
        "approved_by": metadata.get("approved_by") or "",
        "approval_source": metadata.get("approval_source") or "",
    }


def rewrite_outreach_for_opportunity(
    opportunity_id: int,
    *,
    style: str = "",
    instructions: str = "",
    rewritten_by: str = "",
    rewrite_source: str = "operator_command",
) -> dict:
    ensure_outreach_execution_tables()
    normalized_style = _normalize_rewrite_style(style)
    with engine.connect() as conn:
        context = _get_outreach_context(conn, opportunity_id=opportunity_id)
        if not context:
            return {"error": "Opportunity not found"}
        row = conn.execute(
            text(
                """
                SELECT *
                FROM opportunity_outreach_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        if not row:
            return {"error": "Draft outreach first before rewriting it"}
        record = dict(row)
        if str(record.get("status") or "").strip().lower() in {"sent", "replied", "won", "lost", "ignored"}:
            return {"error": "This outreach is already live or closed and should not be rewritten"}

        recommendation = _build_outreach_recommendation(context)
        rewritten = _rewrite_outreach_artifact(
            context,
            recommendation,
            outreach_type=record.get("outreach_type") or "initial_outreach",
            subject=record.get("subject") or "",
            body=record.get("body") or "",
            style=normalized_style,
            instructions=instructions or "",
        )
        metadata = _json_dict(record.get("metadata_json"))
        current_version = int(metadata.get("draft_version") or 1)
        metadata.update(
            {
                "draft_version": current_version + 1,
                "rewrite_style": normalized_style,
                "rewrite_instructions": (instructions or "").strip(),
                "rewritten_by": (rewritten_by or "").strip(),
                "rewrite_source": (rewrite_source or "operator_command").strip(),
                "rewritten_at": datetime.now(timezone.utc).isoformat(),
                "gmail_draft_error": "",
            }
        )

        gmail_draft = None
        gmail_draft_error = ""
        if (record.get("channel") or "gmail") == "gmail":
            try:
                gmail_draft = _create_gmail_draft(
                    to_email=record.get("contact_email") or "",
                    subject=rewritten["subject"],
                    body=rewritten["body"],
                    thread_id=record.get("gmail_thread_id") or "",
                )
            except Exception as exc:
                gmail_draft_error = str(exc)
                metadata["gmail_draft_error"] = gmail_draft_error

        updated = _upsert_outreach_row(
            conn,
            opportunity_id=int(opportunity_id),
            merchant_id=record.get("merchant_id"),
            merchant_domain=record.get("merchant_domain"),
            contact_email=record.get("contact_email"),
            contact_name=record.get("contact_name"),
            channel=record.get("channel") or "gmail",
            selected_play=record.get("selected_play") or recommendation.get("best_play") or "clarify_distress",
            outreach_type=record.get("outreach_type") or "initial_outreach",
            subject=rewritten["subject"],
            body=rewritten["body"],
            status=record.get("status") or "awaiting_approval",
            approval_state=record.get("approval_state") or "approval_required",
            gmail_thread_id=(gmail_draft or {}).get("thread_id") or record.get("gmail_thread_id") or "",
            draft_message_id=(gmail_draft or {}).get("draft_id") or record.get("draft_message_id") or "",
            sent_message_id=record.get("sent_message_id") or "",
            sent_at=record.get("sent_at"),
            replied_at=record.get("replied_at"),
            follow_up_due_at=record.get("follow_up_due_at"),
            outcome_status=record.get("outcome_status") or "pending",
            notes=rewritten.get("notes") or record.get("notes") or "",
            metadata=metadata,
        )
        conn.commit()

    save_event(
        "opportunity_outreach_rewritten",
        {
            "opportunity_id": int(opportunity_id),
            "selected_play": updated.get("selected_play") or "",
            "rewrite_style": normalized_style,
            "rewritten_by": (rewritten_by or "").strip(),
            "rewrite_source": (rewrite_source or "operator_command").strip(),
        },
    )
    return {
        "status": "ok",
        "opportunity_id": int(opportunity_id),
        "rewrite_style": normalized_style,
        "subject": updated.get("subject") or "",
        "body": updated.get("body") or "",
        "approval_state": updated.get("approval_state") or "",
        "outreach_status": updated.get("status") or "",
        "draft_version": int((updated.get("metadata_json") or {}).get("draft_version") or current_version + 1),
        "gmail_draft_error": gmail_draft_error,
        "rewritten_by": (updated.get("metadata_json") or {}).get("rewritten_by") or "",
        "rewrite_source": (updated.get("metadata_json") or {}).get("rewrite_source") or "",
    }


def preview_rewrite_outreach_for_opportunity(
    opportunity_id: int,
    *,
    style: str = "",
    instructions: str = "",
) -> dict:
    ensure_outreach_execution_tables()
    normalized_style = _normalize_rewrite_style(style)
    with engine.connect() as conn:
        context = _get_outreach_context(conn, opportunity_id=opportunity_id)
        if not context:
            return {"error": "Opportunity not found"}
        row = conn.execute(
            text(
                """
                SELECT *
                FROM opportunity_outreach_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        if not row:
            return {"error": "Draft outreach first before previewing a rewrite"}
        recommendation = _build_outreach_recommendation(context)
        rewritten = _rewrite_outreach_artifact(
            context,
            recommendation,
            outreach_type=row.get("outreach_type") or "initial_outreach",
            subject=row.get("subject") or "",
            body=row.get("body") or "",
            style=normalized_style,
            instructions=instructions or "",
        )
    return {
        "status": "ok",
        "opportunity_id": int(opportunity_id),
        "rewrite_style": normalized_style,
        "subject": rewritten.get("subject") or "",
        "body": rewritten.get("body") or "",
        "notes": rewritten.get("notes") or "",
        "instructions": (instructions or "").strip(),
    }


def send_outreach_for_opportunity(
    opportunity_id: int,
    approved_by: str = "",
    approval_source: str = "operator_command",
) -> dict:
    ensure_outreach_execution_tables()
    sync_outreach_execution_state(limit=25)
    with engine.connect() as conn:
        context = _get_outreach_context(conn, opportunity_id=opportunity_id)
        if not context:
            return {"error": "Opportunity not found"}
        recommendation = _build_outreach_recommendation(context)
        if not recommendation.get("contact_send_eligible"):
            return {"error": recommendation.get("wait_reason") or "This lead is not send-ready yet"}
        row = conn.execute(
            text(
                """
                SELECT *
                FROM opportunity_outreach_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        if not row:
            return {"error": "Draft outreach first before sending it"}
        if row.get("approval_state") != "approved":
            return {"error": "Outreach is still awaiting approval"}
        if row.get("status") in {"sent", "won", "lost", "ignored"} and row.get("follow_up_due_at") and not _follow_up_allowed(dict(row)):
            return {"error": "Outreach already sent and no follow-up is due yet"}

        result = _send_gmail_message(
            to_email=row.get("contact_email") or "",
            subject=row.get("subject") or "",
            body=row.get("body") or "",
            thread_id=row.get("gmail_thread_id") or "",
        )
        sent_at = datetime.now(timezone.utc)
        follow_up_due_at = sent_at + timedelta(days=FOLLOW_UP_DAYS)
        metadata = _json_dict(row.get("metadata_json"))
        metadata["last_send_type"] = row.get("outreach_type") or "initial_outreach"
        metadata["sent_by"] = (approved_by or "").strip()
        metadata["send_source"] = (approval_source or "operator_command").strip()
        metadata["sent_at_iso"] = sent_at.isoformat()
        record = _upsert_outreach_row(
            conn,
            opportunity_id=int(opportunity_id),
            merchant_id=row.get("merchant_id"),
            merchant_domain=row.get("merchant_domain"),
            contact_email=row.get("contact_email"),
            contact_name=row.get("contact_name"),
            channel=row.get("channel") or "gmail",
            selected_play=row.get("selected_play") or "clarify_distress",
            outreach_type=row.get("outreach_type") or "initial_outreach",
            subject=row.get("subject") or "",
            body=row.get("body") or "",
            status="sent",
            approval_state="sent",
            gmail_thread_id=result.get("thread_id") or row.get("gmail_thread_id") or "",
            draft_message_id=row.get("draft_message_id") or "",
            sent_message_id=result.get("message_id") or "",
            sent_at=sent_at,
            replied_at=row.get("replied_at"),
            follow_up_due_at=follow_up_due_at,
            outcome_status=row.get("outcome_status") or "pending",
            notes=row.get("notes") or "",
            metadata=metadata,
        )
        conn.commit()

    log_action(
        channel="gmail",
        action_type="outreach_send",
        approval_state="approved",
        mode=get_current_outreach_mode("gmail"),
        result="sent",
        account_identity=GmailAdapter().sender_email,
        merchant_id=row.get("merchant_id"),
        merchant_name="",
        thread_id=record.get("gmail_thread_id") or "",
        target_id=row.get("contact_email") or "",
        rationale=f"Sent {row.get('outreach_type') or 'initial_outreach'} for operator-approved {row.get('selected_play') or 'clarify_distress'} play.",
        final_text=row.get("body") or "",
        idempotency_key=f"outreach:{opportunity_id}:{row.get('outreach_type') or 'initial_outreach'}:{row.get('selected_play') or 'clarify_distress'}",
        metadata={
            "opportunity_id": int(opportunity_id),
            "draft_message_id": row.get("draft_message_id") or "",
            "sent_message_id": record.get("sent_message_id") or "",
            "gmail_thread_id": record.get("gmail_thread_id") or "",
            "approved_by": (approved_by or "").strip(),
            "approval_source": (approval_source or "operator_command").strip(),
        },
        sent=True,
    )
    save_event(
        "opportunity_outreach_sent",
        {
            "opportunity_id": int(opportunity_id),
            "selected_play": row.get("selected_play") or "clarify_distress",
            "outreach_type": row.get("outreach_type") or "initial_outreach",
            "gmail_thread_id": record.get("gmail_thread_id") or "",
            "approved_by": (approved_by or "").strip(),
            "approval_source": (approval_source or "operator_command").strip(),
        },
    )
    record_journal_entry(
        channel="gmail",
        external_id=str(opportunity_id),
        last_action_type="outreach_send",
        account_identity=GmailAdapter().sender_email,
        thread_id=record.get("gmail_thread_id") or "",
        merchant_id=row.get("merchant_id"),
        status="sent",
        metadata={"contact_email": row.get("contact_email"), "selected_play": row.get("selected_play")},
    )
    # Sync to deal lifecycle
    _sync_deal_lifecycle_transition(
        int(opportunity_id),
        "outreach_sent",
        actor=approved_by or "operator",
        update_fields=_lifecycle_update_fields_from_context(
            context,
            outreach_status="sent",
            gmail_thread_id=record.get("gmail_thread_id") or "",
        ),
        metadata={"approval_source": approval_source},
    )

    return {
        "status": "ok",
        "opportunity_id": int(opportunity_id),
        "outreach_status": "sent",
        "gmail_thread_id": record.get("gmail_thread_id") or "",
        "sent_message_id": record.get("sent_message_id") or "",
        "follow_up_due_at": _serialize_dt(record.get("follow_up_due_at")),
        "approved_by": (record.get("metadata_json") or {}).get("approved_by") or (approved_by or "").strip(),
        "approval_source": (record.get("metadata_json") or {}).get("approval_source") or (approval_source or "operator_command").strip(),
        "send_source": (record.get("metadata_json") or {}).get("send_source") or "",
    }


def sync_outreach_execution_state(limit: int = 100) -> dict:
    ensure_outreach_execution_tables()
    adapter = GmailAdapter()
    now = datetime.now(timezone.utc)
    replied = 0
    follow_up_needed = 0
    rows_checked = 0
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM opportunity_outreach_actions
                WHERE channel = 'gmail'
                  AND status IN ('sent', 'follow_up_needed', 'replied')
                ORDER BY updated_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().fetchall()
        for row in rows:
            rows_checked += 1
            current = dict(row)
            if current.get("outcome_status") in {"won", "lost", "ignored"}:
                desired = current.get("outcome_status")
                if current.get("status") != desired:
                    _upsert_outreach_row(
                        conn,
                        opportunity_id=int(current["opportunity_id"]),
                        merchant_id=current.get("merchant_id"),
                        merchant_domain=current.get("merchant_domain"),
                        contact_email=current.get("contact_email"),
                        contact_name=current.get("contact_name"),
                        channel=current.get("channel") or "gmail",
                        selected_play=current.get("selected_play") or "clarify_distress",
                        outreach_type=current.get("outreach_type") or "initial_outreach",
                        subject=current.get("subject") or "",
                        body=current.get("body") or "",
                        status=desired,
                        approval_state=current.get("approval_state") or "approval_required",
                        gmail_thread_id=current.get("gmail_thread_id") or "",
                        draft_message_id=current.get("draft_message_id") or "",
                        sent_message_id=current.get("sent_message_id") or "",
                        sent_at=current.get("sent_at"),
                        replied_at=current.get("replied_at"),
                        follow_up_due_at=current.get("follow_up_due_at"),
                        outcome_status=current.get("outcome_status") or "pending",
                        notes=current.get("notes") or "",
                        metadata=_json_dict(current.get("metadata_json")),
                    )
                continue

            thread_id = current.get("gmail_thread_id") or ""
            sent_at = _ensure_utc_datetime(current.get("sent_at"))
            latest_reply_at = None
            if thread_id:
                try:
                    thread = adapter._request("GET", f"/threads/{thread_id}", params={"format": "full"})
                    for message in thread.get("messages", []) or []:
                        payload = message.get("payload", {}) or {}
                        headers = _headers_map(payload)
                        sender = (headers.get("from", "") or "").lower()
                        internal_date = _ensure_utc_datetime(int(message.get("internalDate", "0")) / 1000 if str(message.get("internalDate", "")).isdigit() else None)
                        if adapter.sender_email and adapter.sender_email.lower() in sender:
                            continue
                        if current.get("contact_email") and current["contact_email"].lower() not in sender and "@" not in sender:
                            continue
                        if sent_at and internal_date and internal_date <= sent_at:
                            continue
                        latest_reply_at = max(latest_reply_at, internal_date) if latest_reply_at and internal_date else (internal_date or latest_reply_at)
                except Exception:
                    pass

            if latest_reply_at:
                replied += 1
                _upsert_outreach_row(
                    conn,
                    opportunity_id=int(current["opportunity_id"]),
                    merchant_id=current.get("merchant_id"),
                    merchant_domain=current.get("merchant_domain"),
                    contact_email=current.get("contact_email"),
                    contact_name=current.get("contact_name"),
                    channel=current.get("channel") or "gmail",
                    selected_play=current.get("selected_play") or "clarify_distress",
                    outreach_type=current.get("outreach_type") or "initial_outreach",
                    subject=current.get("subject") or "",
                    body=current.get("body") or "",
                    status="replied",
                    approval_state=current.get("approval_state") or "sent",
                    gmail_thread_id=current.get("gmail_thread_id") or "",
                    draft_message_id=current.get("draft_message_id") or "",
                    sent_message_id=current.get("sent_message_id") or "",
                    sent_at=current.get("sent_at"),
                    replied_at=latest_reply_at,
                    follow_up_due_at=current.get("follow_up_due_at"),
                    outcome_status=current.get("outcome_status") or "pending",
                    notes=current.get("notes") or "",
                    metadata=_json_dict(current.get("metadata_json")),
                )
                # Sync reply to deal lifecycle
                _sync_deal_lifecycle_transition(
                    int(current["opportunity_id"]),
                    "reply_received",
                    actor="gmail_sync",
                    update_fields=_lifecycle_update_fields_from_context(
                        _get_outreach_context(conn, opportunity_id=int(current["opportunity_id"])),
                        outreach_status="replied",
                    ),
                )
                continue

            due_at = _ensure_utc_datetime(current.get("follow_up_due_at"))
            if current.get("status") == "sent" and due_at and due_at <= now:
                follow_up_needed += 1
                _upsert_outreach_row(
                    conn,
                    opportunity_id=int(current["opportunity_id"]),
                    merchant_id=current.get("merchant_id"),
                    merchant_domain=current.get("merchant_domain"),
                    contact_email=current.get("contact_email"),
                    contact_name=current.get("contact_name"),
                    channel=current.get("channel") or "gmail",
                    selected_play=current.get("selected_play") or "clarify_distress",
                    outreach_type=current.get("outreach_type") or "initial_outreach",
                    subject=current.get("subject") or "",
                    body=current.get("body") or "",
                    status="follow_up_needed",
                    approval_state=current.get("approval_state") or "sent",
                    gmail_thread_id=current.get("gmail_thread_id") or "",
                    draft_message_id=current.get("draft_message_id") or "",
                    sent_message_id=current.get("sent_message_id") or "",
                    sent_at=current.get("sent_at"),
                    replied_at=current.get("replied_at"),
                    follow_up_due_at=current.get("follow_up_due_at"),
                    outcome_status=current.get("outcome_status") or "pending",
                    notes=current.get("notes") or "",
                    metadata=_json_dict(current.get("metadata_json")),
                )
                _sync_deal_lifecycle_transition(
                    int(current["opportunity_id"]),
                    "follow_up_due",
                    actor="gmail_sync",
                    update_fields=_lifecycle_update_fields_from_context(
                        _get_outreach_context(conn, opportunity_id=int(current["opportunity_id"])),
                        outreach_status="follow_up_needed",
                    ),
                )
        conn.commit()
    return {"status": "ok", "rows_checked": rows_checked, "replied": replied, "follow_up_needed": follow_up_needed}


def list_outreach_awaiting_approval(limit: int = 10) -> dict:
    sync_outreach_execution_state(limit=25)
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM opportunity_outreach_actions
                WHERE status IN ('awaiting_approval', 'draft_ready')
                  AND approval_state = 'approval_required'
                ORDER BY updated_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().fetchall()
    cases = [_serialize_outreach_row(dict(row)) for row in rows]
    return {"count": len(cases), "cases": cases}


def list_sent_outreach_needing_follow_up(limit: int = 10) -> dict:
    sync_outreach_execution_state(limit=50)
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM opportunity_outreach_actions
                WHERE status IN ('follow_up_needed', 'replied')
                ORDER BY
                    CASE status WHEN 'replied' THEN 0 ELSE 1 END,
                    follow_up_due_at ASC NULLS LAST,
                    updated_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().fetchall()
    cases = [_serialize_outreach_row(dict(row)) for row in rows]
    return {"count": len(cases), "cases": cases}


def list_send_eligible_outreach_leads(limit: int = 10) -> dict:
    return _list_outreach_readiness_cases(limit=limit, blocked_only=False)


def list_blocked_outreach_leads(limit: int = 10) -> dict:
    return _list_outreach_readiness_cases(limit=limit, blocked_only=True)


def get_selected_outreach_for_opportunity(
    *,
    opportunity_id: int | None = None,
    merchant_domain: str | None = None,
) -> dict:
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        context = _get_outreach_context(conn, opportunity_id=opportunity_id, merchant_domain=merchant_domain)
        if not context:
            return {"error": "Opportunity not found"}
    recommendation = _build_outreach_recommendation(context)
    return {
        **context,
        **recommendation,
        "current_status": context.get("outreach_status") or "no_outreach",
        "approval_state": context.get("approval_state") or ("approval_required" if recommendation.get("should_proceed_now") else "waiting"),
    }


def update_outreach_outcome(opportunity_id: int, outcome_status: str, notes: str = "") -> dict:
    ensure_outreach_execution_tables()
    normalized = str(outcome_status or "").strip().lower()
    if normalized not in {"won", "lost", "ignored"}:
        return {"error": "Unsupported outreach outcome"}
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM opportunity_outreach_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
        if not row:
            conn.execute(
                text(
                    """
                    INSERT INTO opportunity_outreach_actions (
                        opportunity_id, status, approval_state, outcome_status, notes, updated_at
                    )
                    VALUES (:opportunity_id, :status, 'approval_required', :outcome_status, :notes, NOW())
                    ON CONFLICT (opportunity_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        outcome_status = EXCLUDED.outcome_status,
                        notes = EXCLUDED.notes,
                        updated_at = NOW()
                    """
                ),
                {
                    "opportunity_id": int(opportunity_id),
                    "status": normalized,
                    "outcome_status": normalized,
                    "notes": notes or "",
                },
            )
        else:
            _upsert_outreach_row(
                conn,
                opportunity_id=int(opportunity_id),
                merchant_id=row.get("merchant_id"),
                merchant_domain=row.get("merchant_domain"),
                contact_email=row.get("contact_email"),
                contact_name=row.get("contact_name"),
                channel=row.get("channel") or "gmail",
                selected_play=row.get("selected_play") or "clarify_distress",
                outreach_type=row.get("outreach_type") or "initial_outreach",
                subject=row.get("subject") or "",
                body=row.get("body") or "",
                status=normalized,
                approval_state=row.get("approval_state") or "approved",
                gmail_thread_id=row.get("gmail_thread_id") or "",
                draft_message_id=row.get("draft_message_id") or "",
                sent_message_id=row.get("sent_message_id") or "",
                sent_at=row.get("sent_at"),
                replied_at=row.get("replied_at"),
                follow_up_due_at=row.get("follow_up_due_at"),
                outcome_status=normalized,
                notes=notes or row.get("notes") or "",
                metadata=_json_dict(row.get("metadata_json")),
            )
        context = _get_outreach_context(conn, opportunity_id=int(opportunity_id))
        conn.commit()
    if context:
        reply_context = _reply_learning_context(context)
        feedback_note = _build_outcome_feedback_note(reply_context=reply_context, notes=notes or "")
        summary = _build_outreach_learning_summary(
            context=context,
            outcome_status=normalized,
            notes=feedback_note,
        )
        record_outreach_learning(
            opportunity_id=int(opportunity_id),
            merchant_domain=context.get("merchant_domain") or "",
            processor=context.get("processor") or "",
            distress_type=context.get("distress_type") or "",
            selected_play=context.get("selected_play") or context.get("recommended_play") or "",
            outreach_type=context.get("outreach_type") or "initial_outreach",
            rewrite_style=context.get("rewrite_style") or "standard",
            outcome_status=normalized,
            lesson_summary=summary,
            operator_note=feedback_note,
            source="outreach_outcome_auto_feedback" if reply_context else "outreach_outcome",
        )
        save_event(
            "outreach_outcome_feedback_captured",
            {
                "opportunity_id": int(opportunity_id),
                "outcome_status": normalized,
                "reply_intent": reply_context.get("reply_intent") or "",
                "buying_intent": reply_context.get("buying_intent") or "",
            },
        )
    save_event("opportunity_outreach_outcome_updated", {"opportunity_id": int(opportunity_id), "outcome_status": normalized, "notes": notes or ""})
    # Sync outcome to deal lifecycle
    _sync_deal_lifecycle_transition(
        int(opportunity_id),
        f"outcome_{normalized}",
        actor="operator",
        update_fields=_lifecycle_update_fields_from_context(
            context,
            outcome_status=normalized,
            outreach_status=normalized,
        ),
    )
    return {
        "status": "ok",
        "opportunity_id": int(opportunity_id),
        "outcome_status": normalized,
        "learning_feedback_note": feedback_note if context else "",
    }


def get_outreach_execution_metrics(refresh_state: bool = True) -> dict:
    if refresh_state:
        sync_outreach_execution_state(limit=50)
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        drafts_ready = conn.execute(
            text("SELECT COUNT(*) FROM opportunity_outreach_actions WHERE status = 'draft_ready'")
        ).scalar() or 0
        awaiting_approval = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM opportunity_outreach_actions
                WHERE status = 'awaiting_approval'
                   OR (status = 'draft_ready' AND approval_state = 'approval_required')
                """
            )
        ).scalar() or 0
        sent_24h = conn.execute(
            text("SELECT COUNT(*) FROM opportunity_outreach_actions WHERE sent_at >= NOW() - INTERVAL '24 hours'")
        ).scalar() or 0
        replied_24h = conn.execute(
            text("SELECT COUNT(*) FROM opportunity_outreach_actions WHERE replied_at >= NOW() - INTERVAL '24 hours'")
        ).scalar() or 0
        replied_open = conn.execute(
            text("SELECT COUNT(*) FROM opportunity_outreach_actions WHERE status = 'replied'")
        ).scalar() or 0
        won_24h = conn.execute(
            text("SELECT COUNT(*) FROM opportunity_outreach_actions WHERE outcome_status = 'won' AND updated_at >= NOW() - INTERVAL '24 hours'")
        ).scalar() or 0
        lost_24h = conn.execute(
            text("SELECT COUNT(*) FROM opportunity_outreach_actions WHERE outcome_status = 'lost' AND updated_at >= NOW() - INTERVAL '24 hours'")
        ).scalar() or 0
        follow_ups_due = conn.execute(
            text("SELECT COUNT(*) FROM opportunity_outreach_actions WHERE status = 'follow_up_needed'")
        ).scalar() or 0
        play_rows = conn.execute(
            text(
                """
                SELECT
                    selected_play,
                    COUNT(*) AS total,
                    SUM(CASE WHEN outcome_status = 'won' THEN 1 ELSE 0 END) AS wins
                FROM opportunity_outreach_actions
                WHERE selected_play IS NOT NULL AND selected_play != ''
                GROUP BY selected_play
                ORDER BY wins DESC, total DESC
                """
            )
        ).mappings().fetchall()
    conversion_by_play = {}
    for row in play_rows:
        total = int(row.get("total") or 0)
        wins = int(row.get("wins") or 0)
        conversion_by_play[row["selected_play"]] = round(wins / total, 4) if total else 0.0
    contact_metrics = _compute_contact_quality_metrics()
    return {
        "outreach_drafts_ready": int(drafts_ready),
        "outreach_awaiting_approval": int(awaiting_approval),
        "outreach_sent_24h": int(sent_24h),
        "outreach_replied_24h": int(replied_24h),
        "outreach_replied_open": int(replied_open),
        "outreach_won_24h": int(won_24h),
        "outreach_lost_24h": int(lost_24h),
        "follow_ups_due": int(follow_ups_due),
        "outreach_conversion_by_play": conversion_by_play,
        **contact_metrics,
    }


def _list_outreach_readiness_cases(*, limit: int, blocked_only: bool) -> dict:
    ensure_outreach_execution_tables()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, created_at
                FROM merchant_opportunities
                WHERE status IN ('pending_review', 'approved', 'outreach_pending', 'outreach_sent')
                ORDER BY created_at DESC
                LIMIT 250
                """
            )
        ).mappings().fetchall()
        cases = []
        for row in rows:
            context = _get_outreach_context(conn, opportunity_id=row.get("id"))
            if not context:
                continue
            recommendation = _build_outreach_recommendation(context)
            if recommendation.get("queue_eligibility_class") != ELIGIBILITY_OUTREACH:
                continue
            if blocked_only and recommendation.get("contact_send_eligible"):
                continue
            if not blocked_only and not recommendation.get("contact_send_eligible"):
                continue
            if context.get("outcome_status") in {"won", "lost", "ignored"}:
                continue
            cases.append(
                {
                    "opportunity_id": context.get("opportunity_id"),
                    "merchant_id": context.get("merchant_id"),
                    "merchant_domain": context.get("merchant_domain") or "",
                    "merchant_name": context.get("merchant_name_display") or context.get("merchant_name") or "",
                    "contact_email": context.get("contact_email") or "",
                    "contact_name": context.get("contact_name") or "",
                    "contact_quality_label": recommendation.get("contact_quality_label") or "blocked",
                    "contact_state": recommendation.get("contact_state") or "no_contact_found",
                    "contact_trust_score": int(recommendation.get("contact_trust_score") or 0),
                    "contact_reason": recommendation.get("contact_reason") or "",
                    "contact_reason_codes": list(recommendation.get("contact_reason_codes") or []),
                    "contact_source": context.get("contact_source") or "",
                    "contact_source_class": context.get("contact_source_class") or "unknown_source",
                    "contact_source_explanation": context.get("contact_source_explanation") or "",
                    "contact_page_type": context.get("contact_page_type") or "",
                    "contact_page_url": context.get("contact_page_url") or "",
                    "contact_role_hint": context.get("contact_role_hint") or "",
                    "contact_why_better": context.get("contact_why_better") or "",
                    "contact_deepening_summary": context.get("contact_deepening_summary") or "",
                    "contact_deepening_outcome": context.get("contact_deepening_outcome") or "",
                    "next_contact_move": recommendation.get("next_contact_move") or "",
                    "processor": context.get("processor") or "unknown",
                    "distress_type": context.get("distress_type") or "unknown",
                    "urgency": recommendation.get("urgency") or "low",
                    "readiness_state": recommendation.get("readiness_state") or "blocked_contact_quality",
                    "queue_eligibility_class": recommendation.get("queue_eligibility_class") or ELIGIBILITY_OUTREACH,
                    "queue_eligibility_reason": recommendation.get("queue_eligibility_reason") or "",
                    "queue_quality_score": int(recommendation.get("queue_quality_score") or 0),
                    "queue_domain_quality_label": recommendation.get("queue_domain_quality_label") or "",
                    "queue_domain_quality_reason": recommendation.get("queue_domain_quality_reason") or "",
                    "outreach_status": context.get("outreach_status") or "no_outreach",
                    "approval_state": context.get("approval_state") or "approval_required",
                    "best_play": recommendation.get("best_play") or "clarify_distress",
                    "why_now": recommendation.get("why_now") or "",
                }
            )
    cases.sort(key=_outreach_readiness_sort_key, reverse=True)
    limited = cases[: max(1, int(limit))]
    return {"count": len(cases), "cases": limited}


def _compute_contact_quality_metrics() -> dict:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id
                FROM merchant_opportunities
                WHERE status IN ('pending_review', 'approved', 'outreach_pending', 'outreach_sent')
                ORDER BY created_at DESC
                LIMIT 250
                """
            )
        ).mappings().fetchall()
        counts = {
            "send_eligible_leads": 0,
            "blocked_no_contact": 0,
            "blocked_weak_contact": 0,
            "verified_contact_found": 0,
            "weak_contact_found": 0,
            "no_contact_found": 0,
        }
        quality_rows: list[dict] = []
        suppressed_live_24h = 0
        block_reasons: dict[str, int] = {}
        target_roles: dict[str, int] = {}
        for row in rows:
            context = _get_outreach_context(conn, opportunity_id=row.get("id"))
            if not context or context.get("outcome_status") in {"won", "lost", "ignored"}:
                continue
            quality_rows.append({"eligibility_class": context.get("queue_eligibility_class") or ""})
            if context.get("queue_eligibility_class") != ELIGIBILITY_OUTREACH:
                created_at = _ensure_utc_datetime(row.get("created_at"))
                if created_at and created_at >= datetime.now(timezone.utc) - timedelta(hours=24):
                    suppressed_live_24h += 1
                continue
            state = context.get("contact_state") or "no_contact_found"
            if state in counts:
                counts[state] += 1
            if context.get("contact_send_eligible"):
                counts["send_eligible_leads"] += 1
            else:
                if state == "no_contact_found":
                    counts["blocked_no_contact"] += 1
                else:
                    counts["blocked_weak_contact"] += 1
                block_reason = str(context.get("contact_reason_codes") or ["unknown"]).strip()
                block_reason = (context.get("contact_reason_codes") or ["unknown"])[0]
                block_reasons[block_reason] = block_reasons.get(block_reason, 0) + 1
            if context.get("distress_type") and context.get("best_target_roles"):
                top_role = (context.get("best_target_roles") or ["unknown"])[0]
                distress = normalize_distress_topic(context.get("distress_type"))
                key = f"{distress}:{top_role}"
                target_roles[key] = target_roles.get(key, 0) + 1
        deepening_rows = conn.execute(
            text(
                """
                SELECT data
                FROM events
                WHERE event_type = 'merchant_contact_deepening_run'
                  AND created_at >= NOW() - INTERVAL '24 hours'
                ORDER BY created_at DESC
                """
            )
        ).mappings().fetchall()
        latest_deepening_rows = conn.execute(
            text(
                """
                WITH ranked_runs AS (
                    SELECT
                        data,
                        ROW_NUMBER() OVER (
                            PARTITION BY (data::jsonb->>'merchant_id')
                            ORDER BY created_at DESC
                        ) AS row_rank
                    FROM events
                    WHERE event_type = 'merchant_contact_deepening_run'
                )
                SELECT data
                FROM ranked_runs
                WHERE row_rank = 1
                """
            )
        ).mappings().fetchall()
        suppressed_events_24h = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM events
                WHERE event_type = 'merchant_opportunity_suppressed'
                  AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
    top_block_reason = max(block_reasons, key=block_reasons.get) if block_reasons else ""
    top_target_role = max(target_roles, key=target_roles.get) if target_roles else ""
    quality_counts = summarize_quality_counts(quality_rows)
    deepening_runs_24h = len(deepening_rows)
    deepening_candidates_found_24h = 0
    deepening_promoted_contacts_24h = 0
    promoted_page_types: dict[str, int] = {}
    for row in deepening_rows:
        payload = _json_dict(row.get("data"))
        deepening_candidates_found_24h += int(payload.get("candidates_found") or 0)
        deepening_promoted_contacts_24h += int(payload.get("promoted_contacts") or 0)
        if int(payload.get("promoted_contacts") or 0) > 0:
            top_page_type = str(payload.get("top_page_type") or "").strip()
            if top_page_type:
                promoted_page_types[top_page_type] = promoted_page_types.get(top_page_type, 0) + 1
    deepening_blocked_no_contact = 0
    deepening_blocked_weak_contact = 0
    for row in latest_deepening_rows:
        payload = _json_dict(row.get("data"))
        outcome = str(payload.get("outcome") or "").strip()
        if outcome == "blocked_no_contact":
            deepening_blocked_no_contact += 1
        elif outcome == "blocked_weak_contact":
            deepening_blocked_weak_contact += 1
    top_page_type_for_promoted_contacts = max(promoted_page_types, key=promoted_page_types.get) if promoted_page_types else ""
    deepening_success_rate = round(
        deepening_promoted_contacts_24h / deepening_runs_24h,
        4,
    ) if deepening_runs_24h else 0.0
    return {
        **counts,
        **quality_counts,
        "top_block_reason": top_block_reason,
        "top_target_role_by_distress": top_target_role,
        "deepening_runs_24h": deepening_runs_24h,
        "deepening_candidates_found_24h": deepening_candidates_found_24h,
        "deepening_promoted_contacts_24h": deepening_promoted_contacts_24h,
        "deepening_blocked_no_contact": deepening_blocked_no_contact or counts["blocked_no_contact"],
        "deepening_blocked_weak_contact": deepening_blocked_weak_contact or counts["blocked_weak_contact"],
        "deepening_success_rate": deepening_success_rate,
        "top_page_type_for_promoted_contacts": top_page_type_for_promoted_contacts,
        "suppressed_from_blocked_queue_24h": int(suppressed_live_24h) + int(suppressed_events_24h),
    }


def _latest_contact_deepening_run(conn, *, merchant_id: int) -> dict:
    if not merchant_id or int(merchant_id) <= 0:
        return {}
    try:
        row = conn.execute(
            text(
                """
                SELECT data, created_at
                FROM events
                WHERE event_type = 'merchant_contact_deepening_run'
                  AND (data::jsonb->>'merchant_id')::BIGINT = :merchant_id
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"merchant_id": int(merchant_id)},
        ).mappings().first()
    except Exception:
        return {}
    if not row:
        return {}
    payload = _json_dict(row.get("data"))
    payload["created_at"] = _serialize_dt(row.get("created_at"))
    return payload


def _get_outreach_context(conn, *, opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict | None:
    if opportunity_id is not None:
        opportunity = conn.execute(
            text(
                """
                SELECT id, merchant_id, merchant_domain, processor, distress_topic, sales_strategy, status, created_at
                FROM merchant_opportunities
                WHERE id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
    elif merchant_domain:
        opportunity = conn.execute(
            text(
                """
                SELECT id, merchant_id, merchant_domain, processor, distress_topic, sales_strategy, status, created_at
                FROM merchant_opportunities
                WHERE merchant_domain = :merchant_domain
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"merchant_domain": merchant_domain},
        ).mappings().first()
    else:
        return None
    if not opportunity:
        return None

    merchant = conn.execute(
        text(
            """
            SELECT id, canonical_name, domain, industry, status, last_seen
            FROM merchants
            WHERE id = :merchant_id
               OR (COALESCE(:merchant_domain, '') != '' AND (domain = :merchant_domain OR normalized_domain = :merchant_domain))
            ORDER BY CASE WHEN id = :merchant_id THEN 0 ELSE 1 END, last_seen DESC NULLS LAST
            LIMIT 1
            """
        ),
        {"merchant_id": opportunity.get("merchant_id") or -1, "merchant_domain": opportunity.get("merchant_domain") or ""},
    ).mappings().first()

    signal = _safe_outreach_signal_lookup(
        conn,
        int((merchant or {}).get("id") or opportunity.get("merchant_id") or -1),
    )

    action_row = conn.execute(
        text(
            """
            SELECT recommended_action, selected_action, action_reason, updated_at
            FROM opportunity_operator_actions
            WHERE opportunity_id = :opportunity_id
            LIMIT 1
            """
        ),
        {"opportunity_id": int(opportunity["id"])},
    ).mappings().first()

    outreach_row = conn.execute(
        text(
            """
            SELECT *
            FROM opportunity_outreach_actions
            WHERE opportunity_id = :opportunity_id
            LIMIT 1
            """
        ),
        {"opportunity_id": int(opportunity["id"])},
    ).mappings().first()

    processor = opportunity.get("processor")
    if processor in {None, "", "unknown"}:
        processor = (
            (signal or {}).get("gmail_processor")
            or _infer_processor((signal or {}).get("content", ""))
            or _infer_processor(str(opportunity.get("sales_strategy") or ""))
            or "unknown"
        )
    distress_type = normalize_distress_topic(opportunity.get("distress_topic"))
    if distress_type == "unknown":
        distress_type = normalize_distress_topic((signal or {}).get("gmail_distress_type") or _infer_distress_type((signal or {}).get("content", "")))
    industry = (merchant or {}).get("industry") or (signal or {}).get("gmail_industry") or "unknown"
    queue_quality = evaluate_opportunity_queue_quality(
        opportunity={
            **dict(opportunity),
            "processor": processor,
            "distress_topic": distress_type,
        },
        merchant=dict(merchant or {}),
        signal={
            "content": (signal or {}).get("content") or "",
            "processor": processor,
        },
    )
    selected_play = normalize_operator_action((action_row or {}).get("selected_action") or operator_action_for_distress(distress_type))
    recommended_play = normalize_operator_action((action_row or {}).get("recommended_action") or operator_action_for_distress(distress_type))
    target_roles, target_role_reason = _target_roles_for_case(distress_type=distress_type, action=selected_play or recommended_play)
    merchant_domain_value = (merchant or {}).get("domain") or opportunity.get("merchant_domain") or ""
    contacts = _fetch_merchant_contacts(
        conn,
        merchant_id=(merchant or {}).get("id") or opportunity.get("merchant_id") or -1,
    )
    selected_contact, contact_assessment = _select_best_contact(
        contacts,
        merchant_domain=merchant_domain_value,
        sender_email=GmailAdapter().sender_email,
        context_action=selected_play or recommended_play,
        target_roles=target_roles,
    )
    deepening_run = _latest_contact_deepening_run(
        conn,
        merchant_id=(merchant or {}).get("id") or opportunity.get("merchant_id") or -1,
    )
    contact_assessment["next_contact_move"] = _next_contact_move(
        contact_assessment.get("reason_codes") or [],
        merchant_domain=merchant_domain_value,
        target_roles=target_roles,
        target_role_reason=target_role_reason,
        deepening_run=deepening_run,
    )
    contact_reason = contact_assessment.get("reason") or "No merchant contact email is on file yet."
    if deepening_run and not contact_assessment.get("send_eligible") and deepening_run.get("summary"):
        contact_reason = deepening_run.get("summary") or contact_reason
    merchant_name = (merchant or {}).get("canonical_name") or opportunity.get("merchant_domain") or "Unknown merchant"
    merchant_name_display = _display_merchant_name(merchant_name, merchant_domain_value)
    context = {
        "opportunity_id": int(opportunity["id"]),
        "merchant_id": (merchant or {}).get("id") or opportunity.get("merchant_id"),
        "merchant_name": merchant_name,
        "merchant_name_display": merchant_name_display,
        "merchant_domain": merchant_domain_value,
        "industry": industry or "unknown",
        "processor": processor or "unknown",
        "distress_type": distress_type or "unknown",
        "sales_strategy": opportunity.get("sales_strategy") or "",
        "opportunity_status": opportunity.get("status") or "pending_review",
        "signal_content": (signal or {}).get("content") or "",
        "signal_source": (signal or {}).get("source") or "",
        "signal_detected_at": _serialize_dt((signal or {}).get("detected_at")),
        "contact_name": (selected_contact or {}).get("contact_name") or "",
        "contact_email": (selected_contact or {}).get("email") or "",
        "contact_confidence": float((selected_contact or {}).get("confidence") or 0.0),
        "contact_source": (selected_contact or {}).get("source") or "",
        "contact_email_verified": bool((selected_contact or {}).get("email_verified")),
        "contact_page_type": (selected_contact or {}).get("page_type") or "",
        "contact_page_url": (selected_contact or {}).get("page_url") or "",
        "contact_role_hint": (selected_contact or {}).get("role_hint") or "",
        "contact_same_domain": bool((selected_contact or {}).get("same_domain")),
        "contact_explicit_on_site": bool((selected_contact or {}).get("explicit_on_site")),
        "contact_crawl_step": int((selected_contact or {}).get("crawl_step") or 0),
        "contact_send_eligible": bool(contact_assessment.get("send_eligible")),
        "contact_quality_label": contact_assessment.get("quality_label") or "blocked",
        "contact_state": contact_assessment.get("contact_state") or "no_contact_found",
        "contact_trust_score": int(contact_assessment.get("trust_score") or 0),
        "contact_reason_codes": list(contact_assessment.get("reason_codes") or []),
        "contact_reason": contact_reason,
        "contact_source_class": contact_assessment.get("source_class") or "unknown_source",
        "contact_source_quality_score": int(contact_assessment.get("source_quality_score") or 0),
        "contact_source_explanation": contact_assessment.get("source_explanation") or "",
        "contact_candidates_count": int(contact_assessment.get("candidate_count") or 0),
        "send_eligible_contact_count": int(contact_assessment.get("eligible_count") or 0),
        "contact_ranked_candidates": list(contact_assessment.get("top_contacts") or []),
        "next_contact_move": contact_assessment.get("next_contact_move") or "Find a trusted same-domain merchant contact before sending outreach.",
        "contact_page_priority": int(contact_assessment.get("page_priority") or 0),
        "contact_why_better": contact_assessment.get("source_explanation") or "",
        "contact_deepening_summary": deepening_run.get("summary") or "",
        "contact_deepening_outcome": deepening_run.get("outcome") or "",
        "contact_deepening_pages_checked": int(deepening_run.get("pages_checked") or 0),
        "contact_deepening_page_types_checked": list(deepening_run.get("page_types_checked") or []),
        "contact_deepening_last_run_at": deepening_run.get("created_at") or "",
        "contact_deepening_eligibility_reason": deepening_run.get("eligibility_reason") or "",
        "queue_eligibility_class": queue_quality.get("eligibility_class") or ELIGIBILITY_OUTREACH,
        "queue_eligibility_reason": queue_quality.get("eligibility_reason") or "",
        "queue_quality_score": int(queue_quality.get("quality_score") or 0),
        "queue_domain_quality_label": queue_quality.get("domain_quality_label") or "",
        "queue_domain_quality_reason": queue_quality.get("domain_quality_reason") or "",
        "queue_distress_quality_score": int(queue_quality.get("distress_quality_score") or 0),
        "queue_distress_quality_reason": queue_quality.get("distress_quality_reason") or "",
        "icp_fit_score": int(queue_quality.get("icp_fit_score") or 0),
        "icp_fit_label": queue_quality.get("icp_fit_label") or "",
        "icp_fit_reason": queue_quality.get("icp_fit_reason") or "",
        "high_conviction_prospect": bool(queue_quality.get("high_conviction_prospect")),
        "suppressed_from_blocked_queue": bool(queue_quality.get("suppressed_from_blocked_queue")),
        "best_target_roles": target_roles,
        "target_role_reason": target_role_reason,
        "selected_play": selected_play,
        "recommended_play": recommended_play,
        "operator_action_reason": (action_row or {}).get("action_reason") or "",
        "outreach_status": (outreach_row or {}).get("status") or "no_outreach",
        "approval_state": (outreach_row or {}).get("approval_state") or "approval_required",
        "gmail_thread_id": (outreach_row or {}).get("gmail_thread_id") or "",
        "sent_message_id": (outreach_row or {}).get("sent_message_id") or "",
        "sent_at": (outreach_row or {}).get("sent_at"),
        "replied_at": (outreach_row or {}).get("replied_at"),
        "follow_up_due_at": (outreach_row or {}).get("follow_up_due_at"),
        "outcome_status": (outreach_row or {}).get("outcome_status") or "pending",
        "notes": (outreach_row or {}).get("notes") or "",
    }
    commercial_why_now = " ".join(
        part
        for part in [
            contact_reason,
            target_role_reason,
            context.get("queue_eligibility_reason") or "",
        ]
        if part
    )
    commercial = assess_commercial_readiness(
        distress_type=distress_type,
        processor=processor,
        content=(signal or {}).get("content") or "",
        why_now=commercial_why_now,
        contact_email=context.get("contact_email") or "",
        contact_trust_score=int(context.get("contact_trust_score") or 0),
        target_roles=target_roles,
        target_role_reason=target_role_reason,
        icp_fit_score=int(context.get("icp_fit_score") or 0),
        queue_quality_score=int(context.get("queue_quality_score") or 0),
    )
    context["commercial_readiness_score"] = int(commercial.get("commercial_readiness_score") or 0)
    context["commercial_readiness_label"] = commercial.get("commercial_readiness_label") or "low"
    context["commercial_readiness_summary"] = commercial.get("urgency_summary") or ""
    live_thread_status = str(context.get("outreach_status") or "").strip().lower()
    if live_thread_status in {"sent", "replied", "follow_up_needed"} and (
        context.get("sent_message_id") or context.get("gmail_thread_id")
    ):
        context["queue_eligibility_class"] = ELIGIBILITY_OUTREACH
        context["queue_eligibility_reason"] = (
            "This is already a live merchant thread. Keep it in the active reply and follow-up lane."
        )
        context["icp_fit_score"] = max(int(context.get("icp_fit_score") or 0), 75)
        context["icp_fit_label"] = "strong"
        context["icp_fit_reason"] = (
            "This case is already a live merchant thread, so it remains an active high-priority PayFlux conversation."
        )
        context["high_conviction_prospect"] = True
    return context


def get_outreach_context_snapshot(*, opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict | None:
    with engine.connect() as conn:
        return _get_outreach_context(conn, opportunity_id=opportunity_id, merchant_domain=merchant_domain)


def _safe_signal_query(conn, merchant_id: int, *, mode: str) -> dict | None:
    if mode == "direct":
        query = text(
            """
            SELECT s.id, s.content, s.detected_at, s.source,
                   gti.processor AS gmail_processor,
                   gti.distress_type AS gmail_distress_type,
                   gti.industry AS gmail_industry
            FROM signals s
            LEFT JOIN gmail_thread_intelligence gti ON gti.signal_id = s.id
            WHERE s.merchant_id = :merchant_id
            ORDER BY s.detected_at DESC
            LIMIT 1
            """
        )
    else:
        query = text(
            """
            SELECT s.id, s.content, s.detected_at, s.source,
                   gti.processor AS gmail_processor,
                   gti.distress_type AS gmail_distress_type,
                   gti.industry AS gmail_industry
            FROM signals s
            LEFT JOIN gmail_thread_intelligence gti ON gti.signal_id = s.id
            WHERE s.id IN (
                SELECT ms.signal_id
                FROM merchant_signals ms
                WHERE ms.merchant_id = :merchant_id
            )
            ORDER BY s.detected_at DESC
            LIMIT 1
            """
        )
    try:
        row = conn.execute(query, {"merchant_id": int(merchant_id)}).mappings().first()
        return dict(row) if row else None
    except SQLAlchemyError as exc:
        lowered = str(exc).lower()
        if "deadlock detected" in lowered or "lock timeout" in lowered:
            return None
        raise


def _safe_outreach_signal_lookup(conn, merchant_id: int) -> dict | None:
    direct = _safe_signal_query(conn, merchant_id, mode="direct")
    if direct:
        return direct
    return _safe_signal_query(conn, merchant_id, mode="linked")


def _fetch_merchant_contacts(conn, *, merchant_id: int) -> list[dict]:
    if not merchant_id or int(merchant_id) <= 0:
        return []
    rich_query = text(
        """
        WITH ranked_contacts AS (
            SELECT
                COALESCE(contact_name, '') AS contact_name,
                email,
                COALESCE(source, '') AS source,
                COALESCE(confidence, 0) AS confidence,
                COALESCE(email_verified, FALSE) AS email_verified,
                COALESCE(local_part, '') AS local_part,
                COALESCE(email_domain, '') AS email_domain,
                COALESCE(page_type, '') AS page_type,
                COALESCE(page_url, '') AS page_url,
                COALESCE(role_hint, '') AS role_hint,
                COALESCE(same_domain, FALSE) AS same_domain,
                COALESCE(explicit_on_site, FALSE) AS explicit_on_site,
                COALESCE(crawl_step, 0) AS crawl_step,
                created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY LOWER(email)
                    ORDER BY
                        COALESCE(email_verified, FALSE) DESC,
                        COALESCE(explicit_on_site, FALSE) DESC,
                        COALESCE(confidence, 0) DESC,
                        created_at DESC
                ) AS row_rank
            FROM merchant_contacts
            WHERE merchant_id = :merchant_id
              AND email IS NOT NULL
              AND email != ''
        )
        SELECT
            contact_name,
            email,
            source,
            confidence,
            email_verified,
            local_part,
            email_domain,
            page_type,
            page_url,
            role_hint,
            same_domain,
            explicit_on_site,
            crawl_step,
            created_at
        FROM ranked_contacts
        WHERE row_rank = 1
        ORDER BY email_verified DESC, explicit_on_site DESC, confidence DESC NULLS LAST, created_at DESC
        """
    )
    fallback_query = text(
        """
        SELECT
            '' AS contact_name,
            email,
            '' AS source,
            COALESCE(confidence, 0) AS confidence,
            FALSE AS email_verified,
            '' AS local_part,
            '' AS email_domain,
            '' AS page_type,
            '' AS page_url,
            '' AS role_hint,
            FALSE AS same_domain,
            FALSE AS explicit_on_site,
            0 AS crawl_step,
            created_at
        FROM merchant_contacts
        WHERE merchant_id = :merchant_id
          AND email IS NOT NULL
          AND email != ''
        ORDER BY confidence DESC NULLS LAST, created_at DESC
        """
    )
    try:
        rows = conn.execute(rich_query, {"merchant_id": int(merchant_id)}).mappings().fetchall()
    except Exception:
        rows = conn.execute(fallback_query, {"merchant_id": int(merchant_id)}).mappings().fetchall()
    return [dict(row) for row in rows]


def _page_type_label(page_type: str) -> str:
    return PAGE_TYPE_LABELS.get(str(page_type or "").strip().lower(), "merchant page")


def _contact_page_priority(contact: dict, source: str) -> int:
    page_type = str((contact or {}).get("page_type") or "").strip().lower()
    if source == "website:mailto":
        return 5
    return PAGE_TYPE_PRIORITY.get(page_type, 6 if source.startswith("website:") else 7)


def _select_best_contact(
    contacts: list[dict],
    *,
    merchant_domain: str,
    sender_email: str | None,
    context_action: str | None = None,
    target_roles: list[str] | None = None,
) -> tuple[dict, dict]:
    if not contacts:
        return {}, {
            "send_eligible": False,
            "quality_label": "blocked",
            "contact_state": "no_contact_found",
            "reason_codes": ["no_contact_on_file"],
            "reason": "No merchant contact email is on file yet.",
            "candidate_count": 0,
            "eligible_count": 0,
            "top_contacts": [],
            "next_contact_move": _next_contact_move(["no_contact_on_file"], merchant_domain=merchant_domain),
        }

    scored = []
    for contact in contacts:
        assessment = _assess_contact_for_send(
            contact,
            merchant_domain=merchant_domain,
            sender_email=sender_email,
            context_action=context_action,
            target_roles=target_roles,
        )
        scored.append((contact, assessment))

    ranked_contacts = [
        _serialize_contact_candidate(contact, assessment)
        for contact, assessment in sorted(
            scored,
            key=lambda item: _contact_sort_key(item[0], item[1]),
            reverse=True,
        )[:5]
    ]

    eligible = [(contact, assessment) for contact, assessment in scored if assessment["send_eligible"]]
    if eligible:
        best_contact, best_assessment = sorted(
            eligible,
            key=lambda item: _contact_sort_key(item[0], item[1]),
            reverse=True,
        )[0]
        return best_contact, {
            **best_assessment,
            "candidate_count": len(scored),
            "eligible_count": len(eligible),
            "top_contacts": ranked_contacts,
        }

    best_contact, best_assessment = sorted(
        scored,
        key=lambda item: _contact_sort_key(item[0], item[1]),
        reverse=True,
    )[0]
    return best_contact, {
        **best_assessment,
        "candidate_count": len(scored),
        "eligible_count": 0,
        "top_contacts": ranked_contacts,
    }


def _assess_contact_for_send(
    contact: dict,
    *,
    merchant_domain: str,
    sender_email: str | None,
    context_action: str | None = None,
    target_roles: list[str] | None = None,
) -> dict:
    email = str((contact or {}).get("email") or "").strip().lower()
    source = str((contact or {}).get("source") or "").strip().lower()
    confidence = float((contact or {}).get("confidence") or 0.0)
    email_verified = bool((contact or {}).get("email_verified"))
    contact_name = str((contact or {}).get("contact_name") or "").strip()
    page_type = str((contact or {}).get("page_type") or "").strip().lower()
    page_url = str((contact or {}).get("page_url") or "").strip()
    role_hint = str((contact or {}).get("role_hint") or "").strip().lower()
    explicit_on_site = bool((contact or {}).get("explicit_on_site")) or source.startswith("website:")
    crawl_step = int((contact or {}).get("crawl_step") or 0) or _contact_page_priority(contact, source)
    local_part, domain = _split_email(email)
    merchant_domain = (merchant_domain or "").strip().lower()
    sender_domain = _split_email(sender_email or "")[1]
    local_normalized = local_part.replace("+", "").replace(".", "").replace("_", "").replace("-", "")
    same_domain = bool((contact or {}).get("same_domain")) if (contact or {}).get("same_domain") is not None else bool(
        domain and merchant_domain and (domain == merchant_domain or domain.endswith(f".{merchant_domain}"))
    )
    if not same_domain:
        same_domain = bool(domain and merchant_domain and (domain == merchant_domain or domain.endswith(f".{merchant_domain}")))
    same_sender_domain = bool(sender_domain and (domain == sender_domain or domain.endswith(f".{sender_domain}")))
    trusted_role = local_part in TRUSTED_ROLE_LOCAL_PARTS
    decision_role = local_part in DECISION_ROLE_LOCAL_PARTS
    named_contact = bool(contact_name) or _is_named_contact_local(local_part)
    blocked_role = local_part in GENERIC_BLOCKED_LOCAL_PARTS or any(pattern in local_part for pattern in ROLE_ADDRESS_PATTERNS)
    generated_like = local_normalized in GENERIC_BLOCKED_LOCAL_PARTS
    page_priority = _contact_page_priority(contact, source)
    source_class = _normalize_contact_source_class(
        source=source,
        same_domain=same_domain,
        email_verified=email_verified,
        trusted_role=trusted_role,
        blocked_role=blocked_role,
    )
    source_quality = SOURCE_QUALITY_SCORES.get(source_class, SOURCE_QUALITY_SCORES["unknown_source"])
    reason_codes: list[str] = []

    if not email or "@" not in email:
        reason_codes.append("invalid_email")
    if source.startswith("operator_validation") or "validation" in source or "sandbox" in source:
        reason_codes.append("validation_only_contact")
    if _recipient_is_denied(email, sender_email=sender_email):
        reason_codes.append("recipient_denied")
    if domain in INTERNAL_CONTACT_DOMAINS or same_sender_domain:
        reason_codes.append("internal_contact")
    if domain in FREE_EMAIL_DOMAINS:
        reason_codes.append("freemail_domain")
    if not merchant_domain:
        reason_codes.append("merchant_domain_unresolved")
    elif not same_domain:
        reason_codes.append("domain_mismatch")
    if blocked_role or generated_like:
        reason_codes.append("placeholder_or_generated_address")

    role_aligned = _contact_role_aligned(
        local_part,
        role_hint=role_hint,
        context_action=context_action,
        target_roles=target_roles,
    )
    generic_fallback_ok = bool(
        trusted_role
        and same_domain
        and explicit_on_site
        and confidence >= USABLE_FALLBACK_CONTACT_THRESHOLD
        and page_type in GENERIC_FALLBACK_PAGE_TYPES
    )
    if source_class in {"pattern_enumeration", "guessed_address"} and not email_verified and not generic_fallback_ok and confidence < HIGH_CONFIDENCE_CONTACT_THRESHOLD:
        reason_codes.append("weak_source_quality")
    if not email_verified and not generic_fallback_ok and confidence < HIGH_CONFIDENCE_CONTACT_THRESHOLD:
        reason_codes.append("unverified_low_confidence")
    if not explicit_on_site and source_class in {"unknown_source", "public_directory"}:
        reason_codes.append("weak_source_quality")
    if trusted_role and not role_aligned and not generic_fallback_ok:
        reason_codes.append("generic_inbox_role_mismatch")
    send_eligible = bool(
        email
        and same_domain
        and not reason_codes
        and (email_verified or confidence >= HIGH_CONFIDENCE_CONTACT_THRESHOLD or generic_fallback_ok)
        and (explicit_on_site or email_verified)
        and (
            decision_role
            or named_contact
            or (trusted_role and (role_aligned or generic_fallback_ok))
        )
    )

    trust_score = _contact_trust_score(
        confidence=confidence,
        email_verified=email_verified,
        same_domain=same_domain,
        explicit_on_site=explicit_on_site,
        trusted_role=trusted_role,
        named_contact=named_contact,
        decision_role=decision_role,
        role_aligned=role_aligned,
        blocked_role=blocked_role,
        page_priority=page_priority,
        crawl_step=crawl_step,
        source_quality=source_quality,
        reason_codes=reason_codes,
    )
    if send_eligible and email_verified:
        quality_label = "trusted"
        contact_state = "verified_contact_found"
    elif send_eligible:
        quality_label = "high_confidence"
        contact_state = "verified_contact_found"
    elif email:
        quality_label = "blocked"
        contact_state = "weak_contact_found"
    else:
        quality_label = "blocked"
        contact_state = "no_contact_found"
    reason = _contact_reason_from_codes(
        reason_codes,
        merchant_domain=merchant_domain,
        email=email,
        confidence=confidence,
        email_verified=email_verified,
        page_type=page_type,
        role_hint=role_hint,
    )
    return {
        "send_eligible": send_eligible,
        "quality_label": quality_label,
        "contact_state": contact_state,
        "trust_score": trust_score,
        "source_class": source_class,
        "source_quality_score": source_quality,
        "reason_codes": reason_codes if reason_codes else (["verified_same_domain_contact"] if email_verified else ["high_confidence_same_domain_contact"]),
        "reason": reason,
        "next_contact_move": _next_contact_move(reason_codes, merchant_domain=merchant_domain),
        "source_explanation": _contact_source_explanation(
            source_class,
            raw_source=source,
            email_verified=email_verified,
            same_domain=same_domain,
            trusted_role=trusted_role,
            decision_role=decision_role,
            page_type=page_type,
            page_url=page_url,
            role_hint=role_hint,
            explicit_on_site=explicit_on_site,
        ),
        "role_alignment": role_aligned,
        "named_contact": named_contact,
        "page_priority": page_priority,
        "page_type": page_type,
        "page_url": page_url,
        "role_hint": role_hint,
        "explicit_on_site": explicit_on_site,
        "crawl_step": crawl_step,
    }


def _contact_reason_from_codes(
    reason_codes: list[str],
    *,
    merchant_domain: str,
    email: str,
    confidence: float,
    email_verified: bool,
    page_type: str = "",
    role_hint: str = "",
) -> str:
    if not reason_codes:
        page_phrase = _page_type_label(page_type)
        if email_verified:
            return f"{email} is a verified same-domain merchant contact from the merchant {page_phrase}."
        if confidence >= HIGH_CONFIDENCE_CONTACT_THRESHOLD:
            return f"{email} is a high-confidence same-domain merchant contact from the merchant {page_phrase}."
        if confidence >= USABLE_FALLBACK_CONTACT_THRESHOLD:
            return f"{email} is a usable same-domain merchant inbox from the merchant {page_phrase}, even though it is not SMTP-verified yet."
        return f"{email} looks like a usable merchant contact."
    mapping = {
        "no_contact_on_file": "No merchant contact email is on file yet.",
        "invalid_email": "The saved contact email is not usable yet.",
        "validation_only_contact": "This contact is marked as validation-only, so it is blocked from live outreach.",
        "internal_contact": "This contact routes back to an internal PayFlux mailbox, so it is blocked for merchant outreach.",
        "recipient_denied": "This contact is on the outreach denylist, so it is blocked for merchant outreach.",
        "freemail_domain": "This contact uses a freemail domain, so merchant ownership is too weak for live outreach.",
        "merchant_domain_unresolved": "The merchant domain is not resolved cleanly enough to trust this contact for live outreach.",
        "domain_mismatch": f"This contact does not clearly belong to {merchant_domain or 'the merchant domain'}.",
        "placeholder_or_generated_address": "This contact looks generated or too generic for safe merchant outreach.",
        "weak_source_quality": "This contact source is weak and still needs verification before live outreach.",
        "unverified_low_confidence": "This contact is not verified and is below the trust threshold for live outreach.",
        "generic_inbox_role_mismatch": (
            f"This inbox is merchant-owned but the page context does not align it to the target role"
            f"{f' ({role_hint})' if role_hint else ''} for this case."
        ),
    }
    return mapping.get(reason_codes[0], "This contact is not trusted enough for live outreach yet.")


def _normalize_contact_source_class(
    *,
    source: str,
    same_domain: bool,
    email_verified: bool,
    trusted_role: bool,
    blocked_role: bool,
) -> str:
    normalized = (source or "").strip().lower()
    if normalized.startswith("operator_validation"):
        return "operator_validation"
    if email_verified and same_domain:
        return "verified_same_domain"
    if normalized.startswith("website:") or normalized in {"merchant_website", "website"}:
        return "merchant_website"
    if same_domain and trusted_role:
        return "role_inbox_same_domain"
    if "directory" in normalized or normalized in {"public_directory", "apollo", "zoominfo", "crunchbase"}:
        return "public_directory"
    if normalized.startswith("guessed") or (normalized == "pattern_enumeration" and blocked_role):
        return "guessed_address"
    if normalized == "pattern_enumeration":
        return "pattern_enumeration"
    return "unknown_source"


def _contact_source_explanation(
    source_class: str,
    *,
    raw_source: str = "",
    email_verified: bool,
    same_domain: bool,
    trusted_role: bool,
    decision_role: bool,
    page_type: str = "",
    page_url: str = "",
    role_hint: str = "",
    explicit_on_site: bool = False,
) -> str:
    normalized_source = (raw_source or "").strip().lower()
    page_phrase = _page_type_label(page_type)
    role_phrase = f" It aligns to a {role_hint} role." if role_hint else ""
    page_url_phrase = f" Source page: {page_url}." if page_url else ""
    if source_class == "verified_same_domain":
        if normalized_source == "website:mailto":
            return f"This address came from a same-domain mailto link on the merchant {page_phrase} and verified cleanly against the merchant domain.{role_phrase}{page_url_phrase}"
        if normalized_source.startswith("website:"):
            return f"This address came from the merchant {page_phrase} and verified cleanly against the merchant domain.{role_phrase}{page_url_phrase}"
        return "This address came from a verified same-domain merchant-owned source."
    if source_class == "merchant_website":
        if normalized_source == "website:mailto":
            return f"This address came from a same-domain mailto link on the merchant {page_phrase}.{role_phrase}{page_url_phrase}"
        if explicit_on_site or normalized_source.startswith("website:"):
            return f"This address came from the merchant {page_phrase}.{role_phrase}{page_url_phrase}"
        return "This address came from the merchant website."
    if source_class == "role_inbox_same_domain":
        return f"This inbox is same-domain and merchant-owned. It can be used as a fallback manual outreach path when no stronger named contact exists.{page_url_phrase}"
    if source_class == "public_directory":
        return "This address came from a public directory source and still needs merchant verification."
    if source_class == "pattern_enumeration":
        return "This inbox is same-domain but generic and not yet verified."
    if source_class == "guessed_address":
        return "This address appears guessed and is not verified."
    if source_class == "operator_validation":
        return "This address was created for operator validation only and cannot be used for live outreach."
    if email_verified and same_domain:
        return "This address appears merchant-owned and verified."
    if trusted_role or decision_role:
        return "This looks like a plausible same-domain merchant contact, but the source quality is still weak."
    return "This contact source is weak or unresolved."


def _target_roles_for_case(*, distress_type: str | None, action: str | None) -> tuple[list[str], str]:
    normalized_action = normalize_operator_action(action or operator_action_for_distress(distress_type))
    roles, reason = PLAY_TARGET_ROLES.get(normalized_action, PLAY_TARGET_ROLES["clarify_distress"])
    return list(roles), reason


def _contact_role_aligned(
    local_part: str,
    *,
    role_hint: str = "",
    context_action: str | None = None,
    target_roles: list[str] | None = None,
) -> bool:
    normalized_action = normalize_operator_action(context_action)
    active_target_roles = list(target_roles or [])
    if not active_target_roles:
        active_target_roles, _ = _target_roles_for_case(distress_type="unknown", action=normalized_action)
    normalized_targets = {
        role.replace(" ", "_").replace("-", "_").strip().lower()
        for role in active_target_roles
        if role
    }
    normalized_local = str(local_part or "").strip().lower()
    normalized_hint = str(role_hint or "").strip().lower().replace(" ", "_").replace("-", "_")
    alias_map = {
        "ops": "operations",
        "operation": "operations",
        "payment": "payments",
        "support_leadership": "support",
    }
    if normalized_local in normalized_targets or normalized_hint in normalized_targets:
        return True
    return bool(alias_map.get(normalized_local) in normalized_targets or alias_map.get(normalized_hint) in normalized_targets)


def _next_contact_move(
    reason_codes: list[str],
    *,
    merchant_domain: str,
    target_roles: list[str] | None = None,
    target_role_reason: str = "",
    deepening_run: dict | None = None,
) -> str:
    preferred_role = _preferred_contact_role_hint(target_roles or [])
    if not reason_codes or reason_codes[0] in {"verified_same_domain_contact", "high_confidence_same_domain_contact"}:
        return "Use the trusted merchant-owned contact already on file."
    if deepening_run and deepening_run.get("outcome") in {"blocked_no_contact", "blocked_weak_contact"}:
        summary = deepening_run.get("summary") or ""
        checked = ", ".join(deepening_run.get("page_types_checked") or []) or DEEPENING_SEQUENCE_LABEL
        return (
            f"merchant_contact_deepening already checked {checked} for {merchant_domain or 'the merchant domain'}. "
            f"{summary} Next move: hold the lead for operator validation or a stronger merchant-owned contact."
        )
    primary = reason_codes[0]
    if primary == "no_contact_on_file":
        return (
            f"Run merchant_contact_deepening across {DEEPENING_SEQUENCE_LABEL} at {merchant_domain or 'the merchant domain'} "
            f"to find a same-domain {preferred_role} contact because {target_role_reason or 'this case needs a merchant operator who can confirm the issue'}."
        )
    if primary in {"unverified_low_confidence", "placeholder_or_generated_address", "weak_source_quality", "generic_inbox_role_mismatch"}:
        return (
            f"Re-run merchant_contact_deepening across {DEEPENING_SEQUENCE_LABEL} at {merchant_domain or 'the merchant domain'} "
            f"and verify a real same-domain {preferred_role} contact because {target_role_reason or 'the current address is too weak for live outreach'}."
        )
    if primary in {"domain_mismatch", "freemail_domain"}:
        return (
            f"Replace this with a merchant-owned same-domain {preferred_role} inbox at {merchant_domain or 'the merchant domain'} "
            f"because {target_role_reason or 'merchant ownership is not clear enough yet'}."
        )
    if primary == "merchant_domain_unresolved":
        return "Resolve the merchant domain first, then look for a same-domain operator contact."
    if primary in {"internal_contact", "validation_only_contact"}:
        return (
            f"Remove the internal or validation contact and find a real {preferred_role} inbox at {merchant_domain or 'the merchant domain'}."
        )
    return f"Find a trusted same-domain {preferred_role} contact before moving this lead into outreach."


def _preferred_contact_role_hint(target_roles: list[str]) -> str:
    if not target_roles:
        return "finance or operations"
    if len(target_roles) == 1:
        return target_roles[0]
    return f"{target_roles[0]} or {target_roles[1]}"


def _contact_trust_score(
    *,
    confidence: float,
    email_verified: bool,
    same_domain: bool,
    explicit_on_site: bool,
    trusted_role: bool,
    named_contact: bool,
    decision_role: bool,
    role_aligned: bool,
    blocked_role: bool,
    page_priority: int,
    crawl_step: int,
    source_quality: int,
    reason_codes: list[str],
) -> int:
    score = 0
    if same_domain:
        score += 35
    if email_verified:
        score += 35
    if explicit_on_site:
        score += 10
    score += min(20, int(round(source_quality / 5)))
    score += min(25, int(round(confidence * 25)))
    if trusted_role:
        score += 5
    if named_contact:
        score += 6
    if decision_role:
        score += 8
    if role_aligned:
        score += 8
    if blocked_role:
        score -= 10
    if page_priority == 1:
        score += 6
    elif page_priority == 2:
        score += 4
    elif page_priority >= 5:
        score -= 2
    if crawl_step == 5:
        score -= 1
    score -= min(30, 8 * len(reason_codes))
    return max(0, min(100, score))


def _serialize_contact_candidate(contact: dict, assessment: dict) -> dict:
    return {
        "contact_name": (contact or {}).get("contact_name") or "",
        "email": (contact or {}).get("email") or "",
        "source": (contact or {}).get("source") or "",
        "page_type": (contact or {}).get("page_type") or "",
        "page_url": (contact or {}).get("page_url") or "",
        "role_hint": (contact or {}).get("role_hint") or "",
        "source_class": assessment.get("source_class") or "unknown_source",
        "source_quality_score": int(assessment.get("source_quality_score") or 0),
        "source_explanation": assessment.get("source_explanation") or "",
        "confidence": float((contact or {}).get("confidence") or 0.0),
        "email_verified": bool((contact or {}).get("email_verified")),
        "explicit_on_site": bool((contact or {}).get("explicit_on_site")),
        "same_domain": bool((contact or {}).get("same_domain")),
        "named_contact": bool(assessment.get("named_contact")),
        "role_alignment": bool(assessment.get("role_alignment")),
        "send_eligible": bool(assessment.get("send_eligible")),
        "contact_state": assessment.get("contact_state") or "weak_contact_found",
        "quality_label": assessment.get("quality_label") or "blocked",
        "trust_score": int(assessment.get("trust_score") or 0),
        "reason": assessment.get("reason") or "",
        "reason_codes": list(assessment.get("reason_codes") or []),
        "why_better": assessment.get("source_explanation") or "",
    }


def _contact_sort_key(contact: dict, assessment: dict) -> tuple:
    local_part, _ = _split_email(contact.get("email") or "")
    page_priority = _contact_page_priority(contact, str(contact.get("source") or "").strip().lower())
    return (
        1 if assessment.get("send_eligible") else 0,
        1 if local_part in DECISION_ROLE_LOCAL_PARTS and contact.get("email_verified") else 0,
        1 if assessment.get("named_contact") and contact.get("email_verified") else 0,
        1 if assessment.get("role_alignment") else 0,
        1 if bool(contact.get("explicit_on_site")) else 0,
        int(assessment.get("trust_score") or 0),
        100 - page_priority,
        int(assessment.get("source_quality_score") or 0),
        1 if contact.get("email_verified") else 0,
        float(contact.get("confidence") or 0.0),
        1 if _split_email(contact.get("email") or "")[0] in TRUSTED_ROLE_LOCAL_PARTS else 0,
    )


def _build_outreach_recommendation(context: dict) -> dict:
    distress = normalize_distress_topic(context.get("distress_type"))
    play = normalize_operator_action(context.get("selected_play") or context.get("recommended_play"))
    queue_class = str(context.get("queue_eligibility_class") or ELIGIBILITY_OUTREACH)
    queue_reason = context.get("queue_eligibility_reason") or ""
    queue_quality_score = int(context.get("queue_quality_score") or 0)
    queue_outreach_eligible = queue_class == ELIGIBILITY_OUTREACH
    channel = "gmail" if queue_outreach_eligible and context.get("contact_send_eligible") else "wait"
    urgency = URGENCY_MAP.get(distress, "low")
    strategy = strategy_for_distress(distress)
    outreach_status = (context.get("outreach_status") or "no_outreach").strip().lower()
    approval_state = (context.get("approval_state") or "approval_required").strip().lower()
    doctrine = doctrine_priority_reason(
        processor=context.get("processor"),
        distress_type=distress,
        industry=context.get("industry"),
    )
    should_proceed_now = (
        channel == "gmail"
        and context.get("opportunity_status") in ACTIVE_OUTREACH_OPPORTUNITY_STATUSES
        and context.get("outcome_status") not in {"won", "lost", "ignored"}
    )
    wait_reason = ""
    if not queue_outreach_eligible:
        wait_reason = queue_reason or "This opportunity is not eligible for blocked outreach work."
    elif channel != "gmail":
        wait_reason = context.get("contact_reason") or "No trusted merchant contact email is on file yet."
    elif context.get("opportunity_status") in {"rejected", "converted"}:
        should_proceed_now = False
        wait_reason = f"Opportunity is already {context.get('opportunity_status')}."
    elif context.get("outcome_status") in {"won", "lost", "ignored"}:
        should_proceed_now = False
        wait_reason = f"Outreach is already closed as {context.get('outcome_status')}."
    elif outreach_status == "awaiting_approval":
        should_proceed_now = True
        wait_reason = ""
    elif outreach_status == "draft_ready":
        should_proceed_now = True
        wait_reason = ""
    elif outreach_status == "sent":
        should_proceed_now = False
        wait_reason = "Outreach has already been sent and no follow-up is due yet."
    elif outreach_status == "replied":
        should_proceed_now = False
        wait_reason = "The merchant has replied, so the next move is to review that thread before sending anything else."
    if play == "clarify_distress" and distress == "unknown" and queue_quality_score < 55:
        should_proceed_now = False
        wait_reason = "The signal is too weak to justify a clarify-distress outreach move yet."
    readiness_state = "blocked_contact_quality"
    if not queue_outreach_eligible:
        readiness_state = queue_class
    elif channel == "gmail" and should_proceed_now:
        readiness_state = "send_ready"
    if outreach_status == "awaiting_approval":
        readiness_state = "approval_ready"
    elif outreach_status == "draft_ready" and approval_state == "approved":
        readiness_state = "send_ready"
    elif outreach_status == "draft_ready":
        readiness_state = "approval_ready"
    elif outreach_status in {"sent", "replied", "follow_up_needed"}:
        readiness_state = outreach_status
    why_now = " ".join(
        part
        for part in [
            doctrine,
            _signal_context_line(context),
            _contact_context_line(context),
        ]
        if part
    ).strip()
    return {
        "best_channel": channel,
        "best_play": play,
        "best_play_label": operator_action_label(play),
        "urgency": urgency,
        "strategy": strategy,
        "why_now": why_now,
        "should_proceed_now": bool(should_proceed_now),
        "wait_reason": wait_reason,
        "mode": get_current_outreach_mode("gmail"),
        "contact_send_eligible": bool(context.get("contact_send_eligible")),
        "contact_quality_label": context.get("contact_quality_label") or "blocked",
        "contact_state": context.get("contact_state") or "no_contact_found",
        "contact_trust_score": int(context.get("contact_trust_score") or 0),
        "contact_reason_codes": list(context.get("contact_reason_codes") or []),
        "contact_reason": context.get("contact_reason") or "",
        "next_contact_move": context.get("next_contact_move") or "",
        "queue_eligibility_class": queue_class,
        "queue_eligibility_reason": queue_reason,
        "queue_quality_score": int(context.get("queue_quality_score") or 0),
        "queue_domain_quality_label": context.get("queue_domain_quality_label") or "",
        "queue_domain_quality_reason": context.get("queue_domain_quality_reason") or "",
        "readiness_state": readiness_state,
        "approval_ready": readiness_state == "approval_ready",
        "send_ready": readiness_state == "send_ready",
    }


def _auto_send_confidence(context: dict, recommendation: dict, *, outreach_type: str) -> float:
    contact_trust = min(100, max(0, int(context.get("contact_trust_score") or 0)))
    commercial = min(100, max(0, int(context.get("commercial_readiness_score") or 0)))
    icp_fit = min(100, max(0, int(context.get("icp_fit_score") or 0)))
    queue_quality = min(100, max(0, int(context.get("queue_quality_score") or 0)))
    distress_known = 1.0 if normalize_distress_topic(context.get("distress_type")) != "unknown" else 0.0
    processor_known = 1.0 if str(context.get("processor") or "").strip().lower() not in {"", "unknown"} else 0.0
    signal_confident = 1.0 if _high_confidence_signal(context) else 0.0
    follow_up_bonus = 0.05 if outreach_type == "follow_up_nudge" else 0.0
    score = (
        (contact_trust / 100.0) * 0.35
        + (commercial / 100.0) * 0.25
        + (max(icp_fit, queue_quality) / 100.0) * 0.20
        + distress_known * 0.10
        + processor_known * 0.05
        + signal_confident * 0.05
        + follow_up_bonus
    )
    return round(min(1.0, max(0.0, score)), 4)


def evaluate_auto_send_readiness(context: dict, recommendation: dict, *, outreach_type: str) -> dict:
    mode = get_current_outreach_mode("gmail")
    if mode != "auto_send_high_confidence":
        return {"eligible": False, "reason": "gmail_channel_not_in_auto_send_mode", "confidence": 0.0, "mode": mode}

    if recommendation.get("best_channel") != "gmail":
        return {"eligible": False, "reason": "best_channel_not_gmail", "confidence": 0.0, "mode": mode}

    if not recommendation.get("should_proceed_now"):
        return {
            "eligible": False,
            "reason": recommendation.get("wait_reason") or "outreach_should_wait",
            "confidence": 0.0,
            "mode": mode,
        }

    if outreach_type not in {"initial_outreach", "follow_up_nudge"}:
        return {"eligible": False, "reason": "outreach_type_requires_operator_review", "confidence": 0.0, "mode": mode}

    distress = normalize_distress_topic(context.get("distress_type"))
    if distress == "unknown":
        return {"eligible": False, "reason": "distress_ambiguous", "confidence": 0.0, "mode": mode}

    if normalize_operator_action(recommendation.get("best_play")) == "clarify_distress":
        return {"eligible": False, "reason": "clarify_distress_requires_operator_review", "confidence": 0.0, "mode": mode}

    contact_trust = int(context.get("contact_trust_score") or 0)
    commercial = int(context.get("commercial_readiness_score") or 0)
    icp_fit = int(context.get("icp_fit_score") or 0)
    queue_quality = int(context.get("queue_quality_score") or 0)
    if contact_trust < AUTO_SEND_CONTACT_TRUST_MIN:
        return {"eligible": False, "reason": "contact_trust_below_auto_send_min", "confidence": 0.0, "mode": mode}
    if commercial < AUTO_SEND_COMMERCIAL_MIN:
        return {"eligible": False, "reason": "commercial_readiness_below_auto_send_min", "confidence": 0.0, "mode": mode}
    if queue_quality < AUTO_SEND_QUEUE_QUALITY_MIN:
        return {"eligible": False, "reason": "queue_quality_below_auto_send_min", "confidence": 0.0, "mode": mode}
    if icp_fit < AUTO_SEND_ICP_MIN:
        return {"eligible": False, "reason": "icp_fit_below_auto_send_min", "confidence": 0.0, "mode": mode}

    confidence = _auto_send_confidence(context, recommendation, outreach_type=outreach_type)
    if confidence < AUTO_SEND_CONFIDENCE_THRESHOLD:
        return {"eligible": False, "reason": "auto_send_confidence_below_threshold", "confidence": confidence, "mode": mode}

    return {
        "eligible": True,
        "reason": "auto_send_high_confidence_ready",
        "confidence": confidence,
        "mode": mode,
    }


def _build_outreach_artifact(context: dict, recommendation: dict, outreach_type: str) -> dict:
    merchant_label = context.get("merchant_name_display") or context.get("merchant_name") or context.get("merchant_domain") or "there"
    contact_name = _display_contact_name(context.get("contact_name"), merchant_label)
    processor = _display_processor_name(context.get("processor")) or "your processor"
    distress = normalize_distress_topic(context.get("distress_type"))
    distress_label = _distress_phrase(distress)
    strategy = strategy_for_distress(distress)
    rationale = recommendation.get("why_now") or ""
    next_step_offer = _merchant_next_step_offer(recommendation, distress)
    confidence_opening = _confidence_opening(context, processor=processor, distress_label=distress_label, merchant_label=merchant_label)
    if outreach_type == "reply_follow_up":
        subject = f"Re: {_subject_for_distress(context)}"
        body = (
            f"Hi {contact_name},\n\n"
            f"Thanks for the reply. Based on what you shared about {processor} and {distress_label}, "
            f"the most practical next step is usually {strategy}.\n\n"
            "What PayFlux is built for in cases like this is helping merchants see whether the pressure is getting worse, what changed, and what to do next before it turns into a bigger cash-flow problem.\n\n"
            "To make this useful quickly, which matters more right now:\n"
            "1. Restoring payouts or processing this week\n"
            "2. Reducing reserve, review, or account-risk pressure\n\n"
            f"If you want, I can send back {next_step_offer} for that path. If this is actively affecting payouts, reserves, or processing, the next step is usually PayFlux Pro (https://payflux.dev/upgrade) for live monitoring and earlier warnings.\n\n"
            "Best,\nPayFlux"
        )
    elif outreach_type == "follow_up_nudge":
        subject = _subject_for_distress(context)
        body = (
            f"Hi {contact_name},\n\n"
            f"Following up because {processor} {distress_label} usually gets more expensive once it stays live for a few days.\n\n"
            "One quick question: is this still active this week?\n\n"
            f"If it is, I can send back {next_step_offer} so you can see what changed and what to do next without sitting through a long pitch.\n\n"
            "If you just want a low-friction first step, I can point you to the free snapshot (https://payflux.dev/scan). If the issue is already active enough that you need ongoing visibility, the next move is Pro (https://payflux.dev/upgrade).\n\n"
            "Best,\nPayFlux"
        )
    else:
        subject = _subject_for_distress(context)
        body = (
            f"Hi {contact_name},\n\n"
            f"{confidence_opening}\n\n"
            "PayFlux is built for merchants who need to see whether processor pressure is getting worse, what changed, and what to do next before cash flow gets squeezed.\n\n"
            f"When that is tied to payout or processing continuity, the most useful next step is usually {strategy}.\n\n"
            "Two focused questions so I do not guess:\n"
            "1. Is the issue mainly frozen processing, delayed payouts, or reserve / review pressure?\n"
            "2. Are you trying to stabilize the current processor, or evaluate a replacement quickly?\n\n"
            f"If helpful, I can send back {next_step_offer}. If you prefer to start lighter, there is also a free snapshot (https://payflux.dev/scan) before stepping into live monitoring. If the issue is active enough that you need ongoing visibility, the next step is PayFlux Pro (https://payflux.dev/upgrade).\n\n"
            "Best,\nPayFlux"
        )
    return {"subject": subject, "body": body, "notes": rationale}


def _normalize_rewrite_style(style: str) -> str:
    normalized = str(style or "").strip().lower().replace("-", "_")
    allowed = {"sharper", "softer", "shorter", "direct", "standard"}
    return normalized if normalized in allowed else "standard"


def _rewrite_outreach_artifact(
    context: dict,
    recommendation: dict,
    *,
    outreach_type: str,
    subject: str,
    body: str,
    style: str,
    instructions: str = "",
) -> dict:
    merchant_label = context.get("merchant_name_display") or context.get("merchant_name") or context.get("merchant_domain") or "there"
    contact_name = _display_contact_name(context.get("contact_name"), merchant_label)
    processor = _display_processor_name(context.get("processor")) or "your processor"
    distress = normalize_distress_topic(context.get("distress_type"))
    distress_label = _distress_phrase(distress)
    strategy = strategy_for_distress(distress)
    next_step_offer = _merchant_next_step_offer(recommendation, distress)
    confidence_opening = _confidence_opening(context, processor=processor, distress_label=distress_label, merchant_label=merchant_label)

    rewritten_subject = subject or _subject_for_distress(context)
    rewritten_body = body or _build_outreach_artifact(context, recommendation, outreach_type).get("body", "")

    if style == "shorter":
        rewritten_body = (
            f"Hi {contact_name},\n\n"
            f"{confidence_opening}\n\n"
            f"If this is still live, the fastest next step is usually {strategy}. PayFlux helps merchants see what changed and what to do next before cash flow gets squeezed. If helpful, I can send back {next_step_offer}, point you to the free snapshot (https://payflux.dev/scan), or move you toward Pro (https://payflux.dev/upgrade) if the issue is active enough.\n\n"
            "Best,\nPayFlux"
        )
    elif style == "sharper":
        rewritten_body = (
            f"Hi {contact_name},\n\n"
            f"{confidence_opening}\n\n"
            "These cases usually get more expensive once payout, review, or chargeback pressure stays unresolved.\n\n"
            "PayFlux is built to help merchants see that pressure earlier, understand what changed, and know what to do next before it turns into a cash-flow problem.\n\n"
            "Two quick questions so I can be precise:\n"
            "1. Is this primarily payout delay, reserve/review pressure, or full processing disruption?\n"
            "2. Are you trying to stabilize the current processor this week, or move fast on an alternative?\n\n"
            f"Reply with the current blocker and I’ll send back {next_step_offer}. If the issue is truly live, the next step is PayFlux Pro (https://payflux.dev/upgrade) for ongoing monitoring and earlier warnings.\n\n"
            "Best,\nPayFlux"
        )
    elif style == "softer":
        rewritten_body = (
            f"Hi {contact_name},\n\n"
            f"{confidence_opening}\n\n"
            f"When these issues affect payout continuity, the most useful next step is often {strategy}. PayFlux is meant to help merchants get earlier clarity on what changed and what to do next. If this is still relevant, I can send back {next_step_offer}, point you to the free snapshot (https://payflux.dev/scan) first, or move you toward Pro (https://payflux.dev/upgrade) if this is already live.\n\n"
            "Best,\nPayFlux"
        )
    elif style == "direct":
        rewritten_body = (
            f"Hi {contact_name},\n\n"
            f"It looks like {processor} {distress_label} may be putting cash flow at risk.\n\n"
            f"If that is active, the next move is usually {strategy}. PayFlux helps merchants see what changed and what to do next before the pressure gets worse. Reply with the current blocker and I’ll send back {next_step_offer}. If it is actively live, the next move is Pro (https://payflux.dev/upgrade).\n\n"
            "Best,\nPayFlux"
        )

    notes = recommendation.get("why_now") or ""
    if instructions and instructions.strip():
        notes = f"{notes} | rewrite: {instructions.strip()}".strip(" |")
    return {"subject": rewritten_subject, "body": rewritten_body, "notes": notes}


def _build_outreach_learning_summary(*, context: dict, outcome_status: str, notes: str = "") -> str:
    merchant = context.get("merchant_domain") or context.get("merchant_name_display") or "merchant"
    distress = str(context.get("distress_type") or "unknown").replace("_", " ")
    play = str(context.get("selected_play") or context.get("recommended_play") or "unknown").replace("_", " ")
    outreach_type = str(context.get("outreach_type") or "initial_outreach").replace("_", " ")
    outcome = str(outcome_status or "pending").replace("_", " ")
    quality = context.get("contact_quality_label") or "blocked"
    rewrite_style = str(context.get("rewrite_style") or "standard").replace("_", " ")
    why_now = context.get("contact_reason") or context.get("why_now") or ""
    lesson = f"{merchant}: {play} via {outreach_type} ended {outcome} using a {quality} contact and a {rewrite_style} draft."
    if why_now:
        lesson = f"{lesson} Reason in play: {why_now}"
    if notes:
        lesson = f"{lesson} Operator note: {notes.strip()}"
    return lesson


def _reply_learning_context(context: dict) -> dict:
    thread_id = str(context.get("gmail_thread_id") or "").strip()
    if not thread_id:
        return {}
    intelligence = get_gmail_thread_intelligence(thread_id) or {}
    metadata = intelligence.get("metadata") or {}
    return {
        "thread_category": str(intelligence.get("thread_category") or "").strip().lower(),
        "snippet": str(intelligence.get("snippet") or "").strip(),
        "reply_intent": str(metadata.get("reply_intent") or "").strip().lower(),
        "buying_intent": str(metadata.get("buying_intent") or "").strip().lower(),
        "reply_intent_reason": str(metadata.get("reply_intent_reason") or "").strip(),
    }


def _build_outcome_feedback_note(*, reply_context: dict, notes: str = "") -> str:
    parts: list[str] = []
    reply_intent = str(reply_context.get("reply_intent") or "").strip()
    buying_intent = str(reply_context.get("buying_intent") or "").strip()
    reason = str(reply_context.get("reply_intent_reason") or "").strip()
    snippet = str(reply_context.get("snippet") or "").strip()
    if reply_intent:
        parts.append(f"Reply intent: {reply_intent}")
    if buying_intent:
        parts.append(f"buyer signal: {buying_intent}")
    if reason:
        parts.append(reason)
    if snippet:
        parts.append(f"Reply snippet: {snippet[:220]}")
    if notes:
        parts.append(f"Operator note: {notes.strip()}")
    return " | ".join(part for part in parts if part).strip()


def _subject_for_distress(context: dict) -> str:
    merchant = context.get("merchant_name_display") or context.get("merchant_name") or context.get("merchant_domain") or "your payments issue"
    distress = normalize_distress_topic(context.get("distress_type"))
    processor = _display_processor_name(context.get("processor")) or "Payments"
    mapping = {
        "account_frozen": f"{processor} account freeze at {merchant}",
        "payouts_delayed": f"Delayed payouts at {merchant}",
        "reserve_hold": f"{processor} reserve pressure at {merchant}",
        "verification_review": f"{processor} review pressure at {merchant}",
        "chargeback_issue": f"Chargeback pressure at {merchant}",
        "processor_switch_intent": f"Processor options for {merchant}",
        "account_terminated": f"{processor} account termination at {merchant}",
        "onboarding_rejected": f"Alternative processor path for {merchant}",
    }
    return mapping.get(distress, f"Next steps for {merchant}")


def _merchant_next_step_offer(recommendation: dict, distress_type: str | None) -> str:
    play = normalize_operator_action(recommendation.get("best_play"))
    distress = normalize_distress_topic(distress_type)
    if play == "chargeback_mitigation" or distress == "chargeback_issue":
        return "the shortest next-step outline for getting clear on the chargeback pressure and what to do first"
    if play == "payout_acceleration" or distress == "payouts_delayed":
        return "the shortest next-step outline for stabilizing payout timing"
    if play == "reserve_negotiation" or distress == "reserve_hold":
        return "the shortest next-step outline for reducing reserve or review pressure"
    if play == "urgent_processor_migration":
        return "the shortest next-step outline for restoring processing quickly"
    if play == "compliance_remediation":
        return "the shortest next-step outline for getting through the review cleanly"
    if play == "onboarding_assistance":
        return "the shortest next-step outline for evaluating the right processor path"
    return "the shortest next-step outline for your case"


def _default_outreach_type(context: dict) -> str:
    status = context.get("outreach_status") or "no_outreach"
    if status == "replied":
        return "reply_follow_up"
    if status in {"sent", "follow_up_needed"}:
        return "follow_up_nudge"
    return "initial_outreach"


def _upsert_outreach_row(conn, **fields) -> dict:
    conn.execute(
        text(
            """
            INSERT INTO opportunity_outreach_actions (
                opportunity_id, merchant_id, merchant_domain, contact_email, contact_name,
                channel, selected_play, outreach_type, subject, body, status, approval_state,
                gmail_thread_id, draft_message_id, sent_message_id, sent_at, replied_at,
                follow_up_due_at, outcome_status, notes, metadata_json, updated_at
            )
            VALUES (
                :opportunity_id, :merchant_id, :merchant_domain, :contact_email, :contact_name,
                :channel, :selected_play, :outreach_type, :subject, :body, :status, :approval_state,
                :gmail_thread_id, :draft_message_id, :sent_message_id, :sent_at, :replied_at,
                :follow_up_due_at, :outcome_status, :notes, CAST(:metadata_json AS JSONB), NOW()
            )
            ON CONFLICT (opportunity_id) DO UPDATE SET
                merchant_id = EXCLUDED.merchant_id,
                merchant_domain = EXCLUDED.merchant_domain,
                contact_email = EXCLUDED.contact_email,
                contact_name = EXCLUDED.contact_name,
                channel = EXCLUDED.channel,
                selected_play = EXCLUDED.selected_play,
                outreach_type = EXCLUDED.outreach_type,
                subject = EXCLUDED.subject,
                body = EXCLUDED.body,
                status = EXCLUDED.status,
                approval_state = EXCLUDED.approval_state,
                gmail_thread_id = EXCLUDED.gmail_thread_id,
                draft_message_id = EXCLUDED.draft_message_id,
                sent_message_id = EXCLUDED.sent_message_id,
                sent_at = EXCLUDED.sent_at,
                replied_at = EXCLUDED.replied_at,
                follow_up_due_at = EXCLUDED.follow_up_due_at,
                outcome_status = EXCLUDED.outcome_status,
                notes = EXCLUDED.notes,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = NOW()
            """
        ),
        {**fields, "metadata_json": json.dumps(fields.get("metadata") or {})},
    )
    row = conn.execute(
        text("SELECT * FROM opportunity_outreach_actions WHERE opportunity_id = :opportunity_id"),
        {"opportunity_id": int(fields["opportunity_id"])},
    ).mappings().first()
    return _serialize_outreach_row(dict(row)) if row else {}


def _create_gmail_draft(*, to_email: str, subject: str, body: str, thread_id: str = "") -> dict:
    adapter = GmailAdapter()
    if _recipient_is_denied(to_email, sender_email=adapter.sender_email):
        raise ValueError("Recipient is blocked by outreach denylist")
    message = MIMEText(body)
    message["to"] = to_email
    message["subject"] = subject
    sender = adapter.sender_email or adapter.sender_name or "PayFlux"
    message["from"] = f"{adapter.sender_name} <{adapter.sender_email}>" if adapter.sender_email else sender
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    payload = {"message": {"raw": raw}}
    if thread_id:
        payload["message"]["threadId"] = thread_id
    result = adapter._request("POST", "/drafts", json=payload)
    message_payload = result.get("message", {}) or {}
    return {"draft_id": result.get("id", ""), "message_id": message_payload.get("id", ""), "thread_id": message_payload.get("threadId", thread_id)}


def _send_gmail_message(*, to_email: str, subject: str, body: str, thread_id: str = "") -> dict:
    adapter = GmailAdapter()
    if _recipient_is_denied(to_email, sender_email=adapter.sender_email):
        raise ValueError("Recipient is blocked by outreach denylist")
    message = MIMEText(body)
    message["to"] = to_email
    message["subject"] = subject
    message["from"] = f"{adapter.sender_name} <{adapter.sender_email}>" if adapter.sender_email else adapter.sender_name
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    payload = {"raw": raw}
    if thread_id:
        payload["threadId"] = thread_id
    result = adapter._request("POST", "/messages/send", json=payload)
    return {"message_id": result.get("id", ""), "thread_id": result.get("threadId", thread_id)}


def _follow_up_allowed(row: dict) -> bool:
    due_at = _ensure_utc_datetime(row.get("follow_up_due_at"))
    if not due_at:
        return False
    return due_at <= datetime.now(timezone.utc)


def _outreach_readiness_sort_key(case: dict) -> tuple:
    urgency_rank = {"high": 3, "medium": 2, "low": 1}.get(case.get("urgency") or "low", 0)
    readiness_rank = {
        "send_ready": 3,
        "approval_ready": 2,
        "blocked_contact_quality": 1,
        "follow_up_needed": 1,
        "sent": 0,
        "replied": 0,
    }.get(case.get("readiness_state") or "blocked_contact_quality", 0)
    quality_rank = {"trusted": 3, "high_confidence": 2, "blocked": 1}.get(case.get("contact_quality_label") or "blocked", 0)
    return (urgency_rank, readiness_rank, quality_rank, int(case.get("opportunity_id") or 0))


def _signal_context_line(context: dict) -> str:
    processor = _display_processor_name(context.get("processor"))
    distress = normalize_distress_topic(context.get("distress_type"))
    if processor and distress != "unknown":
        return f"The live signal points to {processor} {distress.replace('_', ' ')}."
    if distress != "unknown":
        return f"The live signal points to {distress.replace('_', ' ')}."
    return ""


def _contact_context_line(context: dict) -> str:
    if context.get("contact_send_eligible"):
        page_phrase = _page_type_label(context.get("contact_page_type") or "")
        return f"We have a trusted Gmail path through {context.get('contact_email')} from the merchant {page_phrase}."
    if context.get("contact_deepening_summary"):
        return context.get("contact_deepening_summary")
    return context.get("contact_reason") or "We should wait until a trusted merchant email is on file."


def _contact_summary_line(context: dict) -> str:
    state = context.get("contact_state") or "no_contact_found"
    email = context.get("contact_email") or ""
    score = int(context.get("contact_trust_score") or 0)
    page_phrase = _page_type_label(context.get("contact_page_type") or "")
    role_hint = context.get("contact_role_hint") or ""
    if state == "verified_contact_found" and email:
        role_phrase = f" It aligns to {role_hint}." if role_hint else ""
        return f"The best contact on file is {email} from the merchant {page_phrase} with a trust score of {score}.{role_phrase}"
    if state == "weak_contact_found" and email:
        return f"The best contact on file is {email} from the merchant {page_phrase}, but it is still too weak for live outreach."
    if context.get("contact_deepening_summary"):
        return context.get("contact_deepening_summary")
    return "No usable merchant-owned contact is on file yet."


def _valid_contact_email(email: str | None) -> bool:
    if not email or "@" not in str(email):
        return False
    lowered = str(email).strip().lower()
    return not any(pattern in lowered for pattern in ROLE_ADDRESS_PATTERNS)


def _serialize_outreach_row(row: dict) -> dict:
    row = dict(row)
    for key in ("sent_at", "replied_at", "follow_up_due_at", "created_at", "updated_at"):
        row[key] = _serialize_dt(row.get(key))
    row["metadata_json"] = _json_dict(row.get("metadata_json"))
    return row


def _json_dict(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _serialize_dt(value):
    dt = _ensure_utc_datetime(value)
    return dt.isoformat() if dt else None


def _ensure_utc_datetime(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _humanize(value: str | None) -> str:
    if not value:
        return ""
    return str(value).replace("_", " ")


def _distress_phrase(distress: str) -> str:
    phrases = {
        "account_frozen": "account freeze",
        "payouts_delayed": "payout delays",
        "reserve_hold": "reserve pressure",
        "verification_review": "review hold",
        "processor_switch_intent": "processor switching pressure",
        "chargeback_issue": "chargeback pressure",
        "account_terminated": "account termination",
        "onboarding_rejected": "onboarding rejection",
        "unknown": "payment pressure",
    }
    return phrases.get(distress, phrases["unknown"])


def _infer_processor(content: str) -> str | None:
    lowered = (content or "").lower()
    for processor in ("stripe", "paypal", "square", "adyen", "shopify payments", "braintree", "authorize.net", "worldpay", "checkout.com"):
        if processor in lowered:
            return processor.replace(" ", "_").replace(".", "_")
    return None


def _infer_distress_type(content: str) -> str | None:
    lowered = (content or "").lower()
    patterns = {
        "account_frozen": ("account frozen", "accounts frozen", "froze our account", "frozen account", "payments disabled", "funds frozen"),
        "payouts_delayed": ("payout delayed", "payouts delayed", "payout paused"),
        "reserve_hold": ("funds held", "funds on hold", "rolling reserve", "reserve increase"),
        "verification_review": ("under review", "kyc", "kyb", "compliance review", "website verification"),
        "chargeback_issue": ("chargeback", "dispute spike", "negative balance"),
        "processor_switch_intent": ("new processor", "processor alternative", "alternative to stripe", "need a processor"),
        "account_terminated": (
            "account terminated",
            "account closed",
            "gateway stopped accepting",
            "payment gateway recently stopped accepting",
            "stopped accepting tobacco related product sales",
            "processor cut us off",
            "gateway cut us off",
        ),
        "onboarding_rejected": ("onboarding rejected", "application denied", "application rejected"),
    }
    for distress, hints in patterns.items():
        if any(hint in lowered for hint in hints):
            return distress
    return None


def _split_email(email: str | None) -> tuple[str, str]:
    lowered = str(email or "").strip().lower()
    if "@" not in lowered:
        return "", ""
    local_part, domain = lowered.split("@", 1)
    return local_part, domain


def _is_named_contact_local(local_part: str) -> bool:
    cleaned = str(local_part or "").strip().lower()
    if not cleaned:
        return False
    if cleaned in TRUSTED_ROLE_LOCAL_PARTS or cleaned in DECISION_ROLE_LOCAL_PARTS or cleaned in GENERIC_BLOCKED_LOCAL_PARTS:
        return False
    # Dot/hyphen separators: "andy.smith", "andy-smith"
    if "." in cleaned or "-" in cleaned:
        return True
    # Bare first-name local parts: "andy", "kyle", "dan"
    # Short alphabetic strings that aren't role accounts are personal inboxes.
    if cleaned.isalpha() and 2 <= len(cleaned) <= 20:
        return True
    return False


def _display_processor_name(value: str | None) -> str:
    if not value or str(value).strip().lower() == "unknown":
        return ""
    text_value = str(value).strip().lower().replace("_", " ")
    mapping = {
        "stripe": "Stripe",
        "paypal": "PayPal",
        "square": "Square",
        "adyen": "Adyen",
        "shopify payments": "Shopify Payments",
        "braintree": "Braintree",
        "authorize net": "Authorize.net",
        "worldpay": "Worldpay",
        "checkout com": "Checkout.com",
    }
    return mapping.get(text_value, text_value.title())


def _display_merchant_name(name: str | None, domain: str | None) -> str:
    clean_name = str(name or "").strip()
    if clean_name:
        lowered = clean_name.lower()
        if domain and lowered == str(domain).strip().lower():
            clean_name = ""
        elif " " not in clean_name and clean_name.isalpha() and len(clean_name) >= 8:
            clean_name = _titlecase_domain_root(clean_name)
    if clean_name:
        return clean_name
    return _titlecase_domain_root(domain or "") or (domain or "Unknown merchant")


def _titlecase_domain_root(domain: str) -> str:
    host = str(domain or "").strip().lower()
    if not host:
        return ""
    root = host.split(".")[0]
    if not root:
        return ""
    pieces = re.findall(r"[a-z]+|\d+", root)
    if not pieces:
        return root.title()
    return " ".join(piece.title() for piece in pieces)


def _display_contact_name(name: str | None, merchant_label: str) -> str:
    cleaned = str(name or "").strip()
    if cleaned:
        return cleaned
    return merchant_label


def _confidence_opening(context: dict, *, processor: str, distress_label: str, merchant_label: str) -> str:
    signal_source = str(context.get("signal_source") or "").strip().lower()
    processor_raw = str(context.get("processor") or "").strip().lower()
    if _high_confidence_signal(context):
        distress = normalize_distress_topic(context.get("distress_type"))
        if processor_raw in {"", "unknown"}:
            public_phrase = _public_signal_phrase(context, merchant_label=merchant_label, distress=distress, distress_label=distress_label)
            if distress == "chargeback_issue":
                return f"I’m reaching out because {public_phrase}."
            if distress == "payouts_delayed":
                return f"I’m reaching out because {public_phrase}."
            if distress == "reserve_hold":
                return f"I’m reaching out because {public_phrase}."
            if distress == "verification_review":
                return f"I’m reaching out because {public_phrase}."
            return f"I’m reaching out because {public_phrase}."
        if distress == "account_frozen":
            return f"I’m reaching out because {processor} is already restricting payouts or processing for {merchant_label}."
        if distress == "payouts_delayed":
            return f"I’m reaching out because {merchant_label} is already dealing with delayed payouts through {processor}."
        if distress == "reserve_hold":
            return f"I’m reaching out because {merchant_label} is already under reserve pressure with {processor}."
        if distress == "verification_review":
            return f"I’m reaching out because {merchant_label} is already under review pressure with {processor}."
        if distress == "chargeback_issue":
            return f"I’m reaching out because chargeback pressure is already hitting {merchant_label} through {processor}."
        return f"I’m reaching out because {merchant_label} is already dealing with {processor} {distress_label}."
    if "http" in signal_source or signal_source.startswith("http"):
        return f"I’m reaching out because {_public_signal_phrase(context, merchant_label=merchant_label, distress=normalize_distress_topic(context.get('distress_type')), distress_label=distress_label)}."
    return f"I’m reaching out because {merchant_label} appears to be dealing with {processor} {distress_label}."


def _public_signal_phrase(context: dict, *, merchant_label: str, distress: str, distress_label: str) -> str:
    content = str(context.get("signal_content") or "").strip().lower()
    source = str(context.get("signal_source") or "").strip().lower()
    if distress == "chargeback_issue":
        if "high risk" in content or "high-risk" in content:
            return f"a public discussion tied to {merchant_label} mentioned high-risk processing and chargeback pressure"
        if "chargeback" in content or "dispute" in content or "negative balance" in content:
            return f"a public discussion tied to {merchant_label} mentioned chargeback pressure"
        return f"a public signal suggests {merchant_label} may be dealing with chargeback pressure"
    if distress == "payouts_delayed":
        if "payout" in content or "settlement" in content:
            return f"a public discussion tied to {merchant_label} mentioned payout delays"
        return f"a public signal suggests {merchant_label} may be dealing with payout delays"
    if distress == "reserve_hold":
        if "reserve" in content or "hold" in content:
            return f"a public discussion tied to {merchant_label} mentioned reserve or hold pressure"
        return f"a public signal suggests {merchant_label} may be under reserve or hold pressure"
    if distress == "verification_review":
        if "review" in content or "verification" in content or "kyc" in content:
            return f"a public discussion tied to {merchant_label} mentioned account review pressure"
        return f"a public signal suggests {merchant_label} may be under account review pressure"
    if "http" in source:
        return f"a public discussion tied to {merchant_label} mentioned {distress_label}"
    return f"a public signal suggests {merchant_label} may be dealing with {distress_label}"


def _high_confidence_signal(context: dict) -> bool:
    distress = normalize_distress_topic(context.get("distress_type"))
    processor = str(context.get("processor") or "").strip().lower()
    return distress != "unknown" and (
        bool(context.get("signal_detected_at"))
        or bool(context.get("signal_content"))
        or processor not in {"", "unknown"}
    )
