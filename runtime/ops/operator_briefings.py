from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
import re
import threading
import time
from zoneinfo import ZoneInfo

import redis
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from memory.structured.db import engine
from runtime.health.telemetry import get_component_state, is_stale, record_component_state, utc_now_iso
from runtime.intelligence.commercial_qualification import assess_commercial_readiness
from runtime.ops.status_cache import (
    OPPORTUNITY_COUNTS_CACHE_KEY,
    read_status_snapshot,
    stale_status_snapshot,
    unavailable_status_snapshot,
    write_status_snapshot,
)
from runtime.ops.operator_alerts import send_operator_alert
from runtime.intelligence.distress_normalization import (
    doctrine_priority_reason,
    normalize_distress_topic,
    operator_action_for_distress,
    operator_action_label,
    strategy_for_distress,
)
from runtime.intelligence.opportunity_queue_quality import (
    ELIGIBILITY_OUTREACH,
    ELIGIBILITY_OPERATOR_REVIEW_ONLY,
    evaluate_opportunity_queue_quality,
    normalize_merchant_domain,
)
from runtime.ops.operator_commands import (
    advance_contact_acquisition_command,
    approve_outreach_for_opportunity_command,
    draft_outreach_for_opportunity_command,
    get_contact_intelligence_command,
    get_outreach_recommendation_command,
    get_system_status,
    list_actionable_patterns_command,
    list_distress_patterns_command,
    list_deal_opportunities_command,
    list_gmail_merchant_distress_command,
    list_gmail_triage_command,
    list_merchant_profiles_command,
    list_merchants_by_operator_action_command,
    list_opportunities_by_operator_action_command,
    list_blocked_outreach_leads_command,
    list_outreach_awaiting_approval_command,
    list_send_eligible_outreach_leads_command,
    list_sent_outreach_needing_follow_up_command,
    mark_opportunity_operator_action_command,
    mark_opportunity_outcome_command,
    send_outreach_for_opportunity_command,
    show_outreach_metrics_command,
    show_opportunity_fit_command,
    show_recent_suppressed_leads_command,
    show_selected_outreach_command,
    show_opportunity_operator_action_command,
    show_best_conversion_patterns_command,
    show_conversion_intelligence_command,
    show_reasoning_metrics_command,
    show_contact_blocked_queue_command,
    show_sendable_queue_command,
    show_top_queue_opportunities_command,
)
from runtime.ops.value_heartbeat import get_value_heartbeat
from runtime.reasoning.control_plane import reasoning_settings


_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
_OPERATOR_TIMEZONE = os.getenv("AGENT_FLUX_OPERATOR_TIMEZONE", "America/New_York").strip() or "America/New_York"
_DAILY_BRIEFING_START_HOUR = int(os.getenv("AGENT_FLUX_DAILY_BRIEFING_START_HOUR", "6"))
_DAILY_BRIEFING_END_HOUR = int(os.getenv("AGENT_FLUX_DAILY_BRIEFING_END_HOUR", "11"))
OPPORTUNITY_COUNTS_QUERY_TIMEOUT_MS = 1200
_SHOW_OPERATOR_TELEMETRY = str(os.getenv("AGENT_FLUX_SHOW_OPERATOR_TELEMETRY") or "").strip().lower() in {"1", "true", "yes", "on"}

# ── Alert policy: distress-type-aware thresholds ─────────────────────────────
# Keys are canonical distress_type values stored in distress_pattern_clusters.
_ALERT_POLICY = {
    "account_terminated":      {"min_signal_delta": 1, "min_merchant_delta": 1, "cooldown_seconds": 900},
    "account_frozen":          {"min_signal_delta": 1, "min_merchant_delta": 1, "cooldown_seconds": 900},
    "chargeback_issue":        {"min_signal_delta": 3, "min_merchant_delta": 2, "cooldown_seconds": 3600},
    "verification_review":     {"min_signal_delta": 3, "min_merchant_delta": 2, "cooldown_seconds": 3600},
    "processor_switch_intent": {"min_signal_delta": 5, "min_merchant_delta": 3, "cooldown_seconds": 7200},
    "_default":                {"min_signal_delta": 3, "min_merchant_delta": 2, "cooldown_seconds": 3600},
}

_SOURCE_COOLDOWNS = {
    "distress_pattern": 0,
    "gmail_distress": 1800,
    "merchant_profile": 3600,
    "opportunity": 1800,
    "conversion_intelligence": 7200,
    "health": 0,
    "reasoning_regression": 3600,
}

_MAX_ALERTS_PER_CYCLE = 2
_BYPASS_ANTI_FLAP_SECONDS = 300

_STATUS_SEVERITY = {"urgent": 3, "rising": 2, "watch": 1}


def _get_alert_policy(distress_type: str) -> dict:
    return _ALERT_POLICY.get(distress_type or "", _ALERT_POLICY["_default"])


def _count_phrase(count: int, singular: str, plural: str | None = None) -> str:
    count = int(count or 0)
    if count == 1:
        return f"1 {singular}"
    return f"{count} {plural or singular + 's'}"


def _normalized_processor_label(value: str | None) -> str:
    normalized = str(value or "").strip().lower().replace("_", " ")
    if normalized in {"", "unknown", "unclassified"}:
        return "an unresolved processor"
    return _humanize_label(value)


def _normalized_distress_label(value: str | None) -> str:
    normalized = str(value or "").strip().lower().replace("_", " ")
    if normalized in {"", "unknown"}:
        return "merchant distress"
    return normalized


def _urgency_summary(value: object) -> str:
    raw_text = str(value or "").strip()
    try:
        urgency = int(float(raw_text or 0))
    except (TypeError, ValueError):
        return ""
    if urgency <= 0:
        return ""
    if urgency >= 100:
        return "timing looks urgent"
    if urgency >= 70:
        return "timing looks strong"
    if urgency >= 40:
        return "timing looks active"
    return "timing looks watchable"


def _daily_queue_summary(*, pending_review: int, drafts_waiting: int, replies_open: int, follow_ups_due: int) -> str:
    parts = [
        _count_phrase(pending_review, "case waiting for review", "cases waiting for review"),
        _count_phrase(drafts_waiting, "draft waiting for approval", "drafts waiting for approval"),
    ]
    if replies_open > 0:
        parts.append(_count_phrase(replies_open, "live reply waiting", "live replies waiting"))
    if follow_ups_due > 0:
        parts.append(_count_phrase(follow_ups_due, "follow-up due", "follow-ups due"))
    return ", ".join(parts[:-1]) + (f", and {parts[-1]}" if len(parts) > 1 else parts[0])


def _pattern_change_is_material(pattern: dict) -> bool:
    pattern_key = pattern.get("pattern_key") or ""
    redis_key = f"agent_flux:alert_policy:pattern_snapshot:{pattern_key}"
    prev_raw = _redis.get(redis_key)
    if not prev_raw:
        return True
    try:
        prev = json.loads(prev_raw)
    except Exception:
        return True

    cur_status = pattern.get("status") or "watch"
    prev_status = prev.get("status") or "watch"
    if _STATUS_SEVERITY.get(cur_status, 0) > _STATUS_SEVERITY.get(prev_status, 0):
        return True

    cur_contacts = int(pattern.get("verified_contact_count", 0) or 0)
    prev_contacts = int(prev.get("verified_contact_count", 0) or 0)
    if cur_contacts - prev_contacts >= 1:
        return True

    policy = _get_alert_policy(pattern.get("distress_type"))
    sent_at = float(prev.get("sent_at") or 0)
    if sent_at and time.time() - sent_at < policy["cooldown_seconds"]:
        return False

    signal_delta = abs(int(pattern.get("signal_count", 0) or 0) - int(prev.get("signal_count", 0) or 0))
    merchant_delta = abs(int(pattern.get("affected_merchant_count", 0) or 0) - int(prev.get("affected_merchant_count", 0) or 0))
    return signal_delta >= policy["min_signal_delta"] or merchant_delta >= policy["min_merchant_delta"]


def _save_pattern_snapshot(pattern: dict) -> None:
    pattern_key = pattern.get("pattern_key") or ""
    redis_key = f"agent_flux:alert_policy:pattern_snapshot:{pattern_key}"
    snapshot = {
        "signal_count": int(pattern.get("signal_count", 0) or 0),
        "affected_merchant_count": int(pattern.get("affected_merchant_count", 0) or 0),
        "status": pattern.get("status") or "watch",
        "verified_contact_count": int(pattern.get("verified_contact_count", 0) or 0),
        "sent_at": time.time(),
    }
    _redis.setex(redis_key, 7 * 86400, json.dumps(snapshot))


def _source_cooldown_ok(source_name: str) -> bool:
    cooldown = _SOURCE_COOLDOWNS.get(source_name, 0)
    if cooldown <= 0:
        return True
    redis_key = f"agent_flux:alert_policy:last_sent_ts:{source_name}"
    last_raw = _redis.get(redis_key)
    if not last_raw:
        return True
    try:
        return time.time() - float(last_raw) >= cooldown
    except (ValueError, TypeError):
        return True


def _record_source_send(source_name: str) -> None:
    redis_key = f"agent_flux:alert_policy:last_sent_ts:{source_name}"
    _redis.setex(redis_key, 86400, str(time.time()))


def _bypass_signature_ok(source_name: str, signature: str) -> bool:
    redis_key = f"agent_flux:alert_policy:bypass_sig:{source_name}"
    prev = _redis.get(redis_key)
    if prev == signature:
        return False
    return True


def _record_bypass_signature(source_name: str, signature: str) -> None:
    redis_key = f"agent_flux:alert_policy:bypass_sig:{source_name}"
    _redis.setex(redis_key, _BYPASS_ANTI_FLAP_SECONDS, signature)


def _count_verified_contacts_for_pattern(pattern: dict) -> int:
    cache_key = f"agent_flux:alert_policy:vc_count:{pattern.get('pattern_key', '')}"
    cached = _redis.get(cache_key)
    if cached is not None:
        try:
            return int(cached)
        except (ValueError, TypeError):
            pass
    try:
        processor = pattern.get("processor") or "unknown"
        distress_type = pattern.get("distress_type") or "unknown"
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT ooa.opportunity_id) AS cnt
                    FROM opportunity_outreach_actions ooa
                    JOIN merchant_opportunities mo ON mo.id = ooa.opportunity_id
                    WHERE ooa.contact_email IS NOT NULL
                      AND ooa.contact_email != ''
                      AND mo.processor = :processor
                      AND mo.distress_topic = :distress_type
                    """
                ),
                {"processor": processor, "distress_type": distress_type},
            ).mappings().first()
        count = int((row or {}).get("cnt", 0) or 0)
    except Exception:
        count = 0
    _redis.setex(cache_key, 300, str(count))
    return count


def _compute_rank_score(data: dict, source: str) -> float:
    score = 0.0
    status = data.get("status") or "watch"
    score += {"urgent": 100, "rising": 60, "watch": 20}.get(status, 10)

    merchant_count = int(data.get("affected_merchant_count", 0) or 0)
    score += min(merchant_count * 5, 50)

    verified_contacts = int(data.get("verified_contact_count", 0) or 0)
    score += verified_contacts * 20

    wins = int(data.get("wins", 0) or 0)
    score += wins * 15

    urgency = float(data.get("urgency_score", 0) or 0)
    score += urgency * 0.3

    score += {
        "opportunity": 20,
        "gmail_distress": 15,
        "distress_pattern": 10,
        "merchant_profile": 5,
        "conversion_intelligence": 0,
    }.get(source, 0)

    distress = data.get("distress_type") or "unknown"
    score += {
        "account_terminated": 25,
        "account_frozen": 20,
        "chargeback_issue": 10,
        "verification_review": 5,
        "processor_switch_intent": 0,
    }.get(distress, 5)

    return score


def _timed_status_step(name: str, fn):
    started = time.time()
    result = fn()
    return result, round((time.time() - started) * 1000, 2), name


def _record_status_step_timings(
    steps: list[tuple[str, float]],
    total_ms: float,
    *,
    fallback_used: bool = False,
    fallback_reason: str = "",
):
    if not steps:
        return
    slowest_name, slowest_ms = max(steps, key=lambda item: item[1])
    record_component_state(
        "operator_status",
        ttl=600,
        last_status_total_ms=round(total_ms, 2),
        last_status_slowest_step=slowest_name,
        last_status_slowest_step_ms=round(slowest_ms, 2),
        last_status_step_timings={name: duration for name, duration in steps},
        last_status_profiled_at=utc_now_iso(),
        fallback_used=bool(fallback_used),
        fallback_reason=fallback_reason or "",
        last_status_fallback_source="" if not fallback_used else None,
        last_status_fallback_at="" if not fallback_used else None,
        last_status_fallback_brief="" if not fallback_used else None,
    )


def get_system_status_payload() -> dict:
    timing_started = time.time()
    step_timings: list[tuple[str, float]] = []

    system, duration, name = _timed_status_step("get_system_status", get_system_status)
    step_timings.append((name, duration))
    gmail, duration, name = _timed_status_step("list_gmail_triage", lambda: list_gmail_triage_command(limit=10))
    step_timings.append((name, duration))
    reasoning, duration, name = _timed_status_step("show_reasoning_metrics", show_reasoning_metrics_command)
    step_timings.append((name, duration))
    outreach, duration, name = _timed_status_step("show_outreach_metrics", show_outreach_metrics_command)
    step_timings.append((name, duration))
    opportunities, duration, name = _timed_status_step("get_opportunity_counts", _get_opportunity_counts)
    step_timings.append((name, duration))
    distress_result, duration, name = _timed_status_step(
        "list_gmail_merchant_distress",
        lambda: list_gmail_merchant_distress_command(limit=3),
    )
    step_timings.append((name, duration))
    distress_threads = distress_result.get("threads", [])
    health = _overall_health_label()
    meta = system.get("_meta") or {}
    source_health_summary = system.get("source_health_summary", "")
    telegram_delivery = system.get("telegram_delivery") or {}
    shopify_funnel_summary = system.get("shopify_community_funnel_summary", "")
    shopify_board_summary = system.get("shopify_community_board_summary", "")
    if meta.get("stale"):
        summary = "Live system status is slow, so this is the last safe snapshot."
        matters = (
            f"Gmail is still {gmail.get('gmail_status', 'healthy')}, and {opportunities['pending_review']} opportunities are waiting for review."
        )
        recommendation = "Use this snapshot for triage and only retry for fresh numbers if you need a live update."
    elif meta.get("unavailable"):
        summary = "Live system status is slow right now."
        matters = "I do not have a safe cached status snapshot yet, but the operator path is still up."
        recommendation = "Stay on the current queue and check again shortly for a fresh read."
    else:
        summary = (
            "The system looks steady."
            if health == "normal"
            else "The system needs attention, but the operator path and Gmail intake are still up."
        )
        outreach_line = _system_outreach_line(outreach)
        matters = (
            f"Gmail is quiet right now, but {opportunities['pending_review']} opportunities are waiting for review. {outreach_line}"
            if gmail.get("merchant_distress_count", 0) == 0
            else f"Gmail has {gmail.get('merchant_distress_count', 0)} active merchant-distress thread(s), {opportunities['pending_review']} opportunities are waiting for review, and {outreach_line}"
        )
        telegram_line = _telegram_delivery_line(telegram_delivery)
        if telegram_line:
            matters = f"{matters} {telegram_line}"
        if source_health_summary:
            matters = f"{matters} {source_health_summary}."
        if shopify_funnel_summary:
            matters = f"{matters} {shopify_funnel_summary}"
        if shopify_board_summary:
            matters = f"{matters} Boards: {shopify_board_summary}."
        if str(telegram_delivery.get("status") or "").strip().lower() in {"offline", "degraded", "attention"}:
            recommendation = "Restore Telegram delivery first, then return to the top approval or review case."
        else:
            recommendation = (
                "Start with the next outreach approval case, then return to the newest opportunity in the queue."
                if int(outreach.get("outreach_awaiting_approval", 0) or 0) > 0
                else "Start with the newest opportunity in the queue."
                if opportunities["pending_review"] > 0
                else "Let the system run and check again after the next triage cycle."
            )
    payload = _finalize_payload(
        "system_status",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "health": health,
                "signals_24h": system.get("signals_24h", 0),
                "signals_1h": system.get("signals_1h", 0),
                "gmail_merchant_distress": gmail.get("merchant_distress_count", 0),
                "gmail_reply_worthy": gmail.get("reply_worthy_count", 0),
                "opportunities_pending_review": opportunities["pending_review"],
                "reasoning_status": reasoning.get("reasoning_status", "unknown"),
                "outreach_awaiting_approval": outreach.get("outreach_awaiting_approval", 0),
                "follow_ups_due": outreach.get("follow_ups_due", 0),
                "telegram_delivery_status": telegram_delivery.get("status", "unknown"),
                "telegram_queue_depth": telegram_delivery.get("queue_depth", 0),
                "source_health_summary": source_health_summary,
                "shopify_community_funnel_summary": shopify_funnel_summary,
            },
            "snapshot": {
                "health": health,
                "signals_24h": system.get("signals_24h", 0),
                "signals_1h": system.get("signals_1h", 0),
                "gmail_merchant_distress": gmail.get("merchant_distress_count", 0),
                "opportunities_pending_review": opportunities["pending_review"],
                "reasoning_status": reasoning.get("reasoning_status", "unknown"),
                "outreach_awaiting_approval": outreach.get("outreach_awaiting_approval", 0),
                "follow_ups_due": outreach.get("follow_ups_due", 0),
                "telegram_delivery_status": telegram_delivery.get("status", "unknown"),
                "telegram_queue_depth": telegram_delivery.get("queue_depth", 0),
                "source_health_summary": source_health_summary,
                "shopify_community_funnel_summary": shopify_funnel_summary,
            },
            "focus": {
                "type": "system_status",
                "label": "system status",
            },
        },
    )
    total_ms = (time.time() - timing_started) * 1000
    _record_status_step_timings(
        step_timings,
        total_ms,
        fallback_used=bool(meta.get("stale") or meta.get("unavailable")),
        fallback_reason=meta.get("fallback_reason") or "",
    )
    return payload


def get_signal_activity_payload() -> dict:
    system = get_system_status()
    gmail = list_gmail_merchant_distress_command(limit=5)
    threads = gmail.get("threads", [])
    top_issue = _top_gmail_issue(threads) or "none"
    latest = threads[0] if threads else {}
    count = gmail.get("count", 0)
    summary = (
        f"Signal flow is quiet right now, with {system.get('signals_1h', 0)} new signal(s) in the last hour."
        if count == 0
        else f"Signal flow is active and {count} Gmail merchant-distress conversation(s) are in play."
    )
    matters = (
        "There is no active Gmail merchant-distress conversation waiting."
        if count == 0
        else f"The strongest Gmail issue right now is {top_issue}, led by '{latest.get('subject') or latest.get('thread_id')}'."
    )
    recommendation = (
        "No manual action is needed right now."
        if count == 0
        else "Review the top Gmail distress thread before the next approval decision."
    )
    return _finalize_payload(
        "signal_activity",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "signals_24h": system.get("signals_24h", 0),
                "signals_1h": system.get("signals_1h", 0),
                "gmail_merchant_distress": count,
                "top_issue": top_issue,
            },
            "snapshot": {
                "signals_24h": system.get("signals_24h", 0),
                "signals_1h": system.get("signals_1h", 0),
                "gmail_merchant_distress": count,
                "top_issue": top_issue,
            },
            "focus": {
                "type": "gmail_thread",
                "thread_id": latest.get("thread_id"),
                "subject": latest.get("subject"),
                "label": latest.get("subject") or latest.get("thread_id"),
            },
        },
    )


def get_pattern_summary_payload() -> dict:
    patterns = list_distress_patterns_command(limit=5).get("patterns", [])
    top = patterns[0] if patterns else {}
    trend = top.get("trend_label") or "stable"
    summary = (
        top.get("headline")
        if top
        else "No recurring distress pattern is standing out enough to flag right now."
    )
    matters = (
        " ".join(
            part
            for part in [
                f"This pattern is {trend}.",
                _pattern_interpretation_line(top),
                _pattern_actionability_text(top),
                _pattern_strategy_line(top),
                _pattern_conversion_line(top),
            ]
            if part
        ).strip()
        if top
        else "Signal flow is still active, but nothing has grouped into a higher-confidence recurring pattern yet."
    )
    recommendation = (
        _pattern_recommendation(top)
        if top
        else "Keep watching for a sharper processor-specific pattern before changing focus."
    )
    return _finalize_payload(
        "pattern_summary",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "patterns_visible": len(patterns),
                "top_pattern": top.get("headline") or "none",
                "top_pattern_velocity_24h": top.get("velocity_24h", 0),
                "top_pattern_merchants": top.get("affected_merchant_count", 0),
            },
            "snapshot": {
                "patterns_visible": len(patterns),
                "top_pattern": top.get("headline") or "none",
                "top_pattern_velocity_24h": top.get("velocity_24h", 0),
                "top_pattern_merchants": top.get("affected_merchant_count", 0),
            },
            "focus": {
                "type": "pattern",
                "pattern_key": top.get("pattern_key"),
                "label": top.get("headline") or "distress patterns",
            },
        },
    )


def get_actionable_patterns_payload() -> dict:
    patterns = list_actionable_patterns_command(limit=5).get("patterns", [])
    top = patterns[0] if patterns else {}
    summary = (
        f"{top.get('headline')} This is the clearest actionable pattern right now."
        if top
        else "No distress pattern looks actionable enough to pull focus right now."
    )
    matters = (
        " ".join(
            part
            for part in [
                _pattern_interpretation_line(top),
                _pattern_actionability_text(top),
                _pattern_strategy_line(top),
                _pattern_conversion_line(top),
            ]
            if part
        )
        if top
        else "Patterns are either quiet, fading, or not backed by enough conversion signal yet."
    )
    recommendation = (
        _pattern_recommendation(top)
        if top
        else "Stay with the current lead queue until a sharper pattern forms."
    )
    return _finalize_payload(
        "actionable_patterns",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "actionable_patterns_visible": len(patterns),
                "top_actionable_pattern": top.get("headline") or "none",
                "top_actionable_trend": top.get("trend_label") or "none",
                "top_actionable_converted_before": bool(top.get("converted_before")),
                "top_actionable_operator_action": top.get("recommended_operator_action") or "unknown",
            },
            "snapshot": {
                "actionable_patterns_visible": len(patterns),
                "top_actionable_pattern": top.get("headline") or "none",
                "top_actionable_trend": top.get("trend_label") or "none",
                "top_actionable_converted_before": bool(top.get("converted_before")),
                "top_actionable_operator_action": top.get("recommended_operator_action") or "unknown",
            },
            "focus": {
                "type": "pattern",
                "pattern_key": top.get("pattern_key"),
                "label": top.get("headline") or "actionable patterns",
            },
        },
    )


def get_merchant_profiles_payload() -> dict:
    profiles = list_merchant_profiles_command(limit=5).get("profiles", [])
    top = _select_focus_profile(profiles)
    label = _merchant_profile_label(top)
    summary = (
        f"{label} is the strongest merchant profile on the board right now."
        if top
        else "No merchant profile is standing out enough to escalate right now."
    )
    matters = (
        _merchant_doctrine_summary(top)
        if top
        else "The pipeline is still collecting merchant context, but there is no higher-signal merchant profile to surface yet."
    )
    recommendation = (
        _merchant_profile_recommendation(top)
        if top
        else "Stay with the current opportunity queue and wait for a stronger merchant profile."
    )
    return _finalize_payload(
        "merchant_profiles",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "profiles_visible": len(profiles),
                "top_merchant": label or "none",
                "top_processor": top.get("processor") or "unknown",
                "top_distress_type": top.get("distress_type") or "unknown",
                "top_urgency_score": top.get("urgency_score", 0),
                "top_operator_action": top.get("recommended_operator_action") or "unknown",
            },
            "snapshot": {
                "profiles_visible": len(profiles),
                "top_merchant": label or "none",
                "top_processor": top.get("processor") or "unknown",
                "top_distress_type": top.get("distress_type") or "unknown",
                "top_urgency_score": top.get("urgency_score", 0),
                "top_operator_action": top.get("recommended_operator_action") or "unknown",
            },
            "focus": {
                "type": "opportunity" if top.get("opportunity_id") else "merchant_profile",
                "opportunity_id": top.get("opportunity_id"),
                "merchant_domain": top.get("merchant_domain"),
                "label": label or "merchant profiles",
            },
        },
    )


def get_conversion_intelligence_payload() -> dict:
    patterns = show_conversion_intelligence_command(limit=5).get("patterns", [])
    top = next((row for row in patterns if row.get("wins", 0) > 0), patterns[0] if patterns else {})
    label = _conversion_pattern_label(top)
    if top and top.get("wins", 0) > 0:
        summary = f"{label} is converting better than the rest of the current opportunity mix."
        matters = (
            f"It has {top.get('wins', 0)} won opportunities out of {top.get('opportunity_count', 0)} tracked, "
            f"for a win rate of {round(float(top.get('win_rate', 0.0)) * 100, 1)}%. "
            f"{_conversion_learning_line(top)}"
        )
        recommendation = f"Lean into {label} opportunities when deciding what to review next."
    elif top:
        summary = "Closed-loop conversion data is still thin."
        matters = (
            f"The busiest tracked pattern right now is {label}, but it still has "
            f"{top.get('pending', 0)} pending opportunities and no wins yet. "
            f"{_conversion_learning_line(top)}"
        )
        recommendation = f"Treat {label} as active demand, but do not over-read conversion quality yet."
    else:
        summary = "Conversion intelligence is not populated enough to call a winning pattern yet."
        matters = "The system is tracking outcomes, but there is not enough data to say which distress pattern converts best."
        recommendation = "Keep collecting outcomes before changing the review strategy."
    return _finalize_payload(
        "conversion_intelligence",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "conversion_patterns_visible": len(patterns),
                "top_conversion_pattern": label or "none",
                "top_conversion_wins": top.get("wins", 0),
                "top_conversion_pending": top.get("pending", 0),
                "top_conversion_win_rate": top.get("win_rate", 0.0),
            },
            "snapshot": {
                "conversion_patterns_visible": len(patterns),
                "top_conversion_pattern": label or "none",
                "top_conversion_wins": top.get("wins", 0),
                "top_conversion_pending": top.get("pending", 0),
                "top_conversion_win_rate": top.get("win_rate", 0.0),
            },
            "focus": {
                "type": "conversion_intelligence",
                "label": label or "conversion intelligence",
            },
        },
    )


def get_best_conversion_payload() -> dict:
    patterns = show_best_conversion_patterns_command(limit=5).get("patterns", [])
    top = patterns[0] if patterns else {}
    label = _conversion_pattern_label(top)
    if top and top.get("wins", 0) > 0:
        summary = f"{label} has the best proven chance to convert right now."
        matters = (
            f"It has {top.get('wins', 0)} win(s), {top.get('pending', 0)} still open, "
            f"and a win rate of {round(float(top.get('win_rate', 0.0)) * 100, 1)}%. "
            f"{_conversion_learning_line(top)}"
        )
        recommendation = f"Use {label} as a priority tiebreaker when several leads feel equally urgent."
    elif top:
        summary = "No pattern has a proven conversion edge yet."
        matters = (
            f"The strongest current candidate is {label}, but it has "
            f"{top.get('pending', 0)} open opportunities and no closed wins yet. "
            f"{_conversion_learning_line(top)}"
        )
        recommendation = f"Treat {label} as active demand, but do not call it the best converter yet."
    else:
        summary = "I do not have enough closed-loop data to name a best-converting pattern yet."
        matters = "Outcome tracking is live, but it still needs more operator-marked results."
        recommendation = "Keep marking wins, losses, and ignored leads so the conversion view gets sharper."
    return _finalize_payload(
        "best_conversion",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "best_conversion_pattern": label or "none",
                "best_conversion_wins": top.get("wins", 0),
                "best_conversion_pending": top.get("pending", 0),
                "best_conversion_win_rate": top.get("win_rate", 0.0),
            },
            "snapshot": {
                "best_conversion_pattern": label or "none",
                "best_conversion_wins": top.get("wins", 0),
                "best_conversion_pending": top.get("pending", 0),
                "best_conversion_win_rate": top.get("win_rate", 0.0),
            },
            "focus": {
                "type": "conversion_intelligence",
                "label": label or "best conversion chance",
            },
        },
    )


def get_opportunity_outcome_payload(opportunity_id: int, outcome_status: str, outcome_reason: str = "") -> dict:
    result = mark_opportunity_outcome_command(opportunity_id=opportunity_id, outcome_status=outcome_status, outcome_reason=outcome_reason)
    if result.get("error"):
        return _finalize_payload(
            "opportunity_outcome_update",
            {
                "summary": "I could not update that outcome.",
                "matters": result.get("error"),
                "recommendation": "Check the opportunity id and try again.",
                "telemetry": {"outcome_update_status": "error"},
                "snapshot": {"outcome_update_status": "error"},
                "focus": {"type": "opportunity", "opportunity_id": opportunity_id, "label": str(opportunity_id)},
            },
        )

    lead = _assemble_lead_context(opportunity_id=opportunity_id)
    label = _lead_label(lead or result)
    readable = {"won": "won", "lost": "lost", "ignored": "ignored"}.get(result.get("outcome_status"), result.get("outcome_status"))
    previous = result.get("previous_outcome_status")
    summary = (
        f"I marked {label} as {readable}."
        if not previous or previous == readable
        else f"I changed {label} from {previous} to {readable}."
    )
    matters_parts = [f"This updates the closed-loop conversion view for {_conversion_pattern_label(result)}."]
    if result.get("outcome_reason"):
        matters_parts.append(f"Reason noted: {result.get('outcome_reason')}.")
    return _finalize_payload(
        "opportunity_outcome_update",
        {
            "summary": summary,
            "matters": " ".join(matters_parts),
            "recommendation": "Keep marking outcomes as they close so the conversion view becomes more trustworthy.",
            "telemetry": {
                "outcome_update_status": "ok",
                "opportunity_id": opportunity_id,
                "outcome_status": readable,
                "pattern_key": result.get("pattern_key") or "",
            },
            "snapshot": {
                "outcome_update_status": "ok",
                "opportunity_id": opportunity_id,
                "outcome_status": readable,
                "pattern_key": result.get("pattern_key") or "",
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": opportunity_id,
                "merchant_domain": (lead or {}).get("merchant_domain") or result.get("merchant_domain"),
                "label": label,
            },
        },
    )


def get_opportunity_action_fit_payload(
    opportunity_id: int | None = None,
    merchant_domain: str | None = None,
    pattern_key: str | None = None,
) -> dict:
    if opportunity_id or merchant_domain:
        lead_context = _assemble_lead_context(opportunity_id=opportunity_id, merchant_domain=merchant_domain)
        if not lead_context:
            return _finalize_payload(
                "opportunity_action_fit",
                {
                    "summary": "I don’t have a lead-specific action recommendation yet.",
                    "matters": "I need a specific opportunity in focus before I can map it to a canonical play.",
                    "recommendation": "Open the lead first, then ask what action fits the case.",
                    "telemetry": {"action_fit_status": "unavailable"},
                    "snapshot": {"action_fit_status": "unavailable"},
                    "focus": {"type": "opportunity", "label": merchant_domain or str(opportunity_id or "opportunity")},
                },
            )
        action_info = show_opportunity_operator_action_command(opportunity_id=int(lead_context.get("id")))
        action = action_info.get("recommended_action") or lead_context.get("recommended_operator_action")
        summary = f"{_operator_action_label(action).title()} is the best-fit operator action for {_lead_label(lead_context)}."
        matters = " ".join(
            part for part in [
                doctrine_priority_reason(
                    processor=lead_context.get("processor"),
                    distress_type=lead_context.get("distress_type"),
                    industry=lead_context.get("industry"),
                ),
                _lead_action_fit_line(lead_context),
            ] if part
        )
        recommendation = _lead_next_action_line(lead_context, action)
        return _finalize_payload(
            "opportunity_action_fit",
            {
                "summary": summary,
                "matters": matters,
                "recommendation": recommendation,
                "telemetry": {
                    "opportunity_id": lead_context.get("id"),
                    "merchant_domain": lead_context.get("merchant_domain") or "unknown",
                    "recommended_operator_action": action or "unknown",
                    "selected_operator_action": action_info.get("selected_action") or "unknown",
                },
                "snapshot": {
                    "opportunity_id": lead_context.get("id"),
                    "merchant_domain": lead_context.get("merchant_domain") or "unknown",
                    "recommended_operator_action": action or "unknown",
                    "selected_operator_action": action_info.get("selected_action") or "unknown",
                },
                "focus": {
                    "type": "opportunity",
                    "opportunity_id": lead_context.get("id"),
                    "merchant_domain": lead_context.get("merchant_domain"),
                    "label": _lead_label(lead_context),
                },
            },
        )

    patterns = list_actionable_patterns_command(limit=5).get("patterns", [])
    top = next((row for row in patterns if row.get("pattern_key") == pattern_key), patterns[0] if patterns else {})
    if not top:
        return _finalize_payload(
            "pattern_action_fit",
            {
                "summary": "I don’t have a clear pattern-specific action yet.",
                "matters": "No recurring distress pattern is strong enough to map to a play right now.",
                "recommendation": "Stay with the strongest live lead until a sharper pattern forms.",
                "telemetry": {"pattern_action_status": "unavailable"},
                "snapshot": {"pattern_action_status": "unavailable"},
                "focus": {"type": "pattern", "label": "distress patterns"},
            },
        )
    action = top.get("recommended_operator_action") or operator_action_for_distress(top.get("distress_type"))
    return _finalize_payload(
        "pattern_action_fit",
        {
            "summary": f"{_operator_action_label(action).title()} is the best-fit play for this pattern.",
            "matters": " ".join(
                part for part in [
                    _pattern_strategy_line(top),
                    _pattern_conversion_line(top),
                ] if part
            ),
            "recommendation": _pattern_next_action_line(top, action),
            "telemetry": {
                "pattern_key": top.get("pattern_key") or "unknown",
                "recommended_operator_action": action or "unknown",
            },
            "snapshot": {
                "pattern_key": top.get("pattern_key") or "unknown",
                "recommended_operator_action": action or "unknown",
            },
            "focus": {
                "type": "pattern",
                "pattern_key": top.get("pattern_key"),
                "label": top.get("headline") or "distress pattern",
            },
        },
    )


def get_opportunity_action_update_payload(opportunity_id: int, selected_action: str, action_reason: str = "") -> dict:
    result = mark_opportunity_operator_action_command(
        opportunity_id=opportunity_id,
        selected_action=selected_action,
        action_reason=action_reason,
    )
    if result.get("error"):
        return _finalize_payload(
            "opportunity_action_update",
            {
                "summary": "I could not record that operator action.",
                "matters": result.get("error"),
                "recommendation": "Check the opportunity id and try again.",
                "telemetry": {"action_update_status": "error"},
                "snapshot": {"action_update_status": "error"},
                "focus": {"type": "opportunity", "opportunity_id": opportunity_id, "label": str(opportunity_id)},
            },
        )
    lead = _assemble_lead_context(opportunity_id=opportunity_id)
    label = _lead_label(lead or result)
    current = result.get("selected_action_label") or _operator_action_label(result.get("selected_action"))
    previous = result.get("previous_selected_action")
    previous_label = _operator_action_label(previous) if previous else ""
    summary = (
        f"I marked {label} as {current}."
        if not previous or previous == result.get("selected_action")
        else f"I changed {label} from {previous_label} to {current}."
    )
    matters_parts = []
    if result.get("recommended_action"):
        matters_parts.append(
            f"This keeps the lead aligned with the {_operator_action_label(result.get('recommended_action'))} play."
        )
    if result.get("action_reason"):
        matters_parts.append(f"Reason noted: {result.get('action_reason')}.")
    return _finalize_payload(
        "opportunity_action_update",
        {
            "summary": summary,
            "matters": " ".join(matters_parts) or "The selected operator play is now recorded.",
            "recommendation": "Keep marking the chosen play so the operator record stays aligned with the doctrine.",
            "telemetry": {
                "action_update_status": "ok",
                "opportunity_id": result.get("opportunity_id"),
                "selected_action": result.get("selected_action"),
                "recommended_action": result.get("recommended_action"),
            },
            "snapshot": {
                "action_update_status": "ok",
                "opportunity_id": result.get("opportunity_id"),
                "selected_action": result.get("selected_action"),
                "recommended_action": result.get("recommended_action"),
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain"),
                "label": label,
            },
        },
    )


def get_selected_action_for_lead_payload(opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict:
    lead_context = _assemble_lead_context(opportunity_id=opportunity_id, merchant_domain=merchant_domain)
    if not lead_context:
        return _finalize_payload(
            "selected_action_for_lead",
            {
                "summary": "I don’t have a lead-specific action record yet.",
                "matters": "I need a specific opportunity in focus before I can review the selected action.",
                "recommendation": "Open the lead first, then ask what action is selected for it.",
                "telemetry": {"action_review_status": "unavailable"},
                "snapshot": {"action_review_status": "unavailable"},
                "focus": {"type": "opportunity", "label": merchant_domain or str(opportunity_id or "opportunity")},
            },
        )
    action_info = show_opportunity_operator_action_command(opportunity_id=int(lead_context.get("id")))
    selected_action = action_info.get("selected_action") or ""
    has_selection = bool(action_info.get("has_manual_selection"))
    recommended_action = action_info.get("recommended_action") or lead_context.get("recommended_operator_action")
    current_action_label = _operator_action_label(selected_action) if has_selection else "no selected action"
    summary = (
        f"{current_action_label.title()} is currently selected for {_lead_label(lead_context)}."
        if has_selection
        else f"No operator action has been selected yet for {_lead_label(lead_context)}."
    )
    matters_parts = []
    if has_selection:
        if action_info.get("action_reason"):
            matters_parts.append(f"It was chosen because {action_info.get('action_reason')}.")
        if selected_action == recommended_action:
            matters_parts.append("The selected action still matches the current recommendation.")
        else:
            matters_parts.append(
                f"The selected action differs from the current recommendation, which is {_operator_action_label(recommended_action)}."
            )
    else:
        matters_parts.append(
            f"The current recommendation is {_operator_action_label(recommended_action)}."
        )
    recommendation = (
        _lead_next_action_line(lead_context, selected_action or recommended_action)
        if lead_context.get("status") in {"pending_review", "approved"}
        else "No immediate action is needed unless you want to reopen the lead."
    )
    return _finalize_payload(
        "selected_action_for_lead",
        {
            "summary": summary,
            "matters": " ".join(matters_parts),
            "recommendation": recommendation,
            "telemetry": {
                "opportunity_id": lead_context.get("id"),
                "selected_action": selected_action or "none",
                "recommended_action": recommended_action or "unknown",
                "matches_recommendation": bool(has_selection and selected_action == recommended_action),
            },
            "snapshot": {
                "opportunity_id": lead_context.get("id"),
                "selected_action": selected_action or "none",
                "recommended_action": recommended_action or "unknown",
                "matches_recommendation": bool(has_selection and selected_action == recommended_action),
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": lead_context.get("id"),
                "merchant_domain": lead_context.get("merchant_domain"),
                "label": _lead_label(lead_context),
            },
        },
    )


def get_outreach_recommendation_payload(opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict:
    result = get_outreach_recommendation_command(opportunity_id=opportunity_id, merchant_domain=merchant_domain or "")
    if result.get("error"):
        return _finalize_payload(
            "outreach_recommendation",
            {
                "summary": "I do not have a safe outreach recommendation for that lead yet.",
                "matters": result.get("error"),
                "recommendation": "Open a specific lead first so I can anchor the outreach play to real merchant context.",
                "telemetry": {"outreach_recommendation_status": "unavailable"},
                "snapshot": {"outreach_recommendation_status": "unavailable"},
                "focus": {"type": "opportunity", "label": merchant_domain or str(opportunity_id or "lead")},
            },
        )
    lead = _assemble_lead_context(opportunity_id=result.get("opportunity_id"), merchant_domain=result.get("merchant_domain"))
    label = _lead_label(lead or result)
    readiness_state = result.get("readiness_state") or "blocked_contact_quality"
    if readiness_state == "blocked_contact_quality":
        summary = f"{label} is blocked on contact quality right now."
        recommendation = result.get("next_contact_move") or result.get("wait_reason") or "Wait until the lead has a trusted merchant contact."
    elif readiness_state == "approval_ready":
        summary = f"{label} is approval-ready."
        recommendation = "Review the draft, then approve it if the distress is still live."
    elif readiness_state == "send_ready":
        summary = f"{label} is send-ready from a contact-trust standpoint."
        recommendation = (
            "Draft the Gmail outreach now for the "
            f"{_operator_action_label(result.get('best_play'))} play, then queue it for approval."
            if result.get("outreach_status") in {None, '', 'no_outreach'}
            else "Send the approved Gmail outreach when you want this case to go live."
        )
    elif readiness_state == "follow_up_needed":
        summary = f"{label} is waiting on a follow-up."
        recommendation = "Draft the next follow-up if the original distress is still active."
    elif readiness_state == "replied":
        summary = f"{label} has replied."
        recommendation = "Review the reply before sending anything else."
    else:
        summary = f"{label} is not ready for outreach yet."
        recommendation = result.get("wait_reason") or "Wait until the lead has a trusted merchant contact and the current distress is still live."
    matters = " ".join(
        part
        for part in [
            result.get("why_now"),
            f"The current contact state is {str(result.get('contact_state') or 'no_contact_found').replace('_', ' ')}.",
            f"The canonical strategy here is {result.get('strategy')}." if result.get("strategy") else "",
            f"The best channel right now is {result.get('best_channel')}." if result.get("best_channel") else "",
        ]
        if part
    )
    return _finalize_payload(
        "outreach_recommendation",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain") or "unknown",
                "best_channel": result.get("best_channel") or "unknown",
                "best_play": result.get("best_play") or "unknown",
                "urgency": result.get("urgency") or "unknown",
                "should_proceed_now": bool(result.get("should_proceed_now")),
                "readiness_state": readiness_state,
                "contact_quality_label": result.get("contact_quality_label") or "blocked",
            },
            "snapshot": {
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain") or "unknown",
                "best_channel": result.get("best_channel") or "unknown",
                "best_play": result.get("best_play") or "unknown",
                "urgency": result.get("urgency") or "unknown",
                "should_proceed_now": bool(result.get("should_proceed_now")),
                "readiness_state": readiness_state,
                "contact_quality_label": result.get("contact_quality_label") or "blocked",
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain"),
                "label": label,
            },
        },
    )


def get_contact_intelligence_payload(opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict:
    result = get_contact_intelligence_command(opportunity_id=opportunity_id, merchant_domain=merchant_domain or "")
    if result.get("error"):
        return _finalize_payload(
            "contact_intelligence",
            {
                "summary": "I do not have a contact intelligence read for that lead yet.",
                "matters": result.get("error"),
                "recommendation": "Open a specific lead first so I can anchor the contact read to the right merchant.",
                "telemetry": {"contact_intelligence_status": "unavailable"},
                "snapshot": {"contact_intelligence_status": "unavailable"},
                "focus": {"type": "opportunity", "label": merchant_domain or str(opportunity_id or "lead")},
            },
        )
    lead = _assemble_lead_context(opportunity_id=result.get("opportunity_id"), merchant_domain=result.get("merchant_domain"))
    label = _lead_label(lead or result)
    contact_state = result.get("contact_state") or "no_contact_found"
    page_type = str(result.get("contact_page_type") or "").replace("_", " ")
    if contact_state == "verified_contact_found":
        summary = f"{label} has a verified outreach contact on file from the merchant {page_type or 'website'}."
    elif contact_state == "weak_contact_found":
        summary = f"{label} only has a weak contact on file right now."
    else:
        summary = f"{label} does not have a usable merchant-owned contact on file yet."
    matters = " ".join(
        part
        for part in [
            result.get("contact_summary"),
            result.get("contact_source_explanation"),
            f"The winning page was {page_type} at {result.get('contact_page_url')}."
            if result.get("contact_page_url")
            else "",
            result.get("contact_reason") if contact_state != "verified_contact_found" else "",
            result.get("contact_deepening_summary") if contact_state != "verified_contact_found" else "",
            f"The current contact state is {contact_state.replace('_', ' ')}.",
            f"The best target roles are {', '.join(result.get('best_target_roles') or [])} because {result.get('target_role_reason')}."
            if result.get("best_target_roles")
            else "",
        ]
        if part
    )
    recommendation = result.get("next_contact_move") or "Find a trusted same-domain merchant contact before sending outreach."
    return _finalize_payload(
        "contact_intelligence",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain") or "unknown",
                "contact_state": contact_state,
                "contact_trust_score": int(result.get("contact_trust_score") or 0),
                "contact_quality_label": result.get("contact_quality_label") or "blocked",
                "contact_email": result.get("contact_email") or "",
                "contact_source_class": result.get("contact_source_class") or "unknown_source",
                "contact_page_type": result.get("contact_page_type") or "",
            },
            "snapshot": {
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain") or "unknown",
                "contact_state": contact_state,
                "contact_trust_score": int(result.get("contact_trust_score") or 0),
                "contact_quality_label": result.get("contact_quality_label") or "blocked",
                "contact_source_class": result.get("contact_source_class") or "unknown_source",
                "contact_page_type": result.get("contact_page_type") or "",
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain"),
                "label": label,
            },
        },
    )


def get_outreach_draft_payload(
    opportunity_id: int | None = None,
    merchant_domain: str | None = None,
    outreach_type: str | None = None,
) -> dict:
    result = draft_outreach_for_opportunity_command(
        opportunity_id=opportunity_id,
        merchant_domain=merchant_domain or "",
        outreach_type=outreach_type or "",
    )
    if result.get("error"):
        return _finalize_payload(
            "outreach_draft",
            {
                "summary": "I could not draft outreach for that lead yet.",
                "matters": result.get("error"),
                "recommendation": "Make sure the lead still has a live distress signal and a reachable merchant email before drafting.",
                "telemetry": {"outreach_draft_status": "error"},
                "snapshot": {"outreach_draft_status": "error"},
                "focus": {"type": "opportunity", "label": merchant_domain or str(opportunity_id or "lead")},
            },
        )
    lead = _assemble_lead_context(opportunity_id=result.get("opportunity_id"), merchant_domain=result.get("merchant_domain"))
    label = _lead_label(lead or result)
    summary = f"I drafted Gmail outreach for {label}."
    matters = " ".join(
        part
        for part in [
            result.get("why_now"),
            f"The selected play is {result.get('best_play_label')}." if result.get("best_play_label") else "",
            f"The draft is now {result.get('approval_state')}." if result.get("approval_state") else "",
        ]
        if part
    )
    recommendation = (
        "Review the draft and approve it if the distress is still live."
        if result.get("approval_state") == "approval_required"
        else "Review the draft, then send it when you are comfortable."
    )
    return _finalize_payload(
        "outreach_draft",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain") or "unknown",
                "approval_state": result.get("approval_state") or "unknown",
                "outreach_type": result.get("outreach_type") or "unknown",
                "best_play": result.get("best_play") or "unknown",
                "draft_message_id": result.get("draft_message_id") or "",
            },
            "snapshot": {
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain") or "unknown",
                "approval_state": result.get("approval_state") or "unknown",
                "outreach_type": result.get("outreach_type") or "unknown",
                "best_play": result.get("best_play") or "unknown",
                "draft_message_id": result.get("draft_message_id") or "",
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain"),
                "label": label,
            },
        },
    )


def get_outreach_approval_payload(opportunity_id: int) -> dict:
    result = approve_outreach_for_opportunity_command(opportunity_id=opportunity_id)
    if result.get("error"):
        return _finalize_payload(
            "outreach_approval",
            {
                "summary": "I could not approve that outreach yet.",
                "matters": result.get("error"),
                "recommendation": "Draft the outreach first, then approve it explicitly.",
                "telemetry": {"outreach_approval_status": "error"},
                "snapshot": {"outreach_approval_status": "error"},
                "focus": {"type": "opportunity", "opportunity_id": opportunity_id, "label": str(opportunity_id)},
            },
        )
    lead = _assemble_lead_context(opportunity_id=opportunity_id)
    label = _lead_label(lead or result)
    return _finalize_payload(
        "outreach_approval",
        {
            "summary": f"I approved outreach for {label}.",
            "matters": f"The selected play is {_operator_action_label(result.get('selected_play'))}, and the draft is now ready to send when you explicitly say so.",
            "recommendation": "Send it when you want the outreach to go live.",
            "telemetry": {
                "opportunity_id": opportunity_id,
                "approval_state": result.get("approval_state") or "unknown",
                "outreach_status": result.get("outreach_status") or "unknown",
            },
            "snapshot": {
                "opportunity_id": opportunity_id,
                "approval_state": result.get("approval_state") or "unknown",
                "outreach_status": result.get("outreach_status") or "unknown",
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": opportunity_id,
                "merchant_domain": (lead or {}).get("merchant_domain"),
                "label": label,
            },
        },
    )


def get_outreach_send_payload(opportunity_id: int) -> dict:
    result = send_outreach_for_opportunity_command(opportunity_id=opportunity_id)
    if result.get("error"):
        return _finalize_payload(
            "outreach_send",
            {
                "summary": "I could not send that outreach yet.",
                "matters": result.get("error"),
                "recommendation": "Make sure the draft is explicitly approved before sending.",
                "telemetry": {"outreach_send_status": "error"},
                "snapshot": {"outreach_send_status": "error"},
                "focus": {"type": "opportunity", "opportunity_id": opportunity_id, "label": str(opportunity_id)},
            },
        )
    lead = _assemble_lead_context(opportunity_id=opportunity_id)
    label = _lead_label(lead or result)
    return _finalize_payload(
        "outreach_send",
        {
            "summary": f"I sent Gmail outreach for {label}.",
            "matters": "The execution path is still approval-gated, and this send was operator-approved before it went out.",
            "recommendation": "Watch for a reply and review follow-ups when they come due.",
            "telemetry": {
                "opportunity_id": opportunity_id,
                "outreach_status": result.get("outreach_status") or "unknown",
                "gmail_thread_id": result.get("gmail_thread_id") or "",
                "follow_up_due_at": result.get("follow_up_due_at") or "",
            },
            "snapshot": {
                "opportunity_id": opportunity_id,
                "outreach_status": result.get("outreach_status") or "unknown",
                "gmail_thread_id": result.get("gmail_thread_id") or "",
                "follow_up_due_at": result.get("follow_up_due_at") or "",
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": opportunity_id,
                "merchant_domain": (lead or {}).get("merchant_domain"),
                "label": label,
            },
        },
    )


def get_outreach_awaiting_approval_payload() -> dict:
    result = list_outreach_awaiting_approval_command(limit=10)
    cases = result.get("cases", [])
    top = cases[0] if cases else {}
    lead = _assemble_lead_context(opportunity_id=top.get("opportunity_id")) if top else None
    label = _lead_label(lead or top)
    summary = (
        f"{result.get('count', 0)} lead(s) are ready for outreach approval."
        if cases
        else "There are no outreach drafts waiting for approval right now."
    )
    matters = (
        f"{label} is the best send-now case because {_outreach_case_why_now(top, lead)}"
        if top
        else "The outreach queue is clear right now."
    )
    recommendation = (
        f"Review {label} first, then approve it if the distress context is still live."
        if top
        else "No approval action is needed right now."
    )
    return _finalize_payload(
        "outreach_awaiting_approval",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "outreach_awaiting_approval": result.get("count", 0),
                "top_outreach_case": label or "none",
            },
            "snapshot": {
                "outreach_awaiting_approval": result.get("count", 0),
                "top_outreach_case": label or "none",
            },
            "focus": {
                "type": "opportunity" if top else "outreach_queue",
                "opportunity_id": top.get("opportunity_id"),
                "merchant_domain": (lead or {}).get("merchant_domain") or top.get("merchant_domain"),
                "label": label or "outreach approval queue",
            },
        },
    )


def get_outreach_follow_up_payload() -> dict:
    result = list_sent_outreach_needing_follow_up_command(limit=10)
    cases = result.get("cases", [])
    top = cases[0] if cases else {}
    lead = _assemble_lead_context(opportunity_id=top.get("opportunity_id")) if top else None
    label = _lead_label(lead or top)
    replied_count = len([case for case in cases if case.get("status") == "replied"])
    due_count = len([case for case in cases if case.get("status") == "follow_up_needed"])
    summary = (
        f"{due_count} sent outreach case(s) are waiting on follow-up."
        if due_count
        else (
            f"{replied_count} outreach reply thread(s) need review."
            if replied_count
            else "There is no sent outreach waiting on follow-up right now."
        )
    )
    if top and top.get("status") == "replied":
        matters = f"{label} has replied, so the next move is to review the response and decide whether a reply-follow-up is warranted."
        recommendation = f"Read the reply from {label} before sending anything else."
    elif top:
        matters = f"{label} is due for a follow-up because the last approved outreach is still open and no reply has landed yet."
        recommendation = f"Draft the next follow-up for {label} if the original distress context is still live."
    else:
        matters = "The current outreach queue is not waiting on replies or follow-ups."
        recommendation = "Stay with new approvals or fresh lead review until follow-ups become due."
    return _finalize_payload(
        "outreach_follow_up",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "follow_ups_due": due_count,
                "outreach_replied": replied_count,
                "top_follow_up_case": label or "none",
            },
            "snapshot": {
                "follow_ups_due": due_count,
                "outreach_replied": replied_count,
                "top_follow_up_case": label or "none",
            },
            "focus": {
                "type": "opportunity" if top else "outreach_follow_up",
                "opportunity_id": top.get("opportunity_id"),
                "merchant_domain": (lead or {}).get("merchant_domain") or top.get("merchant_domain"),
                "label": label or "outreach follow-up",
            },
        },
    )


def get_send_eligible_outreach_payload() -> dict:
    result = show_sendable_queue_command(limit=10)
    cases = result.get("cases", [])
    top = cases[0] if cases else {}
    lead = _assemble_lead_context(opportunity_id=top.get("opportunity_id")) if top else None
    label = _lead_label(lead or top)
    summary = (
        f"{result.get('count', 0)} lead(s) are send-eligible based on trusted merchant contact quality."
        if cases
        else "There are no send-eligible leads right now."
    )
    matters = (
        f"{label} is the clearest live case because {top.get('contact_email') or 'the saved contact'} came from the merchant {str(top.get('contact_page_type') or 'website').replace('_', ' ')}, "
        f"the source class is {str(top.get('contact_source_class') or 'verified_same_domain').replace('_', ' ')}, "
        f"and {_operator_action_label(top.get('best_play'))} is the active play. "
        f"{top.get('contact_source_explanation') or ''}".strip()
        if top
        else "The current queue is still blocked on contact trust or waiting on other steps."
    )
    recommendation = (
        f"Start with {label}. If a draft already exists and it is approved, send it. Otherwise draft the Gmail outreach for the {_operator_action_label(top.get('best_play'))} play."
        if top
        else "Focus on blocked leads and contact cleanup before trying to send more outreach."
    )
    return _finalize_payload(
        "outreach_send_eligible",
        {
            "summary": summary.replace("lead(s) are send-eligible", "lead(s) are in the sendable queue"),
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "send_eligible_leads": result.get("count", 0),
                "top_send_ready_case": label or "none",
            },
            "snapshot": {
                "send_eligible_leads": result.get("count", 0),
                "top_send_ready_case": label or "none",
            },
            "focus": {
                "type": "opportunity" if top else "outreach_queue",
                "opportunity_id": top.get("opportunity_id"),
                "merchant_domain": (lead or {}).get("merchant_domain") or top.get("merchant_domain"),
                "label": label or "send-eligible outreach",
            },
        },
    )


def get_blocked_outreach_leads_payload() -> dict:
    result = show_contact_blocked_queue_command(limit=10)
    cases = result.get("cases", [])
    top = cases[0] if cases else {}
    lead = _assemble_lead_context(opportunity_id=top.get("opportunity_id")) if top else None
    label = _lead_label(lead or top)
    outreach_metrics = show_outreach_metrics_command()
    suppressed = int(outreach_metrics.get("suppressed_from_blocked_queue_24h", 0) or 0)
    summary = (
        f"{result.get('count', 0)} lead(s) are blocked on contact quality."
        if cases
        else "There are no contact-quality blocks in the outreach queue right now."
    )
    matters = (
        f"{label} is blocked because {(top.get('contact_reason') or 'the contact is not trusted enough yet').rstrip('.')}. "
        f"The current contact state is {str(top.get('contact_state') or 'weak_contact_found').replace('_', ' ')}. "
        f"{top.get('contact_deepening_summary') or ''}".strip()
        if top
        else (
            "The primary blocked queue is now clear of contact-quality blocks."
            if suppressed <= 0
            else f"Most of the previous blocked queue was weak consumer-noise or non-merchant domains, so {suppressed} case(s) are now suppressed from the primary blocked queue."
        )
    )
    recommendation = (
        f"Do not send {label} yet. {top.get('next_contact_move') or 'Resolve the contact-quality issue first.'}"
        if top
        else "Move to send-eligible leads or approval-ready drafts; research-only and unpromotable cases are no longer part of the primary blocked queue."
    )
    return _finalize_payload(
        "outreach_blocked",
        {
            "summary": summary.replace("lead(s) are blocked on contact quality", "lead(s) are in the contact-blocked queue"),
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "blocked_outreach_leads": result.get("count", 0),
                "top_blocked_case": label or "none",
            },
            "snapshot": {
                "blocked_outreach_leads": result.get("count", 0),
                "top_blocked_case": label or "none",
            },
            "focus": {
                "type": "opportunity" if top else "outreach_queue",
                "opportunity_id": top.get("opportunity_id"),
                "merchant_domain": (lead or {}).get("merchant_domain") or top.get("merchant_domain"),
                "label": label or "blocked outreach",
            },
        },
    )


def get_contact_acquisition_payload() -> dict:
    result = advance_contact_acquisition_command(limit=10)
    if result.get("status") != "ok":
        return _finalize_payload(
            "contact_acquisition",
            {
                "summary": "There is no contact-acquisition move to run right now.",
                "matters": "The primary contact-blocked queue is empty or the best blocked case does not have enough merchant context yet.",
                "recommendation": "Work the sendable queue first, then come back to contact acquisition if a real blocked case appears.",
                "telemetry": {
                    "contact_acquisition_status": result.get("status") or "blocked",
                    "reason": result.get("reason") or "unknown",
                },
                "snapshot": {
                    "contact_acquisition_status": result.get("status") or "blocked",
                    "reason": result.get("reason") or "unknown",
                },
                "focus": {"type": "outreach_queue", "label": "contact acquisition"},
            },
        )

    label = _lead_label(
        {
            "merchant_domain": result.get("merchant_domain") or "",
            "opportunity_id": result.get("opportunity_id"),
        }
    )
    contact_state = str(result.get("contact_state") or "unknown").replace("_", " ")
    created = int(result.get("created") or 0)
    summary = (
        f"Contact acquisition ran for {label}."
        if label
        else "Contact acquisition ran for the top blocked case."
    )
    matters = " ".join(
        part
        for part in [
            f"It created {created} new contact candidate(s)." if created > 0 else "No new contact candidates were persisted on this pass.",
            f"The current contact state is {contact_state}.",
            (
                f"The best contact path is now {result.get('contact_email')}."
                if result.get("contact_send_eligible") and result.get("contact_email")
                else ""
            ),
            result.get("contact_deepening_summary") or "",
            result.get("contact_reason") or "",
        ]
        if part
    )
    recommendation = (
        f"{label} is now sendable. Move it into the sendable queue and draft or send the outreach."
        if result.get("contact_send_eligible")
        else result.get("next_contact_move") or f"Keep working {label} until a trusted same-domain contact is found."
    )
    return _finalize_payload(
        "contact_acquisition",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "contact_acquisition_status": "ok",
                "opportunity_id": result.get("opportunity_id"),
                "contact_candidates_created": created,
                "contact_send_eligible": bool(result.get("contact_send_eligible")),
            },
            "snapshot": {
                "contact_acquisition_status": "ok",
                "opportunity_id": result.get("opportunity_id"),
                "contact_candidates_created": created,
                "contact_send_eligible": bool(result.get("contact_send_eligible")),
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": result.get("opportunity_id"),
                "merchant_domain": result.get("merchant_domain"),
                "label": label or "contact acquisition",
            },
        },
    )


def get_operator_action_cases_payload(
    *,
    selected_action: str = "",
    unselected_only: bool = False,
    mismatched_only: bool = False,
) -> dict:
    result = list_opportunities_by_operator_action_command(
        selected_action=selected_action,
        unselected_only=unselected_only,
        mismatched_only=mismatched_only,
        limit=10,
    )


def get_merchant_action_cases_payload(
    *,
    selected_action: str = "",
    unselected_only: bool = False,
    mismatched_only: bool = False,
) -> dict:
    result = list_merchants_by_operator_action_command(
        selected_action=selected_action,
        unselected_only=unselected_only,
        mismatched_only=mismatched_only,
        limit=10,
    )
    merchants = result.get("merchants", [])
    top = merchants[0] if merchants else {}
    if unselected_only:
        summary = (
            f"{result.get('count', 0)} merchant profile(s) still have no selected operator action."
            if merchants
            else "Every current merchant profile already has a selected operator action."
        )
        matters = (
            f"The highest-priority uncovered merchant is {_merchant_action_label(top)}, with {top.get('opportunity_count', 0)} opportunity record(s) still uncovered."
            if top
            else "The merchant-level action review is fully populated right now."
        )
        recommendation = (
            f"Start by selecting {_operator_action_label(top.get('recommended_action'))} for {_merchant_action_label(top)}."
            if top
            else "No merchant action review is needed right now."
        )
        kind = "merchant_unselected_actions"
    elif mismatched_only:
        summary = (
            f"{result.get('count', 0)} merchant profile(s) have action drift."
            if merchants
            else "Every merchant-level selected action still matches the current recommendation."
        )
        matters = (
            f"The clearest drift is {_merchant_action_label(top)}: selected {_merchant_action_values_text(top.get('selected_action_values', []))}, recommended {_merchant_action_values_text(top.get('recommended_action_values', []))}."
            if top
            else "There is no merchant-level action drift to clean up right now."
        )
        recommendation = (
            f"Review whether {_merchant_action_label(top)} should stay on {_merchant_action_values_text(top.get('selected_action_values', []))} or move back to {_merchant_action_values_text(top.get('recommended_action_values', []))}."
            if top
            else "No merchant action review is needed right now."
        )
        kind = "merchant_action_drift"
    else:
        readable_action = _operator_action_label(selected_action)
        summary = (
            f"{result.get('count', 0)} merchant profile(s) are currently on {readable_action}."
            if merchants
            else f"No merchant profile is currently on {readable_action}."
        )
        matters = (
            f"The top merchant is {_merchant_action_label(top)}, with {top.get('opportunity_count', 0)} linked opportunity record(s)."
            if top
            else f"There is no active merchant queue under {readable_action} right now."
        )
        recommendation = (
            f"Start with {_merchant_action_label(top)} while the {readable_action} context is still fresh."
            if top
            else f"Stay with the broader queue until a merchant clearly fits {readable_action}."
        )
        kind = "merchant_action_cases"
    return _finalize_payload(
        kind,
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "merchant_action_count": result.get("count", 0),
                "selected_action": selected_action or "",
                "top_merchant": _merchant_action_label(top) if top else "none",
            },
            "snapshot": {
                "merchant_action_count": result.get("count", 0),
                "selected_action": selected_action or "",
                "top_merchant": _merchant_action_label(top) if top else "none",
            },
            "focus": {
                "type": "merchant_profile" if top else "merchant_action_cases",
                "merchant_domain": top.get("merchant_domain"),
                "label": _merchant_action_label(top) if top else "merchant action review",
            },
        },
    )


def get_merchant_action_attention_payload() -> dict:
    unselected = list_merchants_by_operator_action_command(unselected_only=True, limit=5)
    drift = list_merchants_by_operator_action_command(mismatched_only=True, limit=5)
    urgent = list_merchants_by_operator_action_command(selected_action="urgent_processor_migration", limit=5)
    top_unselected = (unselected.get("merchants") or [{}])[0]
    top_drift = (drift.get("merchants") or [{}])[0]
    top_urgent = (urgent.get("merchants") or [{}])[0]
    if top_unselected:
        summary = f"{_merchant_action_label(top_unselected)} needs action selection first."
        matters = (
            f"It has {top_unselected.get('opportunity_count', 0)} opportunity record(s) and no selected operator action yet."
        )
        recommendation = (
            f"Start by selecting {_operator_action_label(top_unselected.get('recommended_action'))} for {_merchant_action_label(top_unselected)}."
        )
    elif top_drift:
        summary = f"{_merchant_action_label(top_drift)} needs action review."
        matters = (
            f"The selected action is {_merchant_action_values_text(top_drift.get('selected_action_values', []))}, but the current recommendation is {_merchant_action_values_text(top_drift.get('recommended_action_values', []))}."
        )
        recommendation = (
            f"Review whether {_merchant_action_label(top_drift)} should stay on the current play or move back to the recommended one."
        )
    elif top_urgent:
        summary = f"{_merchant_action_label(top_urgent)} is the strongest merchant already on urgent processor migration."
        matters = (
            f"It has {top_urgent.get('opportunity_count', 0)} linked opportunity record(s) already aligned to that play."
        )
        recommendation = f"Work {_merchant_action_label(top_urgent)} next while the urgent processor migration context is still fresh."
    else:
        summary = "Merchant action review is in a steady state right now."
        matters = "There is no uncovered merchant action queue or action drift demanding immediate attention."
        recommendation = "Stay with the main lead queue until merchant action review changes."
    return _finalize_payload(
        "merchant_action_attention",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "merchant_unselected_count": unselected.get("count", 0),
                "merchant_drift_count": drift.get("count", 0),
                "merchant_urgent_count": urgent.get("count", 0),
            },
            "snapshot": {
                "merchant_unselected_count": unselected.get("count", 0),
                "merchant_drift_count": drift.get("count", 0),
                "merchant_urgent_count": urgent.get("count", 0),
            },
            "focus": {
                "type": "merchant_profile",
                "merchant_domain": (top_unselected or top_drift or top_urgent).get("merchant_domain"),
                "label": _merchant_action_label(top_unselected or top_drift or top_urgent) or "merchant action attention",
            },
        },
    )
    cases = result.get("cases", [])
    top = cases[0] if cases else {}
    if unselected_only:
        summary = (
            f"{result.get('count', 0)} lead(s) still have no selected operator action."
            if cases
            else "Every current lead already has a selected operator action."
        )
        matters = (
            f"The highest-priority uncovered lead is {_action_case_label(top)}, and the current recommendation is {_operator_action_label(top.get('recommended_action'))}."
            if top
            else "The operator action layer is fully populated right now."
        )
        recommendation = (
            f"Start by selecting {_operator_action_label(top.get('recommended_action'))} for {_action_case_label(top)}."
            if top
            else "No action review is needed right now."
        )
        kind = "unselected_operator_actions"
    elif mismatched_only:
        summary = (
            f"{result.get('count', 0)} lead(s) have a selected action that differs from the current recommendation."
            if cases
            else "Every selected action still matches the current recommendation."
        )
        matters = (
            f"The strongest mismatch is {_action_case_label(top)}: selected {_operator_action_label(top.get('selected_action'))}, recommended {_operator_action_label(top.get('recommended_action'))}."
            if top
            else "There is no action drift to clean up right now."
        )
        recommendation = (
            f"Review whether {_action_case_label(top)} should stay on {_operator_action_label(top.get('selected_action'))} or move back to {_operator_action_label(top.get('recommended_action'))}."
            if top
            else "No action review is needed right now."
        )
        kind = "mismatched_operator_actions"
    else:
        readable_action = _operator_action_label(selected_action)
        summary = (
            f"{result.get('count', 0)} lead(s) are currently on {readable_action}."
            if cases
            else f"No current lead is marked for {readable_action}."
        )
        matters = (
            f"The top case is {_action_case_label(top)}, currently in {top.get('opportunity_status')}."
            if top
            else f"There is no active queue under {readable_action} right now."
        )
        recommendation = (
            f"Start with {_action_case_label(top)} while the {readable_action} context is still fresh."
            if top
            else f"Stay with the broader queue until a lead clearly fits {readable_action}."
        )
        kind = "operator_action_cases"
    return _finalize_payload(
        kind,
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "action_case_count": result.get("count", 0),
                "selected_action": selected_action or "",
                "top_case": _action_case_label(top) if top else "none",
            },
            "snapshot": {
                "action_case_count": result.get("count", 0),
                "selected_action": selected_action or "",
                "top_case": _action_case_label(top) if top else "none",
            },
            "focus": {
                "type": "opportunity" if top else "operator_action_cases",
                "opportunity_id": top.get("opportunity_id"),
                "merchant_domain": top.get("merchant_domain"),
                "label": _action_case_label(top) if top else "operator action review",
            },
        },
    )


def get_focus_recommendation_payload() -> dict:
    pattern_payload = get_actionable_patterns_payload()
    merchant_payload = get_merchant_profiles_payload()
    conversion_payload = get_best_conversion_payload()
    outreach_queue = list_outreach_awaiting_approval_command(limit=3)
    top_outreach = (outreach_queue.get("cases") or [{}])[0]
    top_outreach_lead = _assemble_lead_context(opportunity_id=top_outreach.get("opportunity_id")) if top_outreach else None
    top_outreach_metadata = dict(top_outreach.get("metadata_json") or {}) if top_outreach else {}
    top_outreach_has_contact = bool(str((top_outreach or {}).get("contact_email") or "").strip())
    top_outreach_review_only = bool(
        top_outreach
        and (
            str(top_outreach.get("channel") or "").strip().lower() == "review_only"
            or bool(top_outreach_metadata.get("review_only_draft"))
            or not top_outreach_has_contact
        )
    )
    merchant_focus = merchant_payload.get("focus", {})
    top_profile = _focus_profile_snapshot()
    queue_priority = _queue_priority_line(top_profile, counts=_get_opportunity_counts())
    summary = (
        (
            f"{_lead_label(top_outreach_lead or top_outreach)} is the clearest review draft right now."
            if top_outreach_review_only
            else f"{_lead_label(top_outreach_lead or top_outreach)} is the best send-now case right now."
        )
        if top_outreach
        else merchant_payload.get("summary") or pattern_payload.get("summary")
    )
    if top_outreach:
        pending_review = int((_get_opportunity_counts() or {}).get("pending_review", 0) or 0)
        matters = " ".join(
            part for part in [
                (
                    f"{_lead_label(top_outreach_lead or top_outreach)} is in the review queue because {_outreach_case_why_now(top_outreach, top_outreach_lead)}."
                    if top_outreach_review_only
                    else f"{_lead_label(top_outreach_lead or top_outreach)} is ready for outreach approval because {_outreach_case_why_now(top_outreach, top_outreach_lead)}."
                ),
                (
                    (
                        "It is still missing a trusted contact path, so treat it as a review object instead of a send-ready case."
                        if top_outreach_review_only and not top_outreach_has_contact
                        else "It is still staged as review-only, so treat it as a review object instead of a send-ready case."
                    )
                    if top_outreach_review_only
                    else ""
                ),
                (
                    (
                        f"{pending_review} opportunities are still waiting for review behind it, but this is the clearest review object."
                        if top_outreach_review_only
                        else f"{pending_review} opportunities are still waiting for review behind it, but this is the clearest send-now case."
                    )
                    if pending_review > 0
                    else (
                        "This is the clearest send-now case in the queue."
                        if not top_outreach_review_only
                        else "This is the clearest review object in the queue."
                    )
                ),
            ]
            if part
        )
    else:
        matters = " ".join(
            part for part in [
                queue_priority,
                merchant_payload.get("matters"),
                conversion_payload.get("matters"),
            ]
            if part
        )
    recommendation = (
        (
            f"Review the draft for {_lead_label(top_outreach_lead or top_outreach)}, tighten it if the distress is still live, and only move it forward once a real contact path exists."
            if top_outreach_review_only
            else f"Approve and send the draft for {_lead_label(top_outreach_lead or top_outreach)} if the distress is still live, then return to the broader queue."
        )
        if top_outreach
        else _focus_recommendation_line(merchant_payload, pattern_payload, conversion_payload)
    )
    return _finalize_payload(
        "focus_recommendation",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "focus_merchant": merchant_focus.get("label") or "none",
                "focus_pattern": pattern_payload.get("focus", {}).get("label") or "none",
                "focus_conversion_pattern": conversion_payload.get("focus", {}).get("label") or "none",
                "outreach_awaiting_approval": outreach_queue.get("count", 0),
            },
            "snapshot": {
                "focus_merchant": merchant_focus.get("label") or "none",
                "focus_pattern": pattern_payload.get("focus", {}).get("label") or "none",
                "focus_conversion_pattern": conversion_payload.get("focus", {}).get("label") or "none",
                "outreach_awaiting_approval": outreach_queue.get("count", 0),
            },
            "focus": (
                {
                    "type": "opportunity",
                    "opportunity_id": top_outreach.get("opportunity_id"),
                    "merchant_domain": (top_outreach_lead or {}).get("merchant_domain") or top_outreach.get("merchant_domain"),
                    "label": _lead_label(top_outreach_lead or top_outreach),
                }
                if top_outreach
                else merchant_focus or {"type": "focus", "label": "focus recommendation"}
            ),
        },
    )


def get_opportunity_summary_payload() -> dict:
    opportunities = list_deal_opportunities_command(limit=5)
    counts = _get_opportunity_counts()
    latest = opportunities.get("deal_opportunities", [])
    top = latest[0] if latest else {}
    top_label = _display_merchant_name(top.get("merchant_name"), top.get("merchant_domain")) or str(top.get("id") or "latest opportunity")
    top_processor = _humanize_label(top.get("processor")) or "unknown processor"
    top_distress = _humanize_label(top.get("distress_topic")) or "unknown issue"
    top_status = str(top.get("status") or "unknown").replace("_", " ")
    top_eligibility_reason = str(top.get("eligibility_reason") or "").strip()
    summary = (
        f"{counts['pending_review']} opportunities are waiting for review."
        if counts["pending_review"] > 0
        else "There is no urgent opportunity backlog right now."
    )
    matters = (
        (
            (
                f"{counts['pending_review']} opportunities are waiting for review. "
                f"The front of the queue is {top_label} with {top_processor} pressure around {top_distress} and status {top_status}."
                if top
                else f"{counts['pending_review']} opportunities are waiting for review."
            )
        ).strip()
        if counts["pending_review"] > 0
        else (
            f"The newest lead is {top_label} and it is still waiting on operator review."
            if top
            else "No fresh opportunity has landed since the last check."
        )
    )
    recommendation = (
        (
            f"Start with {top_label}. {top_eligibility_reason}"
            if top and top_eligibility_reason
            else f"Start with {top_label}, then move deeper into the queue."
        )
        if counts["pending_review"] > 0
        else "No action is needed beyond monitoring."
    )
    return _finalize_payload(
        "opportunity_summary",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "opportunities_created_24h": counts["created_24h"],
                "opportunities_pending_review": counts["pending_review"],
                "opportunities_approved": counts["approved"],
                "opportunities_converted": counts["converted"],
                "latest_domain": top_label or "none",
            },
            "snapshot": {
                "opportunities_created_24h": counts["created_24h"],
                "opportunities_pending_review": counts["pending_review"],
                "opportunities_approved": counts["approved"],
                "opportunities_converted": counts["converted"],
                "latest_domain": top_label or "none",
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": top.get("id"),
                "merchant_domain": top.get("merchant_domain"),
                "status": top.get("status"),
                "label": top_label or str(top.get("id") or "latest opportunity"),
            },
        },
    )


def get_opportunity_focus_payload(opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict:
    lead_context = _assemble_lead_context(opportunity_id=opportunity_id, merchant_domain=merchant_domain)
    if not lead_context:
        return _finalize_payload(
            "opportunity_focus",
            {
                "summary": "I don’t have a lead-specific record for that opportunity yet.",
                "matters": "I only have queue-level status right now, not a lead-specific update.",
                "recommendation": "Open the opportunity summary again so I can anchor to a specific lead.",
                "telemetry": {
                    "lead_specific_status": "unavailable",
                },
                "snapshot": {
                    "lead_specific_status": "unavailable",
                },
                "focus": {
                    "type": "opportunity",
                    "label": merchant_domain or str(opportunity_id or "opportunity"),
                },
            },
        )

    summary = _lead_state_sentence(lead_context)
    matters = _lead_why_it_matters(lead_context)
    recommendation = (
        _lead_recommendation(lead_context)
        if lead_context.get("status") in {"pending_review", "approved"}
        else "No immediate action is needed unless you want to reopen the lead."
    )
    return _finalize_payload(
        "opportunity_focus",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "lead_id": lead_context.get("id"),
                "merchant_domain": lead_context.get("merchant_domain") or "unknown",
                "merchant_name": lead_context.get("merchant_name") or "unknown",
                "status": lead_context.get("status") or "unknown",
                "processor": lead_context.get("processor") or "unknown",
                "distress_topic": lead_context.get("distress_type") or lead_context.get("distress_topic") or "unknown",
                "last_activity_at": lead_context.get("last_activity_at") or "unknown",
                "recommended_operator_action": lead_context.get("recommended_operator_action") or "unknown",
            },
            "snapshot": {
                "lead_id": lead_context.get("id"),
                "merchant_domain": lead_context.get("merchant_domain") or "unknown",
                "merchant_name": lead_context.get("merchant_name") or "unknown",
                "status": lead_context.get("status") or "unknown",
                "processor": lead_context.get("processor") or "unknown",
                "distress_topic": lead_context.get("distress_type") or lead_context.get("distress_topic") or "unknown",
                "last_activity_at": lead_context.get("last_activity_at") or "unknown",
                "recommended_operator_action": lead_context.get("recommended_operator_action") or "unknown",
            },
            "focus": {
                "type": "opportunity",
                "opportunity_id": lead_context.get("id"),
                "merchant_domain": lead_context.get("merchant_domain"),
                "status": lead_context.get("status"),
                "label": _lead_label(lead_context) or str(lead_context.get("id") or "opportunity"),
            },
        },
    )


def get_model_performance_payload() -> dict:
    reasoning = show_reasoning_metrics_command()
    tier1, tier2 = _provider_success_lines(reasoning)
    summary = "Reasoning looks stable, and the narrowed router is keeping model usage under control."
    matters = (
        f"Tier 1 is running at {tier1} and Tier 2 at {tier2}. "
        f"Rules are still doing most of the work, with {reasoning.get('reasoning_rule_only_decisions_24h', 0)} rule-only decisions in the last day."
    )
    recommendation = (
        "Keep the current narrow routing in place."
        if reasoning.get("reasoning_failures_24h", 0) == 0
        else "Review recent reasoning failures before you trust the models with more scope."
    )
    return _finalize_payload(
        "model_performance",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "telemetry": {
                "reasoning_status": reasoning.get("reasoning_status", "unknown"),
                "rule_only_24h": reasoning.get("reasoning_rule_only_decisions_24h", 0),
                "tier1_calls_24h": reasoning.get("reasoning_tier1_calls_24h", 0),
                "tier2_calls_24h": reasoning.get("reasoning_tier2_calls_24h", 0),
                "reasoning_failures_24h": reasoning.get("reasoning_failures_24h", 0),
                "tier1_success": tier1,
                "tier2_success": tier2,
            },
            "snapshot": {
                "reasoning_status": reasoning.get("reasoning_status", "unknown"),
                "rule_only_24h": reasoning.get("reasoning_rule_only_decisions_24h", 0),
                "tier1_calls_24h": reasoning.get("reasoning_tier1_calls_24h", 0),
                "tier2_calls_24h": reasoning.get("reasoning_tier2_calls_24h", 0),
                "reasoning_failures_24h": reasoning.get("reasoning_failures_24h", 0),
                "tier1_success": tier1,
                "tier2_success": tier2,
            },
            "focus": {
                "type": "reasoning",
                "tier1_model": reasoning_settings().get("tier1_model"),
                "tier2_model": reasoning_settings().get("tier2_model"),
                "label": "model performance",
            },
        },
    )


def _prospect_hypothesis_line(lead: dict, fit: dict) -> str:
    processor = _humanize_label(fit.get("processor") or lead.get("processor"))
    distress = _humanize_label(fit.get("distress_type") or lead.get("distress_type") or lead.get("distress_topic"))
    if processor not in {"unknown", ""} and distress not in {"unknown", ""}:
        return f"Likely {processor} pressure around {distress}."
    if distress not in {"unknown", ""}:
        return f"Likely {distress} that could escalate into a processor problem."
    if processor not in {"unknown", ""}:
        return f"Likely processor pressure tied to {processor}."
    return "Likely processor distress worth qualifying carefully."


def _prospect_contact_line(lead: dict, fit: dict) -> str:
    email = fit.get("contact_email") or lead.get("contact_email") or ""
    trust = int(fit.get("contact_trust_score") or lead.get("contact_trust_score") or 0)
    if email:
        return f"Trusted contact path: {email} (trust {trust})."
    reason = fit.get("contact_reason") or lead.get("contact_reason") or "No trusted merchant-owned contact is on file yet."
    return reason.rstrip(".") + "."


def _prospect_priority_reason(lead: dict, fit: dict) -> str:
    reasons: list[str] = []
    if bool(fit.get("high_conviction_prospect")):
        reasons.append("it already passes the high-conviction filter")
    icp_fit = int(fit.get("icp_fit_score") or 0)
    if icp_fit >= 70:
        reasons.append(f"ICP fit is strong ({icp_fit})")
    trust = int(fit.get("contact_trust_score") or 0)
    if trust >= 80 and fit.get("contact_email"):
        reasons.append("the contact path is strong enough to act on now")
    queue_reason = fit.get("queue_eligibility_reason") or lead.get("queue_eligibility_reason") or ""
    if queue_reason:
        reasons.append(queue_reason.rstrip(".").lower())
    if not reasons:
        return "it is the cleanest prospect in the current overnight set."
    return "Worth your time because " + "; ".join(reasons[:3]) + "."


def _prospect_sales_signals(lead: dict, fit: dict) -> dict:
    return assess_commercial_readiness(
        distress_type=fit.get("distress_type") or lead.get("distress_type") or lead.get("distress_topic") or "unknown",
        processor=fit.get("processor") or lead.get("processor") or "unknown",
        content=lead.get("signal_summary") or lead.get("content_preview") or "",
        why_now=fit.get("why_now") or lead.get("why_now") or lead.get("queue_eligibility_reason") or "",
        contact_email=fit.get("contact_email") or lead.get("contact_email") or "",
        contact_trust_score=int(fit.get("contact_trust_score") or lead.get("contact_trust_score") or 0),
        target_roles=list(lead.get("best_target_roles") or []),
        target_role_reason=lead.get("target_role_reason") or "",
        icp_fit_score=int(fit.get("icp_fit_score") or lead.get("icp_fit_score") or 0),
        queue_quality_score=int(fit.get("queue_quality_score") or lead.get("queue_quality_score") or 0),
        revenue_detected=bool(lead.get("revenue_detected")),
    )


def _build_prospect_dossier(row: dict) -> dict | None:
    if str(row.get("prospect_type") or "").strip() == "signal_candidate":
        merchant_name = str(row.get("merchant_name") or "").strip()
        merchant_domain = str(row.get("merchant_domain") or "").strip()
        label = _display_merchant_name(merchant_name, merchant_domain) or merchant_domain or f"signal {row.get('signal_id')}"
        distress = _humanize_label(row.get("distress_topic") or "processor distress")
        processor = _humanize_label(row.get("processor") or "unknown")
        recommended_action = _operator_action_label(operator_action_for_distress(row.get("distress_topic")))
        source = str(row.get("signal_source") or "signal").strip()
        quality_reason = str(row.get("queue_eligibility_reason") or "").strip()
        evidence = str(row.get("content_preview") or row.get("why_now") or "").strip()
        next_move = (
            "Review the signal, confirm the merchant identity, and decide whether to deepen contact discovery now."
            if "operator review" in quality_reason.lower()
            else "Promote this merchant-backed signal into an actionable opportunity and deepen contact discovery."
        )
        worth_time = "Worth your time because "
        reasons = []
        if bool(row.get("high_conviction_prospect")):
            reasons.append("it already clears Meridian's high-conviction bar")
        if int(row.get("icp_fit_score") or 0) >= 70:
            reasons.append(f"ICP fit is strong ({int(row.get('icp_fit_score') or 0)})")
        if int(row.get("queue_quality_score") or 0) >= 70:
            reasons.append(f"queue quality is strong ({int(row.get('queue_quality_score') or 0)})")
        if quality_reason:
            reasons.append(quality_reason.rstrip(".").lower())
        worth_time += "; ".join(reasons[:3]) + "." if reasons else "it is the strongest merchant-backed signal that is not yet in the outreach loop."
        commercial = _prospect_sales_signals(row, row)
        return {
            "signal_id": int(row.get("signal_id") or 0),
            "label": label,
            "merchant_domain": merchant_domain,
            "distress_hypothesis": (
                f"Likely {processor} pressure around {distress}."
                if processor not in {"unknown", ""} and distress not in {"unknown", ""}
                else f"Likely {distress} worth operator review."
            ),
            "evidence": f"Source: {source}. {evidence}".strip(),
            "contact_path": "No trusted contact path is on file yet. This is still an earlier-stage candidate.",
            "recommended_play": recommended_action,
            "next_move": next_move,
            "worth_time": worth_time,
            **commercial,
        }
    opportunity_id = int(row.get("opportunity_id") or 0)
    if not opportunity_id:
        return None
    lead = _assemble_lead_context(opportunity_id=opportunity_id)
    fit = show_opportunity_fit_command(opportunity_id=opportunity_id)
    if not lead or fit.get("error"):
        return None
    action = lead.get("recommended_operator_action") or operator_action_for_distress(fit.get("distress_type"))
    commercial = _prospect_sales_signals(lead, fit)
    return {
        "opportunity_id": opportunity_id,
        "label": _lead_label(lead),
        "merchant_domain": lead.get("merchant_domain") or fit.get("merchant_domain") or "",
        "distress_hypothesis": _prospect_hypothesis_line(lead, fit),
        "evidence": fit.get("why_now") or lead.get("signal_summary") or lead.get("queue_eligibility_reason") or "",
        "contact_path": _prospect_contact_line(lead, fit),
        "recommended_play": _operator_action_label(action),
        "next_move": _lead_recommendation(lead),
        "worth_time": _prospect_priority_reason(lead, fit),
        **commercial,
    }


def _best_ready_action(heartbeat: dict, dossiers: list[dict]) -> dict:
    if heartbeat.get("live_thread_present") and str(heartbeat.get("live_thread_status") or "").strip().lower() not in {"", "none"}:
        return {
            "label": heartbeat.get("live_thread_merchant_domain") or "live thread",
            "move": heartbeat.get("live_thread_next_step") or "Monitor the live thread closely.",
            "why": "This is the only live merchant conversation in the loop right now.",
        }
    if dossiers:
        top = dossiers[0]
        return {
            "label": top.get("label") or top.get("merchant_domain") or "best prospect",
            "move": top.get("next_move") or "Work the strongest prospect first.",
            "why": top.get("worth_time") or "It is the best new merchant case Meridian surfaced overnight.",
        }
    return {
        "label": "none",
        "move": "No concrete action is ready yet. Meridian needs to surface one real merchant move.",
        "why": "The overnight loop produced activity, but not a decision-ready case.",
    }


def _target_slate_label(targets: list[dict]) -> str:
    count = len(targets)
    if count <= 0:
        return "No target slate"
    if count == 1:
        return "Target Slate"
    return f"Target Slate ({count})"


def _suppression_section() -> dict:
    recent = show_recent_suppressed_leads_command(limit=2)
    cases = list(recent.get("cases") or [])
    lines = []
    for case in cases[:2]:
        reason = str(case.get("disqualifier_reason") or "unknown").replace("_", " ")
        preview = (case.get("content_preview") or "").strip()
        stage = case.get("stage") or "filter"
        snippet = preview[:90] + ("..." if len(preview) > 90 else "") if preview else "no content preview"
        lines.append(f"{stage}: {reason} — {snippet}")
    return {
        "count": int(recent.get("count") or 0),
        "lines": lines,
    }


def _render_daily_sections(sections: list[tuple[str, list[str]]]) -> list[str]:
    lines: list[str] = []
    for title, body_lines in sections:
        if not body_lines:
            continue
        lines.append("")
        lines.append(f"{title}:")
        for line in body_lines:
            if not line:
                continue
            lines.append(f"- {line}")
    return lines


def _coerce_briefing_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _is_low_value_scout_case(case: dict) -> bool:
    reason = str(case.get("reason") or "").strip().lower()
    merchant_domain = str(case.get("merchant_domain") or "").strip().lower()
    why = str(case.get("why") or "").strip().lower()
    content_preview = str(case.get("content_preview") or "").strip().lower()

    weak_reasons = {
        "commercial_readiness_too_low",
        "consumer_noise",
        "identity_weak",
        "icp_fit_too_low",
        "domain_unpromotable",
        "no_contact_path",
        "historical_or_hypothetical",
    }
    weak_domain_markers = (
        "learn.microsoft.com",
        "support.google.com",
        "w3.org",
        "experian.com",
        "coursera",
        "shein",
        "microsoft",
    )
    weak_domain_prefixes = (
        "send.",
        "click.",
        "mail.",
        "email.",
        "news.",
    )
    weak_why_markers = (
        "identity is too weak",
        "too weak even for operator review",
        "still looks interesting rather than commercially ready",
        "consumer noise",
        "historical or hypothetical",
        "marketing mail",
    )
    weak_content_markers = (
        "newsletter",
        "unsubscribe",
        "the gaming channel i'd start from zero today",
        "subject:",
        "sender:",
    )

    return (
        reason in weak_reasons
        or any(merchant_domain.startswith(prefix) for prefix in weak_domain_prefixes)
        or any(marker in merchant_domain for marker in weak_domain_markers)
        or any(marker in why for marker in weak_why_markers)
        or any(marker in content_preview for marker in weak_content_markers)
    )


def _operator_useful_scout_cases(cases: list[dict], *, limit: int = 2) -> list[dict]:
    useful = [case for case in (cases or []) if case and not _is_low_value_scout_case(case)]
    return useful[:limit]


def _is_low_value_dossier(dossier: dict) -> bool:
    blob = " ".join(
        str(dossier.get(key) or "").strip().lower()
        for key in ("label", "merchant_domain", "distress_hypothesis", "evidence", "contact_path", "recommended_play", "next_move", "worth_time")
    )
    noisy_markers = (
        "pre-approved",
        "credit warrior",
        "coupon",
        "newsletter",
        "weekly ad",
        "special offer",
        "unsubscribe",
    )
    return any(marker in blob for marker in noisy_markers)


def get_daily_operator_payload(heartbeat: dict | None = None) -> dict:
    try:
        opportunities = _get_opportunity_counts()
    except Exception:
        opportunities = {"created_24h": 0, "pending_review": 0}
    heartbeat = heartbeat or get_value_heartbeat(refresh=False)
    overnight_status = str(heartbeat.get("overnight_contract_status") or "unknown").replace("_", " ")
    raw_live_thread_present = bool(heartbeat.get("live_thread_present"))
    live_thread_domain = heartbeat.get("live_thread_merchant_domain") or ""
    live_thread_status = str(heartbeat.get("live_thread_status") or "none").replace("_", " ")
    live_thread_status_label = str(heartbeat.get("live_thread_status_label") or live_thread_status).strip()
    live_thread_present = raw_live_thread_present and live_thread_status != "none"
    new_prospects = _coerce_briefing_list(heartbeat.get("daily_target_slate") or heartbeat.get("overnight_best_new_prospects") or [])
    outreach_worthy = _coerce_briefing_list(heartbeat.get("daily_outreach_worthy_targets") or [])
    candidates_considered = int(heartbeat.get("daily_candidates_considered", 0) or 0)
    dossiers = [d for d in (_build_prospect_dossier(row) for row in new_prospects[:3]) if d]
    dossiers.sort(
        key=lambda row: (
            int(row.get("commercial_readiness_score") or 0),
            int(row.get("urgency_score") or 0),
            int(row.get("buying_authority_score") or 0),
            int(row.get("cash_impact_score") or 0),
        ),
        reverse=True,
    )
    dossiers = [dossier for dossier in dossiers if not _is_low_value_dossier(dossier)]
    queue_sharpened = int(heartbeat.get("overnight_queue_sharpened_24h", 0) or 0)
    action_ready = int(heartbeat.get("overnight_action_ready_count", 0) or 0)
    top_suppression_reason = heartbeat.get("overnight_top_suppression_reason") or ""
    scout_report = _coerce_briefing_list(heartbeat.get("prospect_scout_report") or [])
    useful_scout_cases = _operator_useful_scout_cases(scout_report, limit=2)
    best_action = _best_ready_action(heartbeat, dossiers)
    suppression = _suppression_section()
    security_note = str(heartbeat.get("security_guidance") or "").strip()
    security_headline = str(heartbeat.get("security_guardrail_headline") or "").strip()
    security_guardrail_active = bool(heartbeat.get("autonomy_guardrail_active"))
    drafts_waiting = int(heartbeat.get("outreach_awaiting_approval", 0) or 0)
    follow_ups_due = int(heartbeat.get("follow_ups_due", 0) or 0)
    replies_open = int(heartbeat.get("reply_review_needed", 0) or 0)
    sendable_now = int(heartbeat.get("reachable_contact_leads", heartbeat.get("send_eligible_leads", 0)) or 0)
    contact_blocked = int(heartbeat.get("contact_blocked_opportunities", 0) or 0)
    approved_to_send = int(heartbeat.get("outreach_ready_to_send", 0) or 0)
    sends_24h = int(heartbeat.get("channel_sends_24h", 0) or 0)
    wins_24h = int(heartbeat.get("opportunities_converted", 0) or 0)

    if overnight_status == "healthy":
        summary = "Meridian improved the next morning revenue position overnight."
    elif overnight_status == "partial":
        summary = "Meridian moved the loop forward overnight, but the morning position is still mixed."
    else:
        summary = "Overnight output was thin; the loop needs a sharper morning intervention."

    if live_thread_present:
        matters = (
            f"Overnight status is {overnight_status}. "
            f"The real live case is {live_thread_domain or 'the active thread'}. It has {live_thread_status_label}. "
            f"Behind it, there are {_daily_queue_summary(pending_review=opportunities['pending_review'], drafts_waiting=drafts_waiting, replies_open=replies_open, follow_ups_due=follow_ups_due)}. "
            f"Only {candidates_considered} candidate(s) were considered overnight, "
            f"{len(new_prospects)} survived to the slate, and {len(outreach_worthy)} were outreach-worthy."
        )
    else:
        matters = (
            f"Overnight status is {overnight_status}, and there is no live merchant thread right now. "
            f"{candidates_considered} candidates were considered overnight, {len(new_prospects)} made the slate, and {len(outreach_worthy)} look outreach-worthy. "
            f"The queue is carrying {_daily_queue_summary(pending_review=opportunities['pending_review'], drafts_waiting=drafts_waiting, replies_open=replies_open, follow_ups_due=follow_ups_due)}."
        )
    if sendable_now > 0 or contact_blocked > 0:
        matters = f"{matters} Right now there are {sendable_now} sendable lead(s) and {contact_blocked} contact-blocked lead(s)."
    if live_thread_present and heartbeat.get("live_thread_next_step"):
        if drafts_waiting > 0:
            recommendation = f"Handle {live_thread_domain or 'the live thread'} first, then review the strongest pending draft."
        elif follow_ups_due > 0:
            recommendation = f"Handle {live_thread_domain or 'the live thread'} first, then clear the strongest overdue follow-up."
        else:
            recommendation = f"Handle {live_thread_domain or 'the live thread'} first and keep broad queue work secondary."
    elif sendable_now > 0:
        recommendation = "Start with the sendable queue and move the strongest verified-contact case into a real draft or send."
    elif contact_blocked > 0:
        recommendation = "No case is sendable yet. Run contact acquisition on the strongest blocked merchant before doing more queue review."
    elif dossiers:
        best = dossiers[0]
        recommendation = (
            f"Start with {best.get('label') or best.get('merchant_domain') or 'the best prospect'} "
            f"and {best.get('next_move') or 'work the case now.'}"
        )
    else:
        recommendation = "No strong case is resolved yet. Push the queue until one concrete merchant move is ready."

    if top_suppression_reason:
        matters = f"{matters} The main suppression reason overnight was {top_suppression_reason.replace('_', ' ')}."
    if security_guardrail_active and security_note:
        matters = f"{matters} Safety note: {security_note}"
    sections: list[tuple[str, list[str]]] = []
    sections.append(
        (
            "Funnel Truth",
            [
                f"Sendable leads now: {sendable_now}.",
                f"Contact-blocked leads now: {contact_blocked}.",
                f"Approved drafts ready to send: {approved_to_send}.",
                f"Outreach sent in the last 24h: {sends_24h}.",
                f"Open merchant replies: {replies_open}.",
                f"Recorded conversions: {wins_24h}.",
            ],
        )
    )
    sections.append(
        (
            "Revenue Surfaces",
            [
                f"Sendable queue: {_count_phrase(sendable_now, 'lead ready for a real send', 'leads ready for a real send')}.",
                f"Contact-blocked queue: {_count_phrase(contact_blocked, 'lead blocked on contact acquisition', 'leads blocked on contact acquisition')}.",
            ],
        )
    )
    if live_thread_present:
        sections.append(
            (
                "Top Live Case",
                [
                    f"{live_thread_domain or 'unknown'} has {live_thread_status_label}.",
                    "This is the closest case to revenue right now, so it should outrank weak queue exploration.",
                ],
            )
        )
    if dossiers:
        dossier_lines = []
        for dossier in dossiers[:3]:
            matching_target = next(
                (
                    target
                    for target in new_prospects
                    if str(target.get("merchant_domain") or "").strip().lower()
                    == str(dossier.get("merchant_domain") or "").strip().lower()
                ),
                {},
            )
            dossier_lines.extend(
                [
                    f"{dossier.get('label')}: {dossier.get('distress_hypothesis')}",
                    f"Why now: {dossier.get('evidence') or 'No evidence note captured yet.'}",
                    dossier.get("contact_path") or "No trusted contact path is on file yet.",
                    (
                        "Stage: ready for outreach now."
                        if str(matching_target.get("target_stage") or "") == "outreach_worthy"
                        else "Stage: review fit before outreach."
                    ),
                    dossier.get("urgency_summary") or "Urgency is still unclear.",
                    dossier.get("buying_authority_summary") or "Buying authority is still unclear.",
                    f"Cash-impact hypothesis: {dossier.get('cash_impact_hypothesis') or 'The avoided downside is not clear enough yet.'}",
                    f"Play: {dossier.get('recommended_play') or 'clarify distress'}. Next move: {dossier.get('next_move') or 'review it now.'}",
                    dossier.get("worth_time") or "Worth your time because it is the strongest new prospect overnight.",
                ]
            )
        sections.append((_target_slate_label(new_prospects), dossier_lines))
    elif useful_scout_cases:
        scout_lines = []
        for case in useful_scout_cases:
            label = case.get("merchant_domain") or case.get("merchant_name") or f"signal {case.get('signal_id') or ''}".strip()
            scout_lines.append(f"{label}: {case.get('why') or case.get('reason') or 'unknown'}")
            if case.get("content_preview"):
                scout_lines.append(f"Evidence: {case.get('content_preview')}")
        sections.append(("Prospect Scout", scout_lines))
    sections.append(
        (
            "What I Need From You",
            [
                f"{best_action.get('label')}: {best_action.get('move')}",
                (
                    "After that, move to the strongest approval-ready draft."
                    if live_thread_present and drafts_waiting > 0
                    else best_action.get("why") or "This is the best current revenue move."
                ),
            ],
        )
    )
    suppression_lines = []
    if suppression.get("count"):
        suppression_lines.append(
            f"{suppression.get('count')} lead(s) were suppressed recently so Meridian does not waste your morning on noise."
        )
        suppression_lines.extend(suppression.get("lines") or [])
    elif top_suppression_reason:
        suppression_lines.append(f"Main suppression reason: {top_suppression_reason.replace('_', ' ')}.")
    elif scout_report and not useful_scout_cases:
        suppression_lines.append("The scout only surfaced weak or noisy cases overnight, so I kept them out of the main brief.")
    else:
        suppression_lines.append("No meaningful overnight suppression examples were recorded.")
    sections.append(("What Was Suppressed", suppression_lines))
    sections.append(
        (
            "Biggest Bottleneck",
            [
                f"{str(heartbeat.get('primary_bottleneck') or 'unknown').replace('_', ' ')}.",
                (
                    "No merchant is truly sendable yet, so contact acquisition matters more than generic review volume."
                    if str(heartbeat.get("primary_bottleneck") or "") == "contact_acquisition"
                    else
                    f"The live {live_thread_domain or 'merchant'} thread is waiting on judgment before the rest of the queue matters."
                    if live_thread_present and str(heartbeat.get("primary_bottleneck") or "") == "reply_review"
                    else heartbeat.get("next_revenue_move") or "Surface one concrete next revenue move."
                ),
            ],
        )
    )
    if security_guardrail_active and security_note:
        sections.append(("Autonomy Guardrail", [security_note, security_headline] if security_headline else [security_note]))
    payload = _finalize_payload(
        "daily_briefing",
        {
            "summary": summary,
            "matters": matters,
            "recommendation": recommendation,
            "sections": sections,
            "telemetry": {
                "overnight_status": overnight_status,
                "live_thread": live_thread_domain or "none",
                "candidates_considered": candidates_considered,
                "new_real_prospects": len(new_prospects),
                "outreach_worthy": len(outreach_worthy),
                "queue_sharpened": queue_sharpened,
                "action_ready": action_ready,
                "sendable_now": sendable_now,
                "contact_blocked": contact_blocked,
                "approved_to_send": approved_to_send,
                "sends_24h": sends_24h,
                "wins_24h": wins_24h,
                "opportunities_created_24h": opportunities["created_24h"],
                "pending_review": opportunities["pending_review"],
                "reply_review_needed": int(heartbeat.get("reply_review_needed", 0) or 0),
                "follow_ups_due": int(heartbeat.get("follow_ups_due", 0) or 0),
                "primary_bottleneck": str(heartbeat.get("primary_bottleneck") or "unknown").replace("_", " "),
            },
            "snapshot": {
                "overnight_status": overnight_status,
                "live_thread": live_thread_domain or "none",
                "candidates_considered": candidates_considered,
                "new_real_prospects": len(new_prospects),
                "outreach_worthy": len(outreach_worthy),
                "queue_sharpened": queue_sharpened,
                "action_ready": action_ready,
                "sendable_now": sendable_now,
                "contact_blocked": contact_blocked,
                "approved_to_send": approved_to_send,
                "sends_24h": sends_24h,
                "wins_24h": wins_24h,
                "opportunities_created_24h": opportunities["created_24h"],
                "pending_review": opportunities["pending_review"],
                "reply_review_needed": int(heartbeat.get("reply_review_needed", 0) or 0),
                "follow_ups_due": int(heartbeat.get("follow_ups_due", 0) or 0),
            },
            "focus": {"type": "daily_briefing", "label": "daily briefing"},
        },
    )
    if payload.get("what_changed", "").startswith("No material change"):
        if live_thread_present:
            payload["what_changed"] = (
                f"The live case is still {live_thread_domain or 'the active thread'}, and it still has {live_thread_status_label}."
            )
        elif opportunities["pending_review"] > 0:
            payload["what_changed"] = (
                f"The queue is still carrying {_count_phrase(opportunities['pending_review'], 'case waiting for review', 'cases waiting for review')}."
            )
    return payload


def build_system_status_briefing() -> str:
    return render_operator_briefing(get_system_status_payload())


def build_signal_activity_briefing() -> str:
    return render_operator_briefing(get_signal_activity_payload())


def build_pattern_summary_briefing() -> str:
    return render_operator_briefing(get_pattern_summary_payload())


def build_actionable_patterns_briefing() -> str:
    return render_operator_briefing(get_actionable_patterns_payload())


def build_merchant_profiles_briefing() -> str:
    return render_operator_briefing(get_merchant_profiles_payload())


def build_conversion_intelligence_briefing() -> str:
    return render_operator_briefing(get_conversion_intelligence_payload())


def build_best_conversion_briefing() -> str:
    return render_operator_briefing(get_best_conversion_payload())


def build_focus_recommendation_briefing() -> str:
    return render_operator_briefing(get_focus_recommendation_payload())


def build_opportunity_summary_briefing() -> str:
    return render_operator_briefing(get_opportunity_summary_payload())


def build_model_performance_briefing() -> str:
    return render_operator_briefing(get_model_performance_payload())


def build_daily_operator_briefing(heartbeat: dict | None = None) -> str:
    return render_operator_briefing(get_daily_operator_payload(heartbeat=heartbeat))


def render_operator_briefing(payload: dict, what_changed_override: str | None = None) -> str:
    telemetry = ", ".join(f"{key}={value}" for key, value in payload.get("telemetry", {}).items())
    lines = [
        payload.get("summary", ""),
        f"What changed: {what_changed_override or payload.get('what_changed', 'No material change since the last check.')}",
        f"Why it matters: {payload.get('matters', '')}",
        f"Recommendation: {payload.get('recommendation', '')}",
    ]
    lines.extend(_render_daily_sections(payload.get("sections") or []))
    if telemetry and _SHOW_OPERATOR_TELEMETRY:
        lines.append(f"Telemetry: {telemetry}")
    return "\n".join(lines)


def send_daily_operator_briefing() -> dict:
    now_local = datetime.now(ZoneInfo(_OPERATOR_TIMEZONE))
    if not (_DAILY_BRIEFING_START_HOUR <= now_local.hour <= _DAILY_BRIEFING_END_HOUR):
        return {
            "status": "skipped",
            "reason": "outside_daily_briefing_window",
            "local_hour": now_local.hour,
            "timezone": _OPERATOR_TIMEZONE,
        }
    today_key = now_local.date().isoformat()
    if _redis.get("agent_flux:operator_updates:last_daily_briefing") == today_key:
        return {"status": "skipped", "reason": "already_sent_today"}
    heartbeat = get_value_heartbeat(refresh=True)
    sent = send_operator_alert(build_daily_operator_briefing(heartbeat=heartbeat))
    if sent:
        _redis.set("agent_flux:operator_updates:last_daily_briefing", today_key)
        _redis.set("agent_flux:operator_updates:last_daily_briefing_sent_at", str(time.time()))
    return {"status": "sent" if sent else "error"}


def send_proactive_operator_updates(dry_run: bool = False) -> dict:
    candidates = []
    legacy_would_send = []
    suppression_reasons = {}

    # ── 1. Distress pattern ──────────────────────────────────────────────────
    top_pattern = _top_rising_pattern()
    if top_pattern:
        legacy_sig = f"{top_pattern.get('pattern_key')}:{top_pattern.get('velocity_24h')}:{top_pattern.get('status')}"
        legacy_changed = _redis.get("agent_flux:operator_updates:last_distress_pattern_signature") != legacy_sig
        if legacy_changed:
            legacy_would_send.append("distress_pattern")

        normalized_distress_type = str(top_pattern.get("distress_type") or "unknown").strip().lower()
        if normalized_distress_type in {"", "unknown"}:
            suppression_reasons["distress_pattern"] = "unclassified_pattern"
            top_pattern = None

    if top_pattern:
        verified_contacts = _count_verified_contacts_for_pattern(top_pattern)
        top_pattern["verified_contact_count"] = verified_contacts
        material = _pattern_change_is_material(top_pattern)

        if material:
            target_lines = _pattern_target_summary(top_pattern, limit=3)
            if target_lines:
                alert_text = "\n".join(
                    [
                        top_pattern.get("headline") or "A distress pattern is building.",
                        (
                            f"Why it matters: {_pattern_interpretation_line(top_pattern)} {_pattern_actionability_text(top_pattern)} "
                            f"{_pattern_strategy_line(top_pattern)} "
                            f"{_pattern_conversion_line(top_pattern)}"
                        ),
                        f"Why it passed: {_pattern_pass_reason(top_pattern)}",
                        "Best merchants now:\n- " + "\n- ".join(target_lines),
                        f"Recommendation: {_pattern_recommendation(top_pattern)}",
                    ]
                )
                candidates.append({
                    "source": "distress_pattern",
                    "text": alert_text,
                    "rank_score": _compute_rank_score(top_pattern, "distress_pattern"),
                    "data": top_pattern,
                    "signature": legacy_sig,
                    "bypass_ranking": False,
                })
            else:
                suppression_reasons["distress_pattern"] = "no_merchant_targets_resolved"
        elif legacy_changed:
            suppression_reasons["distress_pattern"] = "below_materiality_threshold"

    # ── 2. Gmail distress thread ─────────────────────────────────────────────
    latest_thread, gmail_screen_summary = _latest_gmail_distress_thread_screened()
    if latest_thread:
        signature = f"{latest_thread.get('thread_id')}:{latest_thread.get('triaged_at')}"
        legacy_changed = _redis.get("agent_flux:operator_updates:last_gmail_distress_signature") != signature
        if legacy_changed:
            legacy_would_send.append("gmail_distress")

        if legacy_changed and _source_cooldown_ok("gmail_distress"):
            alert_text = "\n".join(
                [
                    f"New merchant-distress thread: {latest_thread.get('subject') or latest_thread.get('thread_id')}.",
                    "Why it matters: this is the highest-signal inbox event and could turn into a qualified lead quickly.",
                    f"Why it passed: {gmail_screen_summary or 'it passed the merchant-distress screen.'}",
                    "Recommendation: review it before the next approval window.",
                ]
            )
            candidates.append({
                "source": "gmail_distress",
                "text": alert_text,
                "rank_score": _compute_rank_score(latest_thread, "gmail_distress"),
                "data": latest_thread,
                "signature": signature,
                "bypass_ranking": False,
            })
        elif legacy_changed:
            suppression_reasons["gmail_distress"] = "source_cooldown"
    elif gmail_screen_summary:
        suppression_reasons["gmail_distress"] = gmail_screen_summary

    # ── 3. Merchant profile ──────────────────────────────────────────────────
    top_profile = _top_high_value_merchant_profile()
    if top_profile:
        urgency_bucket = int(int(top_profile.get("urgency_score", 0) or 0) // 10)
        signature = f"{top_profile.get('merchant_id')}:{top_profile.get('opportunity_id')}:{urgency_bucket}"
        legacy_sig = f"{top_profile.get('merchant_id')}:{top_profile.get('opportunity_id')}:{top_profile.get('urgency_score')}"
        legacy_changed = _redis.get("agent_flux:operator_updates:last_merchant_profile_signature") != legacy_sig
        sig_changed = _redis.get("agent_flux:operator_updates:last_merchant_profile_signature") != signature
        if legacy_changed:
            legacy_would_send.append("merchant_profile")

        if sig_changed and _source_cooldown_ok("merchant_profile"):
            label = _merchant_profile_label(top_profile)
            alert_text = "\n".join(
                [
                    f"High-value merchant profile: {label} is surfacing as a stronger lead.",
                    f"Why it matters: {_merchant_doctrine_summary(top_profile)}",
                    f"Why it passed: {_merchant_profile_pass_reason(top_profile)}",
                    f"Recommendation: {_merchant_profile_recommendation(top_profile)}",
                ]
            )
            candidates.append({
                "source": "merchant_profile",
                "text": alert_text,
                "rank_score": _compute_rank_score(top_profile, "merchant_profile"),
                "data": top_profile,
                "signature": signature,
                "bypass_ranking": False,
            })
        elif legacy_changed:
            reason = "urgency_bucket_unchanged" if not sig_changed else "source_cooldown"
            suppression_reasons["merchant_profile"] = reason

    # ── 4. Opportunity ───────────────────────────────────────────────────────
    latest_opportunity = _latest_meaningful_opportunity()
    if latest_opportunity:
        signature = f"{latest_opportunity.get('id')}:{latest_opportunity.get('status')}"
        legacy_changed = _redis.get("agent_flux:operator_updates:last_opportunity_signature") != signature
        if legacy_changed:
            legacy_would_send.append("opportunity")

        if legacy_changed and _source_cooldown_ok("opportunity"):
            alert_text = "\n".join(
                [
                    f"New opportunity ready: {latest_opportunity.get('merchant_domain') or 'unknown'} is now in {latest_opportunity.get('status')}.",
                    "Why it matters: this is the clearest path from current signal flow to revenue.",
                    f"Why it passed: {_opportunity_pass_reason(latest_opportunity)}",
                    "Recommendation: review it while the context is still fresh.",
                ]
            )
            candidates.append({
                "source": "opportunity",
                "text": alert_text,
                "rank_score": _compute_rank_score(latest_opportunity, "opportunity"),
                "data": latest_opportunity,
                "signature": signature,
                "bypass_ranking": False,
            })
        elif legacy_changed:
            suppression_reasons["opportunity"] = "source_cooldown"

    # ── 5. Conversion signal ─────────────────────────────────────────────────
    conversion_signal = _top_conversion_signal()
    if conversion_signal:
        signature = f"{conversion_signal.get('processor')}:{conversion_signal.get('distress_type')}:{conversion_signal.get('wins')}"
        legacy_sig = f"{conversion_signal.get('processor')}:{conversion_signal.get('distress_type')}:{conversion_signal.get('wins')}:{conversion_signal.get('pending')}"
        legacy_changed = _redis.get("agent_flux:operator_updates:last_conversion_signal_signature") != legacy_sig
        sig_changed = _redis.get("agent_flux:operator_updates:last_conversion_signal_signature") != signature
        if legacy_changed:
            legacy_would_send.append("conversion_intelligence")

        if sig_changed and _source_cooldown_ok("conversion_intelligence"):
            label = _conversion_pattern_label(conversion_signal)
            alert_text = "\n".join(
                [
                    f"Conversion intelligence update: {label} is the strongest tracked pattern right now.",
                    (
                        "Why it matters: "
                        + (
                            f"It already has {conversion_signal.get('wins', 0)} win(s) on the board. {_conversion_learning_line(conversion_signal)}"
                            if int(conversion_signal.get("wins", 0) or 0) > 0
                            else f"It has {conversion_signal.get('pending', 0)} open opportunities, but no wins yet. {_conversion_learning_line(conversion_signal)}"
                        )
                    ),
                    f"Why it passed: {_conversion_pass_reason(conversion_signal)}",
                    (
                        f"Recommendation: use {label} as a tiebreaker when several leads feel equally urgent."
                        if int(conversion_signal.get("wins", 0) or 0) > 0
                        else f"Recommendation: keep tracking {label}, but do not over-weight it until a win lands."
                    ),
                ]
            )
            candidates.append({
                "source": "conversion_intelligence",
                "text": alert_text,
                "rank_score": _compute_rank_score(conversion_signal, "conversion_intelligence"),
                "data": conversion_signal,
                "signature": signature,
                "bypass_ranking": False,
            })
        elif legacy_changed:
            reason = "wins_unchanged" if not sig_changed else "source_cooldown"
            suppression_reasons["conversion_intelligence"] = reason

    # ── 6. Health (bypass ranking) ───────────────────────────────────────────
    health = _overall_health_label()
    previous_health = _redis.get("agent_flux:operator_updates:last_health_state")
    if health == "attention" and previous_health != "attention":
        health_sig = f"attention:{int(time.time()) // _BYPASS_ANTI_FLAP_SECONDS}"
        if _bypass_signature_ok("health", health_sig):
            legacy_would_send.append("health")
            alert_text = "\n".join(
                [
                    "System health just slipped out of the normal state.",
                    "Why it matters: the operator loop is still online, but a backend component needs attention.",
                    "Recommendation: check status before approving anything new.",
                ]
            )
            candidates.append({
                "source": "health",
                "text": alert_text,
                "rank_score": 0,
                "data": {"health": health},
                "signature": health_sig,
                "bypass_ranking": True,
            })
        else:
            suppression_reasons["health"] = "anti_flap_suppressed"
    elif previous_health != health:
        if not dry_run:
            _redis.set("agent_flux:operator_updates:last_health_state", health)

    # ── 7. Reasoning regression (bypass ranking) ─────────────────────────────
    regression = _current_reasoning_regression()
    if regression:
        reg_signature = hashlib.sha256(json.dumps(regression, sort_keys=True).encode("utf-8")).hexdigest()
        legacy_changed = _redis.get("agent_flux:operator_updates:last_reasoning_regression") != reg_signature
        if legacy_changed:
            legacy_would_send.append("reasoning_regression")
        if legacy_changed and _bypass_signature_ok("reasoning_regression", reg_signature):
            alert_text = "\n".join(
                [
                    regression["headline"],
                    "Why it matters: model quality or fallback behavior may be drifting away from the narrow routing assumptions.",
                    "Recommendation: inspect reasoning metrics before broadening model use.",
                ]
            )
            candidates.append({
                "source": "reasoning_regression",
                "text": alert_text,
                "rank_score": 0,
                "data": regression,
                "signature": reg_signature,
                "bypass_ranking": True,
            })
        elif legacy_changed:
            suppression_reasons["reasoning_regression"] = "anti_flap_suppressed"

    # ── Phase B: Rank ────────────────────────────────────────────────────────
    bypass_candidates = [c for c in candidates if c.get("bypass_ranking")]
    _DTYPE_TIEBREAK = {"account_terminated": 25, "account_frozen": 20, "chargeback_issue": 10,
                       "verification_review": 5, "processor_switch_intent": 0}
    _SRC_TIEBREAK = {"opportunity": 20, "gmail_distress": 15, "distress_pattern": 10,
                     "merchant_profile": 5, "conversion_intelligence": 0}

    def _rank_key(c):
        d = c.get("data") or {}
        return (
            c["rank_score"],
            int(d.get("verified_contact_count", 0) or 0),
            _DTYPE_TIEBREAK.get(d.get("distress_type", ""), 5),
            _SRC_TIEBREAK.get(c.get("source", ""), 0),
        )

    rankable = sorted(
        [c for c in candidates if not c.get("bypass_ranking")],
        key=_rank_key,
        reverse=True,
    )

    policy_would_send = [c["source"] for c in bypass_candidates]
    budget = max(0, _MAX_ALERTS_PER_CYCLE - len(bypass_candidates))
    ranked_to_send = rankable[:budget]
    ranked_suppressed = rankable[budget:]
    policy_would_send.extend(c["source"] for c in ranked_to_send)

    for c in ranked_suppressed:
        suppression_reasons[c["source"]] = f"ranked_out (score={c['rank_score']:.1f}, position>{budget})"

    suppressed_by_policy = [s for s in legacy_would_send if s not in policy_would_send]

    # ── Dry-run: return analysis without side effects ────────────────────────
    if dry_run:
        return {
            "status": "dry_run",
            "all_candidates": [
                {"source": c["source"], "rank_score": c["rank_score"], "bypass": c.get("bypass_ranking", False), "text_preview": c["text"][:120]}
                for c in candidates
            ],
            "would_send": policy_would_send,
            "would_suppress": [c["source"] for c in ranked_suppressed],
            "suppression_reasons": suppression_reasons,
            "legacy_would_send": legacy_would_send,
            "policy_would_send": policy_would_send,
            "suppressed_by_policy": suppressed_by_policy,
        }

    # ── Phase C: Send ────────────────────────────────────────────────────────
    sent = []

    for c in bypass_candidates:
        if send_operator_alert(c["text"]):
            if c["source"] == "health":
                _redis.set("agent_flux:operator_updates:last_health_state", health)
                _record_bypass_signature("health", c["signature"])
            elif c["source"] == "reasoning_regression":
                _redis.set("agent_flux:operator_updates:last_reasoning_regression", c["signature"])
                _record_bypass_signature("reasoning_regression", c["signature"])
            sent.append(c["source"])

    for c in ranked_to_send:
        if send_operator_alert(c["text"]):
            _record_source_send(c["source"])
            if c["source"] == "distress_pattern":
                _save_pattern_snapshot(c["data"])
                _redis.set("agent_flux:operator_updates:last_distress_pattern_signature", c["signature"])
            elif c["source"] == "gmail_distress":
                _redis.set("agent_flux:operator_updates:last_gmail_distress_signature", c["signature"])
            elif c["source"] == "merchant_profile":
                _redis.set("agent_flux:operator_updates:last_merchant_profile_signature", c["signature"])
            elif c["source"] == "opportunity":
                _redis.set("agent_flux:operator_updates:last_opportunity_signature", c["signature"])
            elif c["source"] == "conversion_intelligence":
                _redis.set("agent_flux:operator_updates:last_conversion_signal_signature", c["signature"])
            sent.append(c["source"])

    return {"status": "ok", "updates_sent": sent, "suppressed": list(suppression_reasons.keys())}


def notify_reasoning_evaluation_change(evaluation_result: dict) -> dict:
    summaries = evaluation_result.get("strategy_summaries", {})
    compact = {
        "sample_count": evaluation_result.get("sample_count", 0),
        "tier1_useful_refinements": summaries.get("tier1_only", {}).get("useful_refinement_count", 0),
        "cascade_useful_refinements": summaries.get("cascade", {}).get("useful_refinement_count", 0),
        "cascade_identity_domain_match": summaries.get("cascade", {}).get("merchant_identity_domain_match_rate", 0.0),
    }
    signature = hashlib.sha256(json.dumps(compact, sort_keys=True).encode("utf-8")).hexdigest()
    if _redis.get("agent_flux:operator_updates:last_eval_signature") == signature:
        return {"status": "skipped", "reason": "unchanged"}
    text = "\n".join(
        [
            f"Reasoning evaluation updated on {evaluation_result.get('sample_count', 0)} samples.",
            "Why it matters: this shows whether the current narrow routing is still paying for itself.",
            (
                "Recommendation: keep the current routing narrow."
                if compact["cascade_useful_refinements"] >= compact["tier1_useful_refinements"]
                else "Recommendation: review the current cascade because Tier 1 is carrying more value."
            ),
        ]
    )
    sent = send_operator_alert(text)
    if sent:
        _redis.set("agent_flux:operator_updates:last_eval_signature", signature)
    return {"status": "sent" if sent else "error"}


def _finalize_payload(kind: str, payload: dict) -> dict:
    payload = dict(payload)
    payload["kind"] = kind
    payload["what_changed"] = _compute_change_line(kind, payload.get("snapshot", {}))
    return payload


def _compute_change_line(kind: str, snapshot: dict) -> str:
    key = f"agent_flux:briefing_snapshot:{kind}"
    previous_raw = _redis.get(key)
    previous = json.loads(previous_raw) if previous_raw else {}
    changes = []
    for field, value in snapshot.items():
        previous_value = previous.get(field)
        if previous_value is None:
            continue
        if previous_value == value:
            continue
        label = _natural_label(field)
        if isinstance(value, (int, float)) and isinstance(previous_value, (int, float)):
            delta = value - previous_value
            changes.append(f"{label} {'rose' if delta > 0 else 'fell'} by {abs(delta)}")
        else:
            changes.append(f"{label} changed from {previous_value} to {value}")
        if len(changes) >= 3:
            break
    _redis.setex(key, 86400, json.dumps(snapshot, sort_keys=True))
    if not changes:
        return "No material change since the last check."
    return "; ".join(changes) + "."


def _latest_focus(*, distress_threads: list[dict]) -> dict:
    if distress_threads:
        top = distress_threads[0]
        return {
            "type": "gmail_thread",
            "thread_id": top.get("thread_id"),
            "label": top.get("subject") or top.get("thread_id"),
        }
    opportunities = list_deal_opportunities_command(limit=1).get("deal_opportunities", [])
    if opportunities:
        top = opportunities[0]
        return {
            "type": "opportunity",
            "opportunity_id": top.get("id"),
            "label": top.get("merchant_domain") or str(top.get("id")),
        }
    return {"type": "none", "label": None}


def _latest_meaningful_gmail_distress_thread() -> dict | None:
    rows = list_gmail_merchant_distress_command(limit=5).get("threads", [])
    for row in rows:
        if _gmail_distress_thread_is_actionable(row):
            return row
    return None


def _latest_gmail_distress_thread_screened() -> tuple[dict | None, str]:
    rows = list_gmail_merchant_distress_command(limit=5).get("threads", [])
    first_rejected_reason = ""
    for row in rows:
        actionable, summary = _gmail_distress_thread_screen(row)
        if actionable:
            return row, summary
        if not first_rejected_reason and summary:
            first_rejected_reason = summary
    return None, first_rejected_reason


_GMAIL_DISTRESS_NOISE_RE = re.compile(
    r"\b(send resume|recruitment|apply now|job|hiring|perfect new card|all dressed up|% off|sale)\b",
    re.IGNORECASE,
)


def _gmail_distress_thread_screen(row: dict | None) -> tuple[bool, str]:
    row = dict(row or {})
    reasons: list[str] = []
    confidence = float(row.get("confidence") or 0.0)
    if confidence < 0.85:
        reasons.append("confidence is below 0.85")

    if not row.get("opportunity_eligible"):
        reasons.append("the thread is not marked opportunity-eligible")

    processor = str(row.get("processor") or "unknown").strip().lower()
    distress_signals = [str(item).strip().lower() for item in (row.get("distress_signals") or []) if str(item).strip()]
    metadata = dict(row.get("metadata") or {})
    subject = str(row.get("subject") or "")
    snippet = str(row.get("snippet") or "")
    sender_domain = str(row.get("sender_domain") or "")
    text_blob = " ".join([subject, snippet, sender_domain]).strip()

    if _GMAIL_DISTRESS_NOISE_RE.search(text_blob):
        reasons.append("it looks like job, promo, or inbox noise")
    if metadata.get("human_written") is False:
        reasons.append("it is not a human-written merchant message")
    if processor == "unknown" and (not distress_signals or distress_signals == ["unknown"]):
        reasons.append("processor context is still too weak")

    if reasons:
        return False, "; ".join(reasons)

    pass_reasons: list[str] = []
    distress_type = str(row.get("distress_type") or "unknown").replace("_", " ")
    if processor != "unknown":
        pass_reasons.append(f"processor context is {processor}")
    if distress_type and distress_type != "unknown":
        pass_reasons.append(f"distress reads as {distress_type}")
    if row.get("opportunity_eligible"):
        pass_reasons.append("the thread is opportunity-eligible")
    return True, ", ".join(pass_reasons) or "the thread passed the merchant-distress screen"


def _gmail_distress_thread_is_actionable(row: dict | None) -> bool:
    actionable, _ = _gmail_distress_thread_screen(row)
    return actionable


def _opportunity_pass_reason(opportunity: dict | None) -> str:
    row = dict(opportunity or {})
    opportunity_id = int(row.get("id") or row.get("opportunity_id") or 0)
    if not opportunity_id:
        return "it is the clearest current path from signal to revenue."
    fit = show_opportunity_fit_command(opportunity_id=opportunity_id)
    if fit.get("error"):
        return "it is the clearest current path from signal to revenue."
    if fit.get("outreach_status") in {"sent", "replied", "follow_up_needed"}:
        return "it is already a live merchant thread with a trusted contact path."
    pieces: list[str] = []
    if fit.get("high_conviction_prospect"):
        pieces.append("it is a high-conviction PayFlux prospect")
    if fit.get("queue_eligibility_class") == "outreach_eligible":
        pieces.append("it is outreach-eligible")
    if fit.get("contact_email"):
        pieces.append("a trusted contact path is already on file")
    if fit.get("icp_fit_label") == "strong":
        pieces.append(f"ICP fit is strong ({fit.get('icp_fit_score')})")
    return ", ".join(pieces) or (fit.get("queue_eligibility_reason") or "it is the clearest current path from signal to revenue.")


def _pattern_pass_reason(pattern: dict | None) -> str:
    row = dict(pattern or {})
    pieces: list[str] = []
    status = str(row.get("status") or "watch").replace("_", " ")
    signals = int(row.get("signal_count") or 0)
    merchants = int(row.get("affected_merchant_count") or 0)
    contacts = int(row.get("verified_contact_count") or 0)
    if status and status != "watch":
        pieces.append(f"status is {status}")
    if signals > 0:
        pieces.append(f"{signals} signals are clustering")
    if merchants > 0:
        pieces.append(f"{merchants} merchant(s) are affected")
    if contacts > 0:
        pieces.append(f"{contacts} verified contact path(s) exist")
    return ", ".join(pieces) or "the pattern cleared the materiality threshold."


def _merchant_profile_pass_reason(profile: dict | None) -> str:
    row = dict(profile or {})
    pieces: list[str] = []
    urgency_line = _urgency_summary(row.get("urgency_score"))
    processor = _normalized_processor_label(row.get("processor"))
    distress = _normalized_distress_label(row.get("distress_type"))
    verified_contacts = int(row.get("verified_contact_count") or 0)
    if urgency_line:
        pieces.append(urgency_line)
    if distress != "merchant distress":
        pieces.append(f"the live signal points to {distress}")
    if processor != "an unresolved processor":
        pieces.append(f"processor context points to {processor}")
    else:
        pieces.append("payment-processor context is still unresolved")
    if verified_contacts > 0:
        pieces.append(_count_phrase(verified_contacts, "verified contact path is open", "verified contact paths are open"))
    return ", ".join(pieces) or "the merchant profile is surfacing as the strongest current lead."


def _conversion_pass_reason(signal: dict | None) -> str:
    row = dict(signal or {})
    wins = int(row.get("wins") or 0)
    pending = int(row.get("pending") or 0)
    processor = _normalized_processor_label(row.get("processor"))
    distress = _normalized_distress_label(row.get("distress_type"))
    pieces: list[str] = []
    if wins > 0:
        pieces.append(_count_phrase(wins, "win is already on the board", "wins are already on the board"))
    if pending > 0:
        pieces.append(_count_phrase(pending, "open opportunity is still attached", "open opportunities are still attached"))
    if processor != "an unresolved processor":
        pieces.append(f"processor context points to {processor}")
    else:
        pieces.append("processor context is still unresolved")
    if distress != "merchant distress":
        pieces.append(f"the pattern centers on {distress}")
    return ", ".join(pieces) or "this is the strongest tracked conversion pattern right now."


def _pattern_target_next_move(fit: dict) -> str:
    outreach_status = str(fit.get("outreach_status") or "").strip().lower()
    approval_state = str(fit.get("approval_state") or "").strip().lower()
    if outreach_status in {"sent", "replied", "follow_up_needed"}:
        return "watch reply / follow-up"
    if outreach_status == "draft_ready" and approval_state == "approved":
        return "send approved draft"
    if outreach_status == "draft_ready":
        return "review and approve draft"
    if fit.get("operator_ready"):
        return "draft outreach"
    return "review fit before outreach"


def _pattern_matches_candidate(*, pattern: dict, processor: str, distress_type: str, industry: str) -> bool:
    pattern_processor = str(pattern.get("processor") or "unknown").strip().lower()
    pattern_distress = str(pattern.get("distress_type") or "unknown").strip().lower()
    pattern_industry = str(pattern.get("industry") or "unknown").strip().lower()
    candidate_processor = str(processor or "unknown").strip().lower()
    candidate_distress = str(distress_type or "unknown").strip().lower()
    candidate_industry = str(industry or "unknown").strip().lower()

    if pattern_processor != "unknown" and candidate_processor not in {"", "unknown"} and candidate_processor != pattern_processor:
        return False
    if pattern_distress != "unknown" and candidate_distress != pattern_distress:
        return False
    if pattern_industry not in {"", "unknown"} and candidate_industry not in {"", "unknown"} and candidate_industry != pattern_industry:
        return False
    return True


def _merchant_contact_snapshot(merchant_id: int | None) -> dict:
    if not merchant_id:
        return {"has_contact": False, "contact_email": "", "contact_count": 0, "best_confidence": 0.0}
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (WHERE email IS NOT NULL AND email != '') AS contact_count,
                    MAX(COALESCE(confidence, 0)) FILTER (WHERE email IS NOT NULL AND email != '') AS best_confidence,
                    MAX(email) FILTER (WHERE email IS NOT NULL AND email != '') AS contact_email
                FROM merchant_contacts
                WHERE merchant_id = :merchant_id
                """
            ),
            {"merchant_id": int(merchant_id)},
        ).mappings().first() or {}
    count = int(row.get("contact_count") or 0)
    return {
        "has_contact": count > 0,
        "contact_email": row.get("contact_email") or "",
        "contact_count": count,
        "best_confidence": float(row.get("best_confidence") or 0.0),
    }


def _maybe_seed_pattern_contact_resolution(
    *,
    merchant_id: int | None,
    merchant_domain: str = "",
    distress_type: str = "",
    urgency_score: int = 0,
    queue_quality_score: int = 0,
) -> dict:
    merchant_id = int(merchant_id or 0)
    domain = normalize_merchant_domain(merchant_domain or "")
    normalized_distress = normalize_distress_topic(distress_type)
    if merchant_id <= 0 or not domain or normalized_distress == "unknown":
        return {"attempted": False, "created": 0}
    if queue_quality_score < 50 and urgency_score < 70 and normalized_distress not in {
        "reserve_hold",
        "payouts_delayed",
        "account_frozen",
        "chargeback_issue",
        "verification_review",
    }:
        return {"attempted": False, "created": 0}
    cooldown_key = f"agent_flux:pattern_seed_contact:{merchant_id}:{normalized_distress}"
    if _redis.get(cooldown_key):
        return {"attempted": False, "created": 0}
    _redis.setex(cooldown_key, 12 * 3600, "1")
    try:
        from runtime.intelligence.contact_discovery import discover_contacts_for_merchant
    except ImportError:
        from intelligence.contact_discovery import discover_contacts_for_merchant

    def _run_seed() -> None:
        try:
            discover_contacts_for_merchant(
                merchant_id,
                allow_without_opportunity=True,
                distress_topic=normalized_distress,
                selected_action=operator_action_for_distress(normalized_distress),
            )
        except Exception:
            return

    threading.Thread(target=_run_seed, daemon=True).start()
    return {"attempted": True, "created": 0, "queued": True}


def _pattern_target_summary_from_profiles(pattern: dict | None, *, limit: int, seen: set[str]) -> list[tuple[int, str]]:
    row = dict(pattern or {})
    profiles = list_merchant_profiles_command(limit=100).get("profiles", [])
    ranked: list[tuple[int, str]] = []
    for profile in profiles:
        if not _pattern_matches_candidate(
            pattern=row,
            processor=str(profile.get("processor") or "unknown"),
            distress_type=str(profile.get("distress_type") or "unknown"),
            industry=str(profile.get("industry") or "unknown"),
        ):
            continue
        merchant_status = str(profile.get("merchant_status") or profile.get("status") or "").strip().lower()
        if merchant_status == "candidate":
            continue
        domain = normalize_merchant_domain(profile.get("merchant_domain") or "")
        if not domain:
            continue
        merchant_key = domain.strip().lower()
        if merchant_key in seen:
            continue
        queue_class = str(profile.get("queue_eligibility_class") or "").strip().lower()
        urgency = int(profile.get("urgency_score") or 0)
        queue_quality = int(profile.get("queue_quality_score") or 0)
        if queue_class != ELIGIBILITY_OUTREACH:
            continue
        verified_contacts = int(profile.get("verified_contact_count") or 0)
        if verified_contacts <= 0:
            seeded = _maybe_seed_pattern_contact_resolution(
                merchant_id=profile.get("merchant_id"),
                merchant_domain=domain,
                distress_type=profile.get("distress_type") or "",
                urgency_score=urgency,
                queue_quality_score=queue_quality,
            )
            if seeded.get("attempted"):
                contact = _merchant_contact_snapshot(profile.get("merchant_id"))
                verified_contacts = int(contact.get("contact_count") or 0)
        if verified_contacts <= 0:
            continue
        seen.add(merchant_key)
        merchant = _merchant_profile_label(profile)
        next_move = f"draft {_operator_action_label(profile.get('recommended_operator_action') or operator_action_for_distress(profile.get('distress_type')))} outreach"
        score = queue_quality + min(urgency, 40) + min(verified_contacts * 15, 30)
        ranked.append((score, f"{merchant} — contact yes, next move: {next_move}"))
        if len(ranked) >= max(3, int(limit) * 2):
            break
    return ranked


def _pattern_target_summary_from_signals(pattern: dict | None, *, limit: int, seen: set[str]) -> list[tuple[int, str]]:
    row = dict(pattern or {})
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT s.id,
                       COALESCE(ms.merchant_id, s.merchant_id) AS merchant_id,
                       COALESCE(m.canonical_name, s.merchant_name, '') AS merchant_name,
                       COALESCE(m.domain, '') AS merchant_domain,
                       COALESCE(m.domain_confidence, '') AS domain_confidence,
                       COALESCE(m.status, '') AS merchant_status,
                       COALESCE(m.industry, s.industry, 'unknown') AS industry,
                       COALESCE(s.classification, '') AS classification,
                       COALESCE(s.confidence, 0) AS confidence,
                       s.content,
                       s.detected_at
                FROM signals s
                LEFT JOIN merchant_signals ms ON ms.signal_id = s.id
                LEFT JOIN merchants m ON m.id = COALESCE(ms.merchant_id, s.merchant_id)
                WHERE COALESCE(ms.merchant_id, s.merchant_id) IS NOT NULL
                  AND s.detected_at >= NOW() - INTERVAL '45 days'
                ORDER BY s.detected_at DESC
                LIMIT 500
                """
            )
        ).mappings().all()
    ranked: list[tuple[int, str]] = []
    for signal in rows:
        if str(signal.get("classification") or "").strip().lower() != "merchant_distress":
            continue
        parsed = _parse_signal_content(signal.get("content"))
        sender_domain = str(parsed.get("sender_domain") or "").strip().lower()
        subject = str(parsed.get("subject") or "").strip().lower()
        sender = str(parsed.get("sender") or "").strip().lower()
        if sender_domain and any(
            token in sender_domain
            for token in (
                "ziprecruiter.com",
                "myjobhelper.com",
                "brief.barchart.com",
                "silversea.com",
                "onelink.me",
            )
        ):
            continue
        if any(
            phrase in " ".join(part for part in (subject, sender, signal.get("content") or "") if part).lower()
            for phrase in (
                "job might be right for you",
                "you are cordially invited",
                "spring newsletter",
                "needs a nap",
                "plan the perfect",
            )
        ):
            continue
        parsed_processor = str(parsed.get("processor") or "unknown").strip().lower()
        parsed_distress = str(parsed.get("distress_type") or "unknown").strip().lower()
        signal_industry = str(signal.get("industry") or "unknown").strip().lower()
        if not _pattern_matches_candidate(
            pattern=row,
            processor=parsed_processor,
            distress_type=parsed_distress,
            industry=signal_industry,
        ):
            continue

        merchant_domain = normalize_merchant_domain(parsed.get("merchant_domain_candidate") or signal.get("merchant_domain") or "")
        merchant_name = parsed.get("merchant_name_candidate") or signal.get("merchant_name") or ""
        quality = evaluate_opportunity_queue_quality(
            opportunity={
                "merchant_domain": merchant_domain,
                "processor": parsed_processor or "unknown",
                "distress_topic": parsed_distress or "unknown",
            },
            merchant={
                "canonical_name": merchant_name,
                "domain": merchant_domain,
                "domain_confidence": signal.get("domain_confidence") or "provisional",
                "status": signal.get("merchant_status") or "active",
            },
            signal={
                "content": signal.get("content") or "",
                "merchant_name": merchant_name,
                "merchant_domain": merchant_domain,
            },
        )
        eligibility_class = str(quality.get("eligibility_class") or "")
        quality_score = int(quality.get("quality_score") or 0)
        if eligibility_class != ELIGIBILITY_OUTREACH and quality_score < 55:
            continue
        merchant_key = str(merchant_domain or merchant_name or signal.get("merchant_id") or signal.get("id") or "").strip().lower()
        if not merchant_key or merchant_key in seen:
            continue
        contact = _merchant_contact_snapshot(signal.get("merchant_id"))
        if not contact.get("has_contact"):
            seeded = _maybe_seed_pattern_contact_resolution(
                merchant_id=signal.get("merchant_id"),
                merchant_domain=merchant_domain,
                distress_type=parsed_distress,
                urgency_score=0,
                queue_quality_score=int(quality.get("quality_score") or 0),
            )
            if seeded.get("attempted"):
                contact = _merchant_contact_snapshot(signal.get("merchant_id"))
        if not contact.get("has_contact"):
            continue
        seen.add(merchant_key)
        merchant = _display_merchant_name(merchant_name, merchant_domain) or merchant_domain or f"merchant {signal.get('merchant_id')}"
        next_move = "draft outreach" if eligibility_class == ELIGIBILITY_OUTREACH else "review fit before outreach"
        score = (
            quality_score
            + min(int(float(signal.get("confidence") or 0.0) * 20), 20)
            + (15 if eligibility_class == ELIGIBILITY_OUTREACH else 0)
            + 20
        )
        ranked.append((score, f"{merchant} — contact yes, next move: {next_move}"))
        if len(ranked) >= max(3, int(limit) * 2):
            break
    return ranked


def _pattern_target_summary(pattern: dict | None, *, limit: int = 3) -> list[str]:
    row = dict(pattern or {})
    processor = str(row.get("processor") or "unknown").strip().lower()
    distress_type = str(row.get("distress_type") or "unknown").strip().lower()
    industry = str(row.get("industry") or "unknown").strip().lower()
    cases = list_opportunities_by_operator_action_command(unselected_only=False, limit=50).get("cases", [])
    ranked: list[tuple[int, str]] = []
    seen: set[str] = set()
    for case in cases:
        case_processor = str(case.get("processor") or "unknown").strip().lower()
        case_distress = str(case.get("distress_type") or "unknown").strip().lower()
        case_industry = str(case.get("industry") or "unknown").strip().lower()
        if processor != "unknown" and case_processor != processor:
            continue
        if distress_type != "unknown" and case_distress != distress_type:
            continue
        if industry not in {"", "unknown"} and case_industry not in {"", "unknown"} and case_industry != industry:
            continue
        opportunity_id = int(case.get("opportunity_id") or 0)
        if not opportunity_id:
            continue
        fit = show_opportunity_fit_command(opportunity_id=opportunity_id)
        if fit.get("error"):
            continue
        if not fit.get("contact_email"):
            continue
        merchant_key = str(fit.get("merchant_domain") or fit.get("merchant_name") or opportunity_id).strip().lower()
        if merchant_key in seen:
            continue
        seen.add(merchant_key)
        merchant = fit.get("merchant_domain") or fit.get("merchant_name") or f"opportunity {opportunity_id}"
        contact = "yes" if fit.get("contact_email") else "no"
        next_move = _pattern_target_next_move(fit)
        score = (
            (30 if fit.get("operator_ready") else 0)
            + (25 if fit.get("high_conviction_prospect") else 0)
            + int(fit.get("queue_quality_score") or 0)
            + min(int(fit.get("contact_trust_score") or 0), 25)
        )
        ranked.append((score, f"{merchant} — contact {contact}, next move: {next_move}"))
    if len(ranked) < max(1, int(limit)):
        ranked.extend(_pattern_target_summary_from_profiles(row, limit=limit, seen=seen))
    if len(ranked) < max(1, int(limit)):
        ranked.extend(_pattern_target_summary_from_signals(row, limit=limit, seen=seen))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [line for _, line in ranked[: max(1, int(limit))]]


def _latest_meaningful_opportunity() -> dict | None:
    rows = list_deal_opportunities_command(limit=5).get("deal_opportunities", [])
    for row in rows:
        if row.get("status") in {"pending_review", "approved"}:
            return row
    return None


def _get_opportunity_record(opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict | None:
    with engine.connect() as conn:
        if opportunity_id is not None:
            row = conn.execute(
                text(
                    """
                    SELECT id, merchant_id, merchant_domain, processor, distress_topic,
                           sales_strategy, status, created_at
                    FROM merchant_opportunities
                    WHERE id = :opportunity_id
                    LIMIT 1
                    """
                ),
                {"opportunity_id": int(opportunity_id)},
            ).fetchone()
        elif merchant_domain:
            row = conn.execute(
                text(
                    """
                    SELECT id, merchant_id, merchant_domain, processor, distress_topic,
                           sales_strategy, status, created_at
                    FROM merchant_opportunities
                    WHERE merchant_domain = :merchant_domain
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"merchant_domain": merchant_domain},
            ).fetchone()
        else:
            row = None
    if not row:
        return None
    data = dict(row._mapping)
    if data.get("created_at"):
        data["created_at"] = data["created_at"].isoformat()
    return data


def _assemble_lead_context(opportunity_id: int | None = None, merchant_domain: str | None = None) -> dict | None:
    lead = _get_opportunity_record(opportunity_id=opportunity_id, merchant_domain=merchant_domain)
    if not lead:
        return None

    with engine.connect() as conn:
        merchant = _get_merchant_record(conn, lead)
        signal = _get_linked_signal_record(conn, lead, merchant)
        parsed_signal = _parse_signal_content((signal or {}).get("content"))
        action_record = _get_operator_action_record(conn, lead.get("id"))
        outreach_record = _get_outreach_record(conn, lead.get("id"))
    outreach_recommendation = get_outreach_recommendation_command(
        opportunity_id=lead.get("id"),
        merchant_domain=lead.get("merchant_domain") or "",
    )

    last_activity_at = _last_meaningful_activity_at(
        lead.get("created_at"),
        (merchant or {}).get("last_seen"),
        (signal or {}).get("detected_at"),
    )
    merchant_name = (
        (merchant or {}).get("canonical_name")
        or parsed_signal.get("merchant_name_candidate")
        or lead.get("merchant_name")
    )
    merchant_domain_value = (
        (merchant or {}).get("domain")
        or lead.get("merchant_domain")
        or parsed_signal.get("merchant_domain_candidate")
    )
    processor = (
        parsed_signal.get("processor")
        or (signal or {}).get("processor")
        or lead.get("processor")
    )
    if not processor:
        processor = "unknown"
    distress_type = parsed_signal.get("distress_type") or normalize_distress_topic(lead.get("distress_topic"))
    signal_summary = _signal_summary_line(parsed_signal, signal)
    merchant_name_display = _display_merchant_name(merchant_name, merchant_domain_value)
    lead_context = dict(lead)
    lead_context.update(
        {
            "merchant_name": merchant_name,
            "merchant_name_display": merchant_name_display,
            "merchant_domain": merchant_domain_value,
            "merchant_status": (merchant or {}).get("status"),
            "merchant_identity_summary": _merchant_identity_summary(merchant_name_display, merchant_domain_value),
            "distress_type": distress_type,
            "processor": processor,
            "signal_id": (signal or {}).get("id"),
            "signal_summary": signal_summary,
            "last_activity_at": last_activity_at,
            "last_signal_detected_at": _coerce_iso((signal or {}).get("detected_at")),
            "last_merchant_seen_at": _coerce_iso((merchant or {}).get("last_seen")),
            "recommended_operator_action": (action_record or {}).get("recommended_action") or operator_action_for_distress(distress_type),
            "selected_operator_action": (action_record or {}).get("selected_action") or operator_action_for_distress(distress_type),
            "selected_operator_action_reason": (action_record or {}).get("action_reason") or "",
            "selected_operator_action_updated_at": _coerce_iso((action_record or {}).get("updated_at")),
            "outreach_status": (outreach_record or {}).get("status") or "no_outreach",
            "outreach_approval_state": (outreach_record or {}).get("approval_state") or "approval_required",
            "outreach_play": (outreach_record or {}).get("selected_play") or "",
            "outreach_sent_at": _coerce_iso((outreach_record or {}).get("sent_at")),
            "outreach_replied_at": _coerce_iso((outreach_record or {}).get("replied_at")),
            "follow_up_due_at": _coerce_iso((outreach_record or {}).get("follow_up_due_at")),
            "contact_send_eligible": bool((outreach_recommendation or {}).get("contact_send_eligible")),
            "contact_quality_label": (outreach_recommendation or {}).get("contact_quality_label") or "blocked",
            "contact_state": (outreach_recommendation or {}).get("contact_state") or "no_contact_found",
            "contact_trust_score": int((outreach_recommendation or {}).get("contact_trust_score") or 0),
            "contact_reason": (outreach_recommendation or {}).get("contact_reason") or "",
            "contact_reason_codes": list((outreach_recommendation or {}).get("contact_reason_codes") or []),
            "contact_email": (outreach_recommendation or {}).get("contact_email") or (outreach_record or {}).get("contact_email") or "",
            "contact_name": (outreach_recommendation or {}).get("contact_name") or (outreach_record or {}).get("contact_name") or "",
            "contact_source_class": (outreach_recommendation or {}).get("contact_source_class") or "unknown_source",
            "contact_source_explanation": (outreach_recommendation or {}).get("contact_source_explanation") or "",
            "next_contact_move": (outreach_recommendation or {}).get("next_contact_move") or "",
            "best_target_roles": list((outreach_recommendation or {}).get("best_target_roles") or []),
            "target_role_reason": (outreach_recommendation or {}).get("target_role_reason") or "",
            "outreach_readiness_state": (outreach_recommendation or {}).get("readiness_state") or "blocked_contact_quality",
            "queue_eligibility_class": (outreach_recommendation or {}).get("queue_eligibility_class") or "outreach_eligible",
            "queue_eligibility_reason": (outreach_recommendation or {}).get("queue_eligibility_reason") or "",
            "queue_quality_score": int((outreach_recommendation or {}).get("queue_quality_score") or 0),
            "queue_domain_quality_label": (outreach_recommendation or {}).get("queue_domain_quality_label") or "",
            "queue_domain_quality_reason": (outreach_recommendation or {}).get("queue_domain_quality_reason") or "",
        }
    )
    return lead_context


def _get_merchant_record(conn, lead: dict) -> dict | None:
    if lead.get("merchant_id"):
        row = conn.execute(
            text(
                """
                SELECT id, canonical_name, domain, status, last_seen
                FROM merchants
                WHERE id = :merchant_id
                LIMIT 1
                """
            ),
            {"merchant_id": int(lead["merchant_id"])},
        ).mappings().first()
        if row:
            return dict(row)
    if lead.get("merchant_domain"):
        row = conn.execute(
            text(
                """
                SELECT id, canonical_name, domain, status, last_seen
                FROM merchants
                WHERE domain = :merchant_domain OR normalized_domain = :merchant_domain
                ORDER BY last_seen DESC NULLS LAST
                LIMIT 1
                """
            ),
            {"merchant_domain": lead["merchant_domain"]},
        ).mappings().first()
        if row:
            return dict(row)
    return None


def _get_linked_signal_record(conn, lead: dict, merchant: dict | None) -> dict | None:
    merchant_id = (merchant or {}).get("id") or lead.get("merchant_id")
    if merchant_id:
        merchant_id = int(merchant_id)

        direct_row = _safe_latest_signal_record(
            conn,
            text(
                """
                SELECT s.id, s.content, s.detected_at, s.source, s.confidence, s.classification
                FROM signals s
                WHERE s.merchant_id = :merchant_id
                ORDER BY s.detected_at DESC
                LIMIT 1
                """
            ),
            {"merchant_id": merchant_id},
            lookup_name="direct_signal_lookup",
        )
        if direct_row:
            return direct_row

        linked_row = _safe_latest_signal_record(
            conn,
            text(
                """
                SELECT s.id, s.content, s.detected_at, s.source, s.confidence, s.classification
                FROM signals s
                WHERE s.id IN (
                    SELECT ms.signal_id
                    FROM merchant_signals ms
                    WHERE ms.merchant_id = :merchant_id
                )
                ORDER BY s.detected_at DESC
                LIMIT 1
                """
            ),
            {"merchant_id": merchant_id},
            lookup_name="linked_signal_lookup",
        )
        if linked_row:
            return linked_row
    return None


def _safe_latest_signal_record(conn, query, params: dict, *, lookup_name: str) -> dict | None:
    try:
        row = conn.execute(query, params).mappings().first()
        return dict(row) if row else None
    except SQLAlchemyError as exc:
        message = str(exc).lower()
        if "deadlock detected" in message or "lock timeout" in message:
            record_component_state(
                "operator_status",
                ttl=600,
                last_operator_read_degraded=lookup_name,
                last_operator_read_error="deadlock_detected",
                last_operator_read_error_at=utc_now_iso(),
            )
            return None
        raise


def _get_operator_action_record(conn, opportunity_id: int | None) -> dict | None:
    if not opportunity_id:
        return None
    row = conn.execute(
        text(
            """
            SELECT recommended_action, selected_action, action_reason, updated_at
            FROM opportunity_operator_actions
            WHERE opportunity_id = :opportunity_id
            LIMIT 1
            """
        ),
        {"opportunity_id": int(opportunity_id)},
    ).mappings().first()
    return dict(row) if row else None


def _get_outreach_record(conn, opportunity_id: int | None) -> dict | None:
    if not opportunity_id:
        return None
    try:
        row = conn.execute(
            text(
                """
                SELECT status, approval_state, selected_play, sent_at, replied_at, follow_up_due_at
                FROM opportunity_outreach_actions
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
                """
            ),
            {"opportunity_id": int(opportunity_id)},
        ).mappings().first()
    except Exception:
        return None
    return dict(row) if row else None


def _get_opportunity_counts() -> dict:
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET statement_timeout TO {OPPORTUNITY_COUNTS_QUERY_TIMEOUT_MS}"))
            try:
                total = conn.execute(text("SELECT COUNT(*) FROM merchant_opportunities")).scalar() or 0
                created_24h = conn.execute(
                    text("SELECT COUNT(*) FROM merchant_opportunities WHERE created_at >= NOW() - INTERVAL '24 hours'")
                ).scalar() or 0
                pending_review = conn.execute(
                    text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'pending_review'")
                ).scalar() or 0
                approved = conn.execute(
                    text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'approved'")
                ).scalar() or 0
                converted = conn.execute(
                    text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'converted'")
                ).scalar() or 0
            finally:
                conn.execute(text("SET statement_timeout TO 0"))
        payload = {
            "total": int(total),
            "created_24h": int(created_24h),
            "pending_review": int(pending_review),
            "approved": int(approved),
            "converted": int(converted),
        }
        write_status_snapshot(OPPORTUNITY_COUNTS_CACHE_KEY, payload)
        return payload
    except Exception as exc:
        cached = read_status_snapshot(OPPORTUNITY_COUNTS_CACHE_KEY)
        if cached:
            stale = stale_status_snapshot(cached, str(exc))
            stale.pop("_meta", None)
            return stale
        unavailable = unavailable_status_snapshot(str(exc))
        unavailable.pop("_meta", None)
        return {
            "total": 0,
            "created_24h": 0,
            "pending_review": 0,
            "approved": 0,
            "converted": 0,
        }


def _top_gmail_issue(rows: list[dict]) -> str:
    if not rows:
        return ""
    issue_counts = {}
    for row in rows:
        key = row.get("distress_type") or "unknown"
        issue_counts[key] = issue_counts.get(key, 0) + 1
    return max(issue_counts.items(), key=lambda item: item[1])[0]


def _system_outreach_line(metrics: dict) -> str:
    awaiting = int(metrics.get("outreach_awaiting_approval", 0) or 0)
    follow_ups = int(metrics.get("follow_ups_due", 0) or 0)
    if awaiting > 0 and follow_ups > 0:
        return f"{awaiting} lead(s) are ready for outreach approval and {follow_ups} sent outreach case(s) need follow-up."
    if awaiting > 0:
        return f"{awaiting} lead(s) are ready for outreach approval."
    if follow_ups > 0:
        return f"{follow_ups} sent outreach case(s) need follow-up."
    return "The outreach queue is clear."


def _top_rising_pattern() -> dict | None:
    patterns = list_actionable_patterns_command(limit=5).get("patterns", [])
    for pattern in patterns:
        if pattern.get("trend_label") == "rising":
            return pattern
    return patterns[0] if patterns else None


def _top_high_value_merchant_profile() -> dict | None:
    profiles = list_merchant_profiles_command(limit=5).get("profiles", [])
    for profile in [_select_focus_profile(profiles)] + profiles:
        if not profile:
            continue
        if int(profile.get("urgency_score", 0) or 0) >= 60 and profile.get("opportunity_status") in {"pending_review", "approved"}:
            return profile
    return None


def _top_conversion_signal() -> dict | None:
    patterns = show_best_conversion_patterns_command(limit=5).get("patterns", [])
    return patterns[0] if patterns else None


def _merchant_profile_label(profile: dict) -> str:
    if not profile:
        return ""
    name = _display_merchant_name(profile.get("merchant_name"), profile.get("merchant_domain"))
    domain = profile.get("merchant_domain")
    if name and domain:
        return f"{name} ({domain})"
    return name or domain or "the top merchant"


def _select_focus_profile(profiles: list[dict]) -> dict:
    for profile in profiles:
        if profile.get("merchant_domain"):
            return profile
    return profiles[0] if profiles else {}


def _conversion_pattern_label(pattern: dict) -> str:
    if not pattern:
        return ""
    processor = pattern.get("processor") or "unknown"
    distress = pattern.get("distress_type") or "unknown"
    normalized_processor = str(processor).strip().lower()
    distress_text = "distress" if distress == "unknown" else str(distress).replace("_", " ")
    industry = pattern.get("industry")
    if normalized_processor in {"unknown", "unclassified", ""}:
        base = f"{distress_text} with unresolved processor context"
    else:
        processor_text = str(processor).replace("_", " ").title()
        base = f"{processor_text} {distress_text}"
    if industry and industry not in {"unknown", ""}:
        return f"{base} in {industry}"
    return base


def _pattern_actionability_text(pattern: dict) -> str:
    if not pattern:
        return ""
    if pattern.get("actionable"):
        return "It looks actionable right now."
    trend = pattern.get("trend_label") or "stable"
    if trend == "fading":
        return "It is fading, so it is more of a watch item than an immediate push."
    return "It is worth watching, but it is not pulling focus yet."


def _pattern_interpretation_line(pattern: dict) -> str:
    if not pattern:
        return ""
    distress = normalize_distress_topic(pattern.get("distress_type"))
    processor = pattern.get("processor") or "unknown"
    industry = pattern.get("industry") or "unknown"
    if distress == "account_frozen":
        if processor != "unknown" and industry != "unknown":
            return f"Historically, {processor.replace('_', ' ').title()} freezes in {industry} often precede processor migration demand."
        return "Historically, account freezes often precede processor migration demand."
    if distress == "payouts_delayed":
        return "Historically, payout delays signal immediate liquidity pressure and push merchants toward faster alternatives."
    if distress == "reserve_hold":
        return "Historically, reserve pressure means merchants need both cash-flow relief and processor negotiation support."
    if distress == "verification_review":
        return "Historically, review-driven distress can flip quickly if the merchant gets compliance help fast enough."
    if distress == "processor_switch_intent":
        return "Historically, merchants already asking about alternatives are high-intent and easier to move quickly."
    if distress == "chargeback_issue":
        return "Historically, chargeback-heavy cases convert only when we pair processor advice with operational remediation."
    if distress == "account_terminated":
        return "Historically, terminated-account cases are urgent because payment continuity is already broken."
    if distress == "onboarding_rejected":
        return "Historically, rejected onboarding cases are valuable when we can quickly place the merchant with an alternative processor."
    if pattern.get("trend_label") == "rising":
        return "This usually means demand is clustering before the operators feel it in the queue."
    return ""


def _pattern_strategy_line(pattern: dict) -> str:
    if not pattern:
        return ""
    distress = pattern.get("distress_type")
    strategy = strategy_for_distress(distress)
    doctrine = doctrine_priority_reason(
        processor=pattern.get("processor"),
        distress_type=distress,
        industry=pattern.get("industry"),
    )
    if distress and distress != "unknown":
        action = pattern.get("recommended_operator_action") or operator_action_for_distress(distress)
        return f"{doctrine} The canonical play here is {strategy}. The recommended next operator action is {_operator_action_label(action)}."
    return f"{doctrine} The next step is to clarify the distress before escalating."


def _pattern_conversion_line(pattern: dict) -> str:
    if not pattern:
        return ""
    wins = int(pattern.get("wins", 0) or 0)
    pending = int(pattern.get("pending", 0) or 0)
    if wins > 0:
        return f"Similar opportunities have converted before, with {wins} win(s) already recorded."
    if pending > 0:
        return f"We have {pending} open opportunity case(s) tied to it, but no closed wins yet."
    return "We do not have enough closed-loop conversion history on it yet."


def _conversion_learning_line(pattern: dict) -> str:
    if not pattern:
        return ""
    wins = int(pattern.get("wins", 0) or 0)
    pending = int(pattern.get("pending", 0) or 0)
    losses = int(pattern.get("losses", 0) or 0)
    ignored = int(pattern.get("ignored", 0) or 0)
    total = int(pattern.get("opportunity_count", 0) or 0)
    if wins > 0:
        return "That suggests the current positioning is resonating well enough to keep leaning into this play."
    if pending >= 10 and wins == 0:
        return "That suggests the current outreach or positioning may not be resonating yet."
    if losses + ignored >= max(3, total // 2) and total > 0:
        return "That suggests the current play is meeting resistance and needs tighter positioning."
    if pending > 0:
        return "That gives us demand signal, but not enough evidence yet to say the play is working."
    return "We still need more closed-loop outcomes before drawing a strong lesson from it."


def _pattern_recommendation(pattern: dict) -> str:
    if not pattern:
        return "Keep watching for a sharper processor-specific pattern before changing focus."
    label = _conversion_pattern_label(pattern) or "this pattern"
    distress = pattern.get("distress_type")
    strategy = strategy_for_distress(distress)
    if pattern.get("actionable"):
        return f"Prioritize merchants exposed to {label} while the signal is still fresh, then work the {strategy} playbook."
    if pattern.get("trend_label") == "fading":
        return f"Keep {label} on watch, but do not let it pull attention away from fresher leads."
    return f"Keep monitoring {label} until it either sharpens, starts converting, or clearly points to a stronger {strategy} play."


def _focus_profile_snapshot() -> dict:
    profiles = list_merchant_profiles_command(limit=5).get("profiles", [])
    return _select_focus_profile(profiles)


def _queue_priority_line(profile: dict, *, counts: dict | None = None) -> str:
    if not profile:
        return "I have queue-level volume, but not a sharper high-leverage slice yet."
    label = _merchant_profile_label(profile)
    processor = _humanize_label(profile.get("processor")) or "unknown processor"
    distress = _humanize_label(profile.get("distress_type")) or "merchant distress"
    industry = profile.get("industry") or "unknown"
    prefix = ""
    if counts and int(counts.get("pending_review", 0) or 0) > 0:
        prefix = f"{int(counts.get('pending_review', 0) or 0)} opportunities are waiting for review. "
    if industry not in {"", "unknown", None}:
        return f"{prefix}The highest-leverage cases right now look like {processor}-related {industry} merchants dealing with {distress}, led by {label}."
    return f"{prefix}The highest-leverage cases right now look like merchants dealing with {processor} {distress}, led by {label}."


def _queue_recommendation_line(profile: dict, *, fallback_label: str) -> str:
    if not profile:
        return f"Review {fallback_label} next."
    label = _merchant_profile_label(profile)
    action = profile.get("recommended_operator_action") or operator_action_for_distress(profile.get("distress_type"))
    return f"Start with {label}, then work the {_operator_action_label(action)} play before moving deeper into the queue."


def _queue_quality_line(metrics: dict) -> str:
    ratio = float(metrics.get("blocked_queue_quality_ratio", 0.0) or 0.0)
    suppressed = int(metrics.get("suppressed_from_blocked_queue_24h", 0) or 0)
    eligible = int(metrics.get("outreach_eligible_opportunities", 0) or 0)
    if suppressed > 0:
        return (
            f"The blocked queue is smaller and cleaner now: {eligible} outreach-eligible case(s) remain, "
            f"and {suppressed} weak or unpromotable case(s) are suppressed from the primary blocked queue."
        )
    if ratio > 0:
        return f"The blocked queue quality ratio is {round(ratio * 100, 1)}%, so the remaining cases are mostly contactable."
    return "The queue-quality filter is active, but there are no outreach-eligible blocked cases right now."


def _outreach_case_why_now(case: dict, lead: dict | None) -> str:
    metadata = (case or {}).get("metadata_json") or {}
    why_now = metadata.get("why_now") if isinstance(metadata, dict) else ""
    if why_now:
        return why_now[0].lower() + why_now[1:] if len(why_now) > 1 else why_now.lower()
    distress = normalize_distress_topic((lead or {}).get("distress_type") or (lead or {}).get("distress_topic"))
    processor = _humanize_label((lead or {}).get("processor"))
    if processor and distress not in {"", "unknown", None}:
        return f"the {processor} {distress.replace('_', ' ')} signal is still fresh and the play is {_operator_action_label((case or {}).get('selected_play'))}"
    if distress not in {"", "unknown", None}:
        return f"the {distress.replace('_', ' ')} signal is still fresh and the play is {_operator_action_label((case or {}).get('selected_play'))}"
    return f"the outreach play is {_operator_action_label((case or {}).get('selected_play'))} and the case is already operator-scoped"


def _merchant_doctrine_summary(profile: dict) -> str:
    if not profile:
        return ""
    parts = []
    base = profile.get("why_it_matters")
    if base:
        parts.append(base)
    doctrine = doctrine_priority_reason(
        processor=profile.get("processor"),
        distress_type=profile.get("distress_type"),
        industry=profile.get("industry"),
    )
    if doctrine:
        parts.append(doctrine)
    distress = normalize_distress_topic(profile.get("distress_type"))
    if distress != "unknown":
        parts.append(f"The canonical strategy here is {strategy_for_distress(distress)}.")
        parts.append(f"The best-fit operator action is {_operator_action_label(profile.get('recommended_operator_action') or operator_action_for_distress(distress))}.")
    return " ".join(parts).strip()


def _merchant_profile_recommendation(profile: dict) -> str:
    if not profile:
        return "Stay with the current opportunity queue and wait for a stronger merchant profile."
    label = _merchant_profile_label(profile)
    distress = normalize_distress_topic(profile.get("distress_type"))
    strategy = strategy_for_distress(distress)
    if distress == "unknown":
        return f"Start with {label} while the context is still fresh, then clarify the payment distress before you spread attention wider."
    return f"Start with {label}, and treat it like {_strategy_case_phrase(strategy)} while the context is still fresh."


def _focus_recommendation_line(merchant_payload: dict, pattern_payload: dict, conversion_payload: dict) -> str:
    merchant_recommendation = merchant_payload.get("recommendation")
    if merchant_recommendation:
        merchant_snapshot = merchant_payload.get("snapshot", {})
        doctrine = doctrine_priority_reason(
            processor=merchant_snapshot.get("top_processor"),
            distress_type=merchant_snapshot.get("top_distress_type"),
        )
        if doctrine:
            return f"{doctrine} {merchant_recommendation}"
        return merchant_recommendation
    pattern_recommendation = pattern_payload.get("recommendation")
    if pattern_recommendation:
        return f"Under PayFlux doctrine, focus first on processor-linked merchant distress with the clearest liquidity or switching signal. {pattern_recommendation}"
    return conversion_payload.get("recommendation") or "Keep the operator focus on the sharpest processor-linked distress case."


def _provider_success_lines(reasoning: dict) -> tuple[str, str]:
    settings = reasoning_settings()
    tier1 = "no calls yet"
    tier2 = "no calls yet"
    for row in _ordered_provider_rows(reasoning):
        label = f"{round(float(row.get('success_rate', 0.0)) * 100, 1)}%"
        if row.get("provider") == settings.get("tier1_provider") and row.get("model") == settings.get("tier1_model"):
            tier1 = label
        elif row.get("provider") == settings.get("tier2_provider") and row.get("model") == settings.get("tier2_model"):
            tier2 = label
    return tier1, tier2


def _ordered_provider_rows(reasoning: dict) -> list[dict]:
    settings = reasoning_settings()
    rows = list(reasoning.get("reasoning_provider_performance_24h", []))

    def sort_key(row: dict):
        provider = row.get("provider")
        model = row.get("model")
        if provider == settings.get("tier1_provider") and model == settings.get("tier1_model"):
            return (0, provider or "", model or "")
        if provider == settings.get("tier2_provider") and model == settings.get("tier2_model"):
            return (1, provider or "", model or "")
        return (2, provider or "", model or "")

    return sorted(rows, key=sort_key)


def _overall_health_label() -> str:
    scheduler_state = get_component_state("scheduler")
    gmail_state = get_component_state("gmail")
    reasoning_state = get_component_state("reasoning")
    telegram_state = get_component_state("telegram")
    if reasoning_state.get("reasoning_status") == "degraded":
        return "attention"
    if gmail_state.get("gmail_status") == "degraded":
        return "attention"
    if str(telegram_state.get("proactive_watchdog_status") or telegram_state.get("status") or "").strip().lower() in {"offline", "degraded", "attention"}:
        return "attention"
    if scheduler_state.get("last_error"):
        error_at = scheduler_state.get("last_error_at")
        if not is_stale(error_at, 900):
            return "attention"
    return "normal"


def _telegram_delivery_line(telegram_delivery: dict) -> str:
    if not isinstance(telegram_delivery, dict):
        return ""
    status = str(telegram_delivery.get("status") or "").strip().lower()
    headline = str(telegram_delivery.get("headline") or "").strip()
    if status in {"offline", "degraded", "attention"} and headline:
        return headline
    return ""


def _current_reasoning_regression() -> dict | None:
    reasoning = show_reasoning_metrics_command()
    settings = reasoning_settings()
    for row in reasoning.get("reasoning_provider_performance_24h", []):
        if row.get("provider") == settings.get("tier1_provider") and row.get("model") == settings.get("tier1_model"):
            if int(row.get("total_calls", 0)) >= 2 and float(row.get("success_rate", 0.0)) < 0.6:
                return {"headline": f"Tier 1 regression detected for {row.get('model')}."}
        if row.get("provider") == settings.get("tier2_provider") and row.get("model") == settings.get("tier2_model"):
            if int(row.get("total_calls", 0)) >= 2 and float(row.get("success_rate", 0.0)) < 0.6:
                return {"headline": f"Tier 2 regression detected for {row.get('model')}."}
    return None


def _natural_label(field: str) -> str:
    return {
        "health": "system health",
        "signals_24h": "24-hour signal volume",
        "signals_1h": "hourly signal volume",
        "gmail_merchant_distress": "Gmail merchant distress",
        "opportunities_pending_review": "opportunities waiting for review",
        "opportunities_created_24h": "new opportunities",
        "patterns_visible": "visible distress patterns",
        "top_pattern": "top distress pattern",
        "top_pattern_velocity_24h": "top pattern velocity",
        "top_pattern_merchants": "merchants in the top pattern",
        "actionable_patterns_visible": "actionable distress patterns",
        "top_actionable_pattern": "top actionable pattern",
        "top_actionable_trend": "top actionable trend",
        "top_actionable_converted_before": "top actionable conversion history",
        "profiles_visible": "visible merchant profiles",
        "top_merchant": "top merchant profile",
        "top_processor": "top merchant processor",
        "top_distress_type": "top merchant distress",
        "top_urgency_score": "top merchant urgency",
        "conversion_patterns_visible": "tracked conversion patterns",
        "top_conversion_pattern": "top conversion pattern",
        "top_conversion_wins": "wins in the top conversion pattern",
        "top_conversion_pending": "pending opportunities in the top conversion pattern",
        "top_conversion_win_rate": "top conversion win rate",
        "best_conversion_pattern": "best conversion pattern",
        "best_conversion_wins": "wins in the best conversion pattern",
        "best_conversion_pending": "pending opportunities in the best conversion pattern",
        "best_conversion_win_rate": "best conversion win rate",
        "focus_merchant": "focus merchant",
        "focus_pattern": "focus pattern",
        "focus_conversion_pattern": "focus conversion pattern",
        "reasoning_status": "reasoning status",
        "rule_only_24h": "rule-only decisions",
        "tier1_calls_24h": "Tier 1 usage",
        "tier2_calls_24h": "Tier 2 usage",
        "reasoning_failures_24h": "reasoning failures",
        "latest_domain": "latest opportunity",
        "merchant_name": "merchant name",
        "top_issue": "top issue",
        "system_health": "system health",
        "most_promising": "most promising lead",
        "tier1_success": "Tier 1 success",
        "tier2_success": "Tier 2 success",
        "last_activity_at": "last lead activity",
        "outcome_update_status": "outcome update status",
        "opportunity_id": "opportunity",
        "outcome_status": "outcome status",
        "pattern_key": "pattern key",
    }.get(field, field.replace("_", " "))


def _summarize_sales_strategy(value) -> str:
    if isinstance(value, dict):
        return value.get("angle") or value.get("recommended_pitch") or value.get("source") or "strategy available"
    return str(value)


def _lead_state_sentence(lead: dict) -> str:
    domain = _lead_label(lead) or "This lead"
    if lead.get("queue_eligibility_class") and lead.get("queue_eligibility_class") != "outreach_eligible":
        return f"{domain} is currently {str(lead.get('queue_eligibility_class')).replace('_', ' ')}."
    status = lead.get("status") or "unknown"
    readable_status = {
        "pending_review": "is waiting for review",
        "approved": "has already been approved",
        "rejected": "has been rejected",
        "converted": "has converted",
        "outreach_sent": "has already had outreach sent",
    }.get(status, f"is currently {status}")
    return f"{domain} {readable_status}."


def _lead_why_it_matters(lead: dict) -> str:
    parts = []
    if lead.get("merchant_identity_summary"):
        parts.append(lead["merchant_identity_summary"])

    if lead.get("queue_eligibility_class") and lead.get("queue_eligibility_class") != "outreach_eligible":
        parts.append(lead.get("queue_eligibility_reason") or "This case is not in the primary blocked outreach queue.")
        if lead.get("queue_domain_quality_reason"):
            parts.append(lead.get("queue_domain_quality_reason"))
        doctrine = doctrine_priority_reason(
            processor=lead.get("processor"),
            distress_type=lead.get("distress_type") or normalize_distress_topic(lead.get("distress_topic")),
            industry=lead.get("industry"),
        )
        if doctrine:
            parts.append(doctrine)
        return " ".join(part for part in parts if part)

    processor = lead.get("processor")
    distress = lead.get("distress_type") or normalize_distress_topic(lead.get("distress_topic"))
    strategy = _summarize_sales_strategy(lead.get("sales_strategy")) if lead.get("sales_strategy") else ""
    signal_summary = lead.get("signal_summary")
    last_activity_at = lead.get("last_activity_at")

    if signal_summary:
        parts.append(signal_summary)
    elif processor and processor != "unknown" and distress and distress != "unknown":
        parts.append(f"The linked signal points to {_humanize_label(processor)} trouble around {_humanize_label(distress)}.")
    elif distress and distress != "unknown":
        parts.append(f"The issue on record is {_humanize_label(distress)}.")
    elif processor and processor != "unknown":
        parts.append(f"The processor in play is {_humanize_label(processor)}.")

    if last_activity_at and last_activity_at != "unknown":
        parts.append(f"The last meaningful lead activity I have is from {_humanize_timestamp(last_activity_at)}.")
    else:
        parts.append("I do not have a fresher lead-specific activity timestamp yet.")

    if strategy:
        parts.append(f"The current angle is {strategy}.")
    outreach_status = lead.get("outreach_status")
    contact_state = lead.get("contact_state") or "no_contact_found"
    if not lead.get("contact_send_eligible"):
        parts.append(
            " ".join(
                part
                for part in [
                    f"Outreach is blocked on contact quality right now. The current contact state is {contact_state.replace('_', ' ')}.",
                    lead.get("contact_source_explanation") or "",
                    lead.get("contact_reason") or "No trusted merchant contact is on file yet.",
                ]
                if part
            )
        )
    elif outreach_status == "awaiting_approval":
        parts.append("A Gmail outreach draft is already waiting for approval.")
    elif outreach_status == "draft_ready":
        parts.append("A Gmail outreach draft is ready to send once you give the word.")
    elif outreach_status == "sent":
        parts.append("Outreach has already been sent and the thread is waiting on a reply.")
    elif outreach_status == "replied":
        parts.append("The merchant has replied, so the next move should stay inside that thread.")
    elif outreach_status == "follow_up_needed":
        parts.append("The last outreach is now due for follow-up.")
    elif lead.get("outreach_readiness_state") == "send_ready":
        parts.append("The lead is send-ready from a contact-trust standpoint.")
    doctrine = doctrine_priority_reason(
        processor=processor,
        distress_type=distress,
        industry=lead.get("industry"),
    )
    if doctrine:
        parts.append(doctrine)
    if distress and distress != "unknown":
        parts.append(f"The canonical strategy here is {strategy_for_distress(distress)}.")
        parts.append(f"The recommended next operator action is {_operator_action_label(lead.get('recommended_operator_action') or operator_action_for_distress(distress))}.")
    if lead.get("best_target_roles"):
        parts.append(
            f"The best target roles here are {', '.join(lead.get('best_target_roles') or [])} because {lead.get('target_role_reason') or 'they are closest to the processor problem'}."
        )

    if not parts:
        return "I have a lead-specific status, but not much more detail than the current state."
    return " ".join(parts)


def _lead_recommendation(lead: dict) -> str:
    if lead.get("queue_eligibility_class") and lead.get("queue_eligibility_class") != "outreach_eligible":
        return lead.get("queue_eligibility_reason") or "Keep this case out of the primary outreach queue."
    processor = lead.get("processor")
    distress = lead.get("distress_type") or normalize_distress_topic(lead.get("distress_topic"))
    outreach_status = lead.get("outreach_status") or "no_outreach"
    if not lead.get("contact_send_eligible"):
        return lead.get("next_contact_move") or lead.get("contact_reason") or "Hold outreach until a trusted merchant-owned contact is on file."
    if outreach_status == "awaiting_approval":
        return "Review the drafted outreach, then approve it if the distress is still live."
    if outreach_status == "draft_ready":
        return "Send the approved outreach when you want this case to go live."
    if outreach_status == "replied":
        return "Review the merchant reply first, then decide whether a reply-follow-up should go out."
    if outreach_status == "follow_up_needed":
        return "Draft the next follow-up now if the original distress is still active."
    if lead.get("status") in {"pending_review", "approved"} and processor not in {None, "", "unknown"} and distress not in {None, "", "unknown"}:
        strategy = strategy_for_distress(distress)
        action = lead.get("recommended_operator_action") or operator_action_for_distress(distress)
        return f"Treat this like {_strategy_case_phrase(strategy)}: {_lead_next_action_line(lead, action)}"
    if lead.get("status") in {"pending_review", "approved"}:
        return "Review the lead with the linked signal context in mind, then decide whether to move it forward."
    return "No immediate action is needed unless you want to reopen the lead."


def _strategy_case_phrase(strategy: str | None) -> str:
    cleaned = (strategy or "").strip()
    if not cleaned:
        return "the current distress case"
    article = "an" if cleaned[0].lower() in "aeiou" else "a"
    return f"{article} {cleaned} case"


def _operator_action_label(action: str | None) -> str:
    return operator_action_label(action)


def _action_case_label(case: dict) -> str:
    if not case:
        return ""
    name = _display_merchant_name(case.get("merchant_name"), case.get("merchant_domain"))
    domain = case.get("merchant_domain")
    if name and domain:
        return f"{name} ({domain})"
    return name or domain or str(case.get("opportunity_id") or "lead")


def _merchant_action_label(case: dict) -> str:
    if not case:
        return ""
    name = _display_merchant_name(case.get("merchant_name"), case.get("merchant_domain"))
    domain = case.get("merchant_domain")
    if name and domain:
        return f"{name} ({domain})"
    return name or domain or str(case.get("merchant_key") or "merchant")


def _merchant_action_values_text(values: list[str]) -> str:
    cleaned = [_operator_action_label(value) for value in values if value]
    if not cleaned:
        return "no selected action"
    if len(cleaned) == 1:
        return cleaned[0]
    return ", ".join(cleaned[:-1]) + f", and {cleaned[-1]}"


def _lead_action_fit_line(lead: dict) -> str:
    processor = lead.get("processor")
    distress = normalize_distress_topic(lead.get("distress_type") or lead.get("distress_topic"))
    if processor not in {None, "", "unknown"} and distress != "unknown":
        return f"This fits because {_humanize_label(processor)} is tied to {_humanize_label(distress)} on this lead."
    if distress != "unknown":
        return f"This fits because the lead is showing {_humanize_label(distress)}."
    return "This is the safest play until the distress is clearer."


def _lead_next_action_line(lead: dict, action: str | None) -> str:
    normalized = action or "clarify_distress"
    processor = _humanize_label(lead.get("processor"))
    if normalized == "urgent_processor_migration":
        return f"review the {processor} exposure, confirm the account restriction is still live, and decide whether to move the migration play forward."
    if normalized == "payout_acceleration":
        return "confirm how severe the payout delay is, then decide whether to move the payout acceleration play forward."
    if normalized == "reserve_negotiation":
        return "confirm the reserve pressure and decide whether reserve negotiation support is the next move."
    if normalized == "compliance_remediation":
        return "confirm the verification blocker and decide whether compliance remediation support should be the next step."
    if normalized == "chargeback_mitigation":
        return "check the dispute pressure and decide whether chargeback mitigation support is the right move."
    if normalized == "onboarding_assistance":
        return "confirm the merchant is still seeking an alternative and decide whether onboarding assistance should move next."
    return "clarify the exact distress before you commit operator time."


def _pattern_next_action_line(pattern: dict, action: str | None) -> str:
    normalized = action or "clarify_distress"
    label = _conversion_pattern_label(pattern) or "this pattern"
    if normalized == "urgent_processor_migration":
        return f"Use {label} to surface merchants that need urgent processor migration first."
    if normalized == "payout_acceleration":
        return f"Use {label} to surface merchants whose payout delay risk needs immediate review."
    if normalized == "reserve_negotiation":
        return f"Use {label} to find merchants where reserve pressure is now worth operator attention."
    if normalized == "compliance_remediation":
        return f"Use {label} to find merchants that need compliance remediation support next."
    if normalized == "chargeback_mitigation":
        return f"Use {label} to find merchants where dispute pressure is likely to need intervention."
    if normalized == "onboarding_assistance":
        return f"Use {label} to find merchants who are ready for onboarding help right now."
    return f"Keep {label} on watch until the distress is sharp enough to pick a stronger operator play."


def _lead_label(lead: dict) -> str:
    name = lead.get("merchant_name_display") or lead.get("merchant_name")
    domain = lead.get("merchant_domain")
    if name and domain:
        return f"{name} ({domain})"
    return name or domain or str(lead.get("id") or "lead")


def _merchant_identity_summary(name: str | None, domain: str | None) -> str:
    if name and domain:
        return f"The merchant on record is {name} at {domain}."
    if domain:
        return f"The merchant on record is {domain}."
    if name:
        return f"The merchant on record is {name}."
    return "I do not have a clean merchant identity on this lead yet."


def _signal_summary_line(parsed_signal: dict, signal: dict | None) -> str:
    if not signal:
        return ""
    processor = parsed_signal.get("processor")
    distress = parsed_signal.get("distress_type")
    subject = parsed_signal.get("subject")
    domain = parsed_signal.get("merchant_domain_candidate")
    if processor and distress:
        detail = f"The linked signal says {_humanize_label(processor)} is tied to {_humanize_label(distress)}"
        if domain:
            detail += f" for {domain}"
        return detail + "."
    if subject:
        return f"The linked signal is the Gmail thread '{subject}'."
    return "There is a linked distress signal on this lead."


def _parse_signal_content(content: str | None) -> dict:
    if not content:
        return {}
    parsed = {
        "processor": _match_group(r"\[processor:([^\]]+)\]", content),
        "distress_type": _match_group(r"\[distress:([^\]]+)\]", content),
        "subject": _match_group(r"^Subject:\s*(.+)$", content, flags=re.MULTILINE),
        "sender": _match_group(r"^Sender:\s*(.+)$", content, flags=re.MULTILINE),
        "sender_domain": _match_group(r"^Sender domain:\s*(.+)$", content, flags=re.MULTILINE),
        "merchant_name_candidate": _match_group(r"^Merchant candidate:\s*(.+)$", content, flags=re.MULTILINE),
        "merchant_domain_candidate": _match_group(r"^Merchant domain candidate:\s*(.+)$", content, flags=re.MULTILINE),
        "signals": _split_csv(_match_group(r"^Signals:\s*(.+)$", content, flags=re.MULTILINE)),
    }
    return {key: value for key, value in parsed.items() if value is not None and value != "" and value != []}


def _humanize_label(value: str | None) -> str:
    if not value:
        return "unknown"
    text_value = str(value).replace("_", " ").strip()
    if not text_value:
        return "unknown"
    replacements = {
        "stripe": "Stripe",
        "paypal": "PayPal",
        "adyen": "Adyen",
        "braintree": "Braintree",
        "authorize net": "Authorize.net",
        "checkout com": "Checkout.com",
        "unclassified": "an unresolved processor",
    }
    return replacements.get(text_value.lower(), text_value)


def _display_merchant_name(name: str | None, domain: str | None = None) -> str | None:
    if not name and not domain:
        return None

    candidate = (name or "").strip()
    if candidate:
        candidate = re.sub(r"[_\-]+", " ", candidate)
        candidate = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", candidate)
        candidate = re.sub(r"\s+", " ", candidate).strip()
        candidate = _split_known_suffixes(candidate)
        candidate = " ".join(_title_preserving_acronyms(part) for part in candidate.split())
        if len(candidate) > 2:
            return candidate

    if domain:
        root = domain.split(".", 1)[0]
        root = re.sub(r"[_\-]+", " ", root)
        root = _split_known_suffixes(root)
        root = re.sub(r"\s+", " ", root).strip()
        if root:
            return " ".join(_title_preserving_acronyms(part) for part in root.split())
    return name


def _split_known_suffixes(value: str) -> str:
    suffixes = [
        "cart",
        "goods",
        "store",
        "shop",
        "market",
        "commerce",
        "pay",
        "payments",
        "labs",
        "studio",
        "digital",
        "health",
        "wellness",
        "systems",
        "solutions",
        "agency",
        "group",
    ]
    compact = value.replace(" ", "")
    lowered = compact.lower()
    for suffix in suffixes:
        if lowered.endswith(suffix) and len(compact) > len(suffix) + 2:
            prefix = compact[: -len(suffix)]
            return f"{prefix} {suffix}"
    return value


def _title_preserving_acronyms(token: str) -> str:
    if token.isupper() and len(token) <= 5:
        return token
    if token.lower() in {"saas", "kyc", "kyb"}:
        return token.upper()
    return token.capitalize()


def _humanize_timestamp(value: str | None) -> str:
    if not value or value == "unknown":
        return "an unknown time"
    dt = _parse_timestamp(value)
    if not dt:
        return value
    now = datetime.now(timezone.utc)
    delta = now - dt
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 90 * 60:
        minutes = max(1, round(seconds / 60))
        if minutes < 60:
            return f"about {minutes} minutes ago"
        hours = max(1, round(minutes / 60))
        return f"about {hours} hour{'s' if hours != 1 else ''} ago"
    if dt.date() == now.date():
        return f"earlier today at {dt.strftime('%-I:%M %p UTC')}"
    if (now.date() - dt.date()).days == 1:
        return f"yesterday at {dt.strftime('%-I:%M %p UTC')}"
    return dt.strftime("%B %-d at %-I:%M %p UTC")


def _parse_timestamp(value: str):
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _match_group(pattern: str, value: str, *, flags: int = 0) -> str | None:
    match = re.search(pattern, value, flags)
    if not match:
        return None
    result = (match.group(1) or "").strip()
    return result or None


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _coerce_iso(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _last_meaningful_activity_at(*timestamps) -> str | None:
    valid = [ts for ts in timestamps if ts]
    if not valid:
        return None
    normalized = [_coerce_iso(ts) for ts in valid if _coerce_iso(ts)]
    if not normalized:
        return None
    return max(normalized)
