import sys, os, redis, re
from typing import Any
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import json
from memory.structured.db import save_event
from runtime.channels.service import (
    create_gmail_signal,
    draft_gmail_reply,
    draft_gmail_distress_reply,
    draft_reddit_reply,
    list_gmail_merchant_distress,
    list_gmail_triage,
    list_gmail_threads,
    list_reddit_candidates,
    run_gmail_triage_cycle,
    send_gmail_reply,
    send_reddit_reply,
    set_channel_kill_switch,
    set_channel_mode,
    show_gmail_thread_intelligence,
    show_channel_audit_log,
)
from runtime.channels.store import get_gmail_thread_intelligence
from runtime.reasoning.control_plane import (
    rerun_reasoning_task,
    show_reasoning_decision,
    show_reasoning_decision_log,
    show_reasoning_metrics,
)
from runtime.reasoning.store import list_reasoning_decisions
from runtime.reasoning.evaluation import run_reasoning_evaluation
from runtime.intelligence.payflux_intelligence import (
    get_conversion_intelligence,
    list_merchants_by_operator_action,
    list_opportunities_by_operator_action,
    get_opportunity_operator_action,
    get_merchant_intelligence_profile,
    get_payflux_intelligence_metrics,
    get_top_merchant_profiles,
    list_distress_patterns,
    mark_opportunity_operator_action,
    mark_opportunity_outcome,
    sync_payflux_intelligence,
)
from runtime.intelligence.distress_normalization import normalize_distress_topic
from runtime.intelligence.opportunity_queue_quality import evaluate_opportunity_queue_quality
from runtime.ops.mission_execution import get_latest_execution_plan, get_primary_operator_user_id, run_mission_execution_loop
from runtime.ops.strategic_deliberation import get_latest_brain_state, run_strategic_deliberation_loop
from runtime.ops.critic_review import run_brain_critic_loop, show_latest_brain_review, review_outreach_draft
from runtime.ops.value_heartbeat import _build_signal_candidate_pool, get_value_heartbeat
from runtime.ops.telegram_delivery_watchdog import get_telegram_delivery_health
from runtime.ops.outreach_execution import (
    approve_outreach_for_opportunity,
    draft_outreach_for_opportunity,
    get_contact_intelligence_for_opportunity,
    get_outreach_context_snapshot,
    get_outreach_execution_metrics,
    get_selected_outreach_for_opportunity,
    list_blocked_outreach_leads,
    list_outreach_awaiting_approval,
    list_send_eligible_outreach_leads,
    list_sent_outreach_needing_follow_up,
    recommend_outreach_for_opportunity,
    rewrite_outreach_for_opportunity,
    preview_rewrite_outreach_for_opportunity,
    send_outreach_for_opportunity,
    sync_outreach_execution_state,
    update_outreach_outcome,
)
from runtime.ops.outreach_learning import get_outreach_learning_signal, list_outreach_learning, summarize_outreach_learning
from runtime.contact_discovery import get_all_contacts
from runtime.safety.control_plane import list_action_envelopes, list_security_incidents

engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

_GMAIL_DISTRESS_NOISE_RE = re.compile(
    r"\b(send resume|recruitment|apply now|job|hiring|perfect new card|all dressed up|% off|sale)\b",
    re.IGNORECASE,
)


def _safe_latest_signal_row(conn, merchant_id: int, *, mode: str) -> dict:
    if mode == "direct":
        query = text(
            """
            SELECT s.id AS signal_id, s.content
            FROM signals s
            WHERE s.merchant_id = :merchant_id
            ORDER BY s.detected_at DESC
            LIMIT 1
            """
        )
    else:
        query = text(
            """
            SELECT s.id AS signal_id, s.content
            FROM signals s
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
        return dict(row or {})
    except SQLAlchemyError as exc:
        lowered = str(exc).lower()
        if "deadlock detected" in lowered or "lock timeout" in lowered:
            return {}
        raise


def _latest_signal_row_for_merchant(conn, merchant_id: int) -> dict:
    direct = _safe_latest_signal_row(conn, merchant_id, mode="direct")
    if direct:
        return direct
    return _safe_latest_signal_row(conn, merchant_id, mode="linked")


def _coerce_cached_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def get_today_signals():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT source, content, detected_at
            FROM signals
            WHERE detected_at > CURRENT_DATE
            ORDER BY id DESC LIMIT 20
        """))
        return [dict(row._mapping) for row in rows]


def get_signal_summary():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT topic, summary, created_at
            FROM knowledge
            WHERE topic = 'merchant_signal_summary'
            ORDER BY id DESC LIMIT 1
        """))
        results = [dict(row._mapping) for row in rows]
        return results[0] if results else None


def get_system_stats():
    with engine.connect() as conn:
        tasks = conn.execute(text("SELECT COUNT(*) as c FROM tasks")).scalar()
        signals = conn.execute(text("SELECT COUNT(*) as c FROM signals")).scalar()
        events = conn.execute(text("SELECT COUNT(*) as c FROM events")).scalar()
        return {"total_tasks": tasks, "total_signals": signals, "total_events": events}


def get_system_status():
    """Comprehensive system status for operator queries."""
    last_scan = r.get("last_scan_time") or "unknown"

    with engine.connect() as conn:
        # Pipeline metrics
        signals_24h = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '24 hours'"
        )).scalar() or 0

        signals_1h = conn.execute(text(
            "SELECT COUNT(*) FROM signals WHERE detected_at >= NOW() - INTERVAL '1 hour'"
        )).scalar() or 0

        # Cluster health
        clusters = conn.execute(text(
            "SELECT cluster_topic, cluster_size FROM clusters ORDER BY cluster_size DESC LIMIT 5"
        )).fetchall()

        total_clusters = conn.execute(text("SELECT COUNT(*) FROM clusters")).scalar() or 0

        # Investigations
        investigations = conn.execute(text(
            "SELECT COUNT(*) FROM cluster_intelligence WHERE created_at >= NOW() - INTERVAL '24 hours'"
        )).scalar() or 0

        # Processor anomalies
        proc_anomalies = conn.execute(text("""
            SELECT processor, COUNT(*) FROM qualified_leads
            WHERE created_at >= NOW() - INTERVAL '24 hours'
              AND processor IS NOT NULL AND processor != 'unknown'
            GROUP BY processor ORDER BY COUNT(*) DESC LIMIT 5
        """)).fetchall()

        # Top merchants
        merchants = conn.execute(text("""
            SELECT COALESCE(m.canonical_name, m.domain, s.content, 'unknown') AS merchant_name,
                   q.processor,
                   q.qualification_score
            FROM qualified_leads q
            LEFT JOIN signals s ON s.id = q.signal_id
            LEFT JOIN merchants m ON m.id = s.merchant_id
            WHERE q.created_at >= NOW() - INTERVAL '24 hours'
              AND COALESCE(m.canonical_name, m.domain, s.content) IS NOT NULL
            ORDER BY q.qualification_score DESC LIMIT 5
        """)).fetchall()

        # Leads
        leads_24h = conn.execute(text(
            "SELECT COUNT(*) FROM qualified_leads WHERE created_at >= NOW() - INTERVAL '24 hours'"
        )).scalar() or 0

    payflux_metrics = get_payflux_intelligence_metrics()
    telegram_delivery = get_telegram_delivery_health()
    return {
        "last_scan_time": last_scan,
        "signals_24h": signals_24h,
        "signals_1h": signals_1h,
        "active_clusters": total_clusters,
        "top_clusters": [{"topic": c[0], "size": c[1]} for c in clusters],
        "investigations_24h": investigations,
        "qualified_leads_24h": leads_24h,
        "shopify_community_threads_seen_24h": payflux_metrics.get("shopify_community_threads_seen_24h", 0),
        "shopify_community_signals_created_24h": payflux_metrics.get("shopify_community_signals_created_24h", 0),
        "stack_overflow_questions_seen_24h": payflux_metrics.get("stack_overflow_questions_seen_24h", 0),
        "stack_overflow_signals_created_24h": payflux_metrics.get("stack_overflow_signals_created_24h", 0),
        "source_domains_extracted_24h": payflux_metrics.get("source_domains_extracted_24h", 0),
        "source_ingestion_errors_24h": payflux_metrics.get("source_ingestion_errors_24h", 0),
        "shopify_community_top_distress_24h": payflux_metrics.get("shopify_community_top_distress_24h", ""),
        "shopify_community_signals_filtered_24h": payflux_metrics.get("shopify_community_signals_filtered_24h", 0),
        "shopify_community_rate_limited_24h": payflux_metrics.get("shopify_community_rate_limited_24h", 0),
        "shopify_community_duplicate_skips_24h": payflux_metrics.get("shopify_community_duplicate_skips_24h", 0),
        "shopify_community_domainless_signals_24h": payflux_metrics.get("shopify_community_domainless_signals_24h", 0),
        "shopify_community_merchant_matched_24h": payflux_metrics.get("shopify_community_merchant_matched_24h", 0),
        "shopify_community_opportunities_created_24h": payflux_metrics.get("shopify_community_opportunities_created_24h", 0),
        "shopify_community_outreach_eligible_24h": payflux_metrics.get("shopify_community_outreach_eligible_24h", 0),
        "shopify_community_blocked_24h": payflux_metrics.get("shopify_community_blocked_24h", 0),
        "shopify_community_board_summary": payflux_metrics.get("shopify_community_board_summary", ""),
        "shopify_community_funnel_summary": payflux_metrics.get("shopify_community_funnel_summary", ""),
        "stack_overflow_top_distress_24h": payflux_metrics.get("stack_overflow_top_distress_24h", ""),
        "source_health_summary": payflux_metrics.get("source_health_summary", ""),
        "telegram_delivery": telegram_delivery,
        "processor_anomalies": [{"processor": p[0], "count": p[1]} for p in proc_anomalies],
        "top_distressed_merchants": [{"name": m[0], "processor": m[1] or "unknown", "score": m[2]} for m in merchants],
        "pipeline_schedule": {
            "autonomous_market_cycle": "every 30 min",
            "cluster_investigation": "every 20 min",
            "processor_discovery": "every 30 min",
            "distress_radar": "every 60 min",
            "failure_forecast": "every 60 min"
        }
    }


def get_top_merchants(limit=10):
    """Return top distressed merchants ranked by distress_score."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT m.canonical_name, m.domain, m.distress_score,
                   (SELECT COUNT(*) FROM merchant_signals ms WHERE ms.merchant_id = m.id) as signal_count
            FROM merchants m
            WHERE m.distress_score > 0
            ORDER BY m.distress_score DESC
            LIMIT :lim
        """), {"lim": limit}).fetchall()

    return [{
        "name": r[0],
        "domain": r[1] or "",
        "distress_score": round(r[2], 1),
        "signal_count": r[3],
    } for r in rows]


def list_deal_opportunities_command(limit: int = 10):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, merchant_id, merchant_domain, processor, distress_topic,
                   sales_strategy, status, created_at
            FROM merchant_opportunities
            ORDER BY created_at DESC
            LIMIT :lim
        """), {"lim": limit}).fetchall()
        output = []
        for r in rows:
            merchant = conn.execute(
                text(
                    """
                    SELECT canonical_name, domain, normalized_domain, domain_confidence
                    FROM merchants
                    WHERE id = :merchant_id
                    LIMIT 1
                    """
                ),
                {"merchant_id": r[1] or -1},
            ).mappings().first()
            signal = _latest_signal_row_for_merchant(conn, int(r[1] or -1))
            quality = evaluate_opportunity_queue_quality(
                opportunity={
                    "id": r[0],
                    "merchant_id": r[1],
                    "merchant_domain": r[2],
                    "processor": r[3],
                    "distress_topic": r[4],
                    "sales_strategy": r[5],
                },
                merchant=dict(merchant or {}),
                signal=dict(signal or {}),
            )
            output.append({
                "id": r[0],
                "merchant_id": r[1],
                "merchant_domain": r[2],
                "processor": r[3],
                "distress_topic": r[4],
                "sales_strategy": r[5],
                "status": r[6],
                "created_at": r[7].isoformat() if r[7] else None,
                "eligibility_class": quality.get("eligibility_class") or "",
                "eligibility_reason": quality.get("eligibility_reason") or "",
                "queue_quality_score": int(quality.get("quality_score") or 0),
            })
    return {
        "count": len(output),
        "deal_opportunities": output,
    }

def get_merchant_contacts(merchant_id: int):
    """Return discovered contacts for a merchant for the Telegram command."""
    with engine.connect() as conn:
        merchant = conn.execute(text("SELECT canonical_name, domain FROM merchants WHERE id = :mid"), {"mid": merchant_id}).fetchone()
        if not merchant:
            return None
    contacts = get_all_contacts(merchant_id)
    return {
        "merchant": merchant[0],
        "domain": merchant[1],
        "contacts": [
            {
                "name": contact.get("contact_name") or "",
                "email": contact.get("email") or "",
                "linkedin": contact.get("linkedin_url") or "",
                "confidence": round(float(contact.get("confidence") or 0.0), 2),
                "source": contact.get("source") or "",
                "page_type": contact.get("page_type") or "",
                "page_url": contact.get("page_url") or "",
                "role_hint": contact.get("role_hint") or "",
                "email_verified": bool(contact.get("email_verified")),
            }
            for contact in contacts
        ],
    }

def get_similar_signals(query: str, limit: int = 5):
    """Return top semantically similar signals for the operator."""
    from runtime.intelligence.semantic_memory import retrieve_similar_signals
    results = retrieve_similar_signals(query, limit=limit)
    return results

def fix_pipeline():
    """Trigger the pipeline diagnostics engine to self-heal any detected anomalies."""
    from runtime.ops.pipeline_diagnostics import run_pipeline_diagnostics
    results = run_pipeline_diagnostics()
    return results


def list_reddit_candidates_command(query: str = "", limit: int = 10):
    return list_reddit_candidates(query=query, limit=limit)


def draft_reddit_reply_command(target_id: str, permalink: str = ""):
    return draft_reddit_reply(target_id=target_id, permalink=permalink)


def send_reddit_reply_command(
    target_id: str,
    permalink: str = "",
    human_approved: bool = False,
    approved_by: str = "",
    approval_source: str = "operator_command",
):
    return send_reddit_reply(
        target_id=target_id,
        permalink=permalink,
        human_approved=human_approved,
        approved_by=approved_by,
        approval_source=approval_source,
    )


def list_gmail_threads_command(query: str = "label:INBOX", limit: int = 10):
    return list_gmail_threads(query=query, limit=limit)


def draft_gmail_reply_command(thread_id: str):
    return draft_gmail_reply(thread_id=thread_id)


def draft_gmail_distress_reply_command(thread_id: str):
    return draft_gmail_distress_reply(thread_id=thread_id)


def send_gmail_reply_command(
    thread_id: str,
    human_approved: bool = False,
    approved_by: str = "",
    approval_source: str = "operator_command",
):
    return send_gmail_reply(
        thread_id=thread_id,
        human_approved=human_approved,
        approved_by=approved_by,
        approval_source=approval_source,
    )


def list_gmail_triage_command(limit: int = 25):
    return list_gmail_triage(limit=limit)


def list_gmail_merchant_distress_command(limit: int = 25):
    return list_gmail_merchant_distress(limit=limit)


def _humanize_filter_reason(reason: str) -> str:
    mapping = {
        "consumer_noise": "the signal reads like consumer noise, not merchant-operator distress",
        "historical_or_hypothetical": "the signal reads as historical or hypothetical instead of active processor pressure",
        "generic_tooling_mismatch": "the signal looks like generic tooling or growth pain rather than processor distress",
        "queue class research_only": "the case is still research-only and not ready for frontline outreach",
        "queue class operator_review_only": "the case still needs operator review before it should enter frontline outreach",
        "queue class identity_weak": "the merchant identity is too weak to trust for outreach",
        "queue class consumer_noise": "the case was filtered as consumer noise",
        "queue class domain_unpromotable": "the domain is not promotable as a merchant prospect",
        "queue class outreach_eligible": "the case is eligible for outreach",
        "no trusted operator contact path": "there is no trusted merchant contact path yet",
        "processor and distress are both unknown": "both the processor and distress type are still unknown",
        "icp fit is too weak": "the case is not a strong enough PayFlux fit yet",
        "best play is clarify_distress without strong signal quality": "the signal is too weak to justify a clarify-distress outreach move yet",
        "not yet a high-conviction PayFlux prospect": "the case is not yet a high-conviction PayFlux prospect",
        "queue quality score is too low": "the queue quality score is still too low",
    }
    return mapping.get(reason or "", str(reason or "unknown").replace("_", " "))


def _humanize_reason_code(code: str) -> str:
    mapping = {
        "clear_merchant_payment_distress": "clear live merchant payment distress",
        "merchant_distress_needs_operator_review": "merchant distress is plausible but still needs operator review",
        "merchant_distress_not_outreach_ready": "merchant distress exists but is not outreach-ready yet",
        "insufficient_merchant_distress": "not enough merchant distress signal",
        "consumer_language_detected": "consumer complaint language is present",
        "merchant_domain_missing_or_invalid": "merchant domain is missing or invalid",
        "merchant_identity_too_weak": "merchant identity is too weak",
        "provisional_identity_candidate": "merchant identity is still provisional",
        "icp_fit_requires_operator_review": "ICP fit is weak enough that operator review is required",
        "icp_fit_too_weak_for_outreach": "ICP fit is too weak for outreach",
    }
    return mapping.get(code or "", str(code or "unknown").replace("_", " "))


def _screen_gmail_distress_thread(row: dict | None) -> dict:
    row = dict(row or {})
    reasons: list[str] = []
    confidence = float(row.get("confidence") or 0.0)
    processor = str(row.get("processor") or "unknown").strip().lower()
    opportunity_eligible = bool(row.get("opportunity_eligible"))
    distress_signals = [str(item).strip().lower() for item in (row.get("distress_signals") or []) if str(item).strip()]
    metadata = dict(row.get("metadata") or {})
    subject = str(row.get("subject") or "")
    snippet = str(row.get("snippet") or "")
    sender_domain = str(row.get("sender_domain") or "")
    text_blob = " ".join([subject, snippet, sender_domain]).strip()

    if confidence < 0.85:
        reasons.append("confidence below 0.85")
    if not opportunity_eligible:
        reasons.append("not marked opportunity-eligible")
    if _GMAIL_DISTRESS_NOISE_RE.search(text_blob):
        reasons.append("looks like job, promo, or other inbox noise")
    if metadata.get("human_written") is False:
        reasons.append("message is not human-written")
    if processor == "unknown" and (not distress_signals or distress_signals == ["unknown"]):
        reasons.append("processor is unknown and distress signals are too weak")

    return {
        "actionable": not reasons,
        "reasons": reasons,
        "screen_summary": "actionable merchant-distress thread" if not reasons else "; ".join(reasons),
    }


def show_gmail_distress_screen_command(limit: int = 5) -> dict:
    rows = list_gmail_merchant_distress_command(limit=max(1, int(limit))).get("threads", [])
    screened = []
    actionable = 0
    for row in rows:
        screen = _screen_gmail_distress_thread(row)
        if screen["actionable"]:
            actionable += 1
        screened.append(
            {
                "subject": row.get("subject") or "",
                "sender_domain": row.get("sender_domain") or "",
                "confidence": float(row.get("confidence") or 0.0),
                "processor": row.get("processor") or "unknown",
                "distress_type": row.get("distress_type") or "unknown",
                "opportunity_eligible": bool(row.get("opportunity_eligible")),
                "actionable": bool(screen.get("actionable")),
                "screen_summary": screen.get("screen_summary") or "",
                "reasons": list(screen.get("reasons") or []),
            }
        )
    return {
        "count": len(screened),
        "actionable_count": actionable,
        "screened_threads": screened,
    }


def show_recent_suppressed_leads_command(limit: int = 5) -> dict:
    rows_out = []
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT event_type, data, created_at
                FROM events
                WHERE event_type IN ('lead_qualification_suppressed', 'opportunity_extraction_suppressed')
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            {"limit": max(1, int(limit))},
        ).mappings().fetchall()
    for row in rows:
        payload = row.get("data") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {"raw": payload}
        rows_out.append(
            {
                "stage": "lead qualification" if row.get("event_type") == "lead_qualification_suppressed" else "opportunity extraction",
                "signal_id": payload.get("signal_id"),
                "qualification_score": payload.get("qualification_score"),
                "icp_fit_score": payload.get("icp_fit_score"),
                "icp_fit_label": payload.get("icp_fit_label") or "",
                "disqualifier_reason": payload.get("disqualifier_reason") or "",
                "content_preview": payload.get("content_preview") or "",
                "created_at": row.get("created_at"),
            }
        )
    return {"count": len(rows_out), "cases": rows_out}


def show_opportunity_fit_command(opportunity_id: int | None = None, merchant_domain: str = "") -> dict:
    context = get_selected_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        merchant_domain=merchant_domain or None,
    )
    if context.get("error"):
        return context
    operator_ready, block_reason = _operator_ready_context(context)
    queue_class = str(context.get("queue_eligibility_class") or "")
    raw_queue_reason_codes = list(context.get("queue_reason_codes") or [])
    if not raw_queue_reason_codes and queue_class:
        raw_queue_reason_codes = [queue_class]
    reason_codes = [_humanize_reason_code(code) for code in raw_queue_reason_codes if str(code).strip()]
    contact_reason_codes = [_humanize_reason_code(code) for code in (context.get("contact_reason_codes") or []) if str(code).strip()]
    return {
        "status": "ok",
        "opportunity_id": int(context.get("opportunity_id") or 0),
        "merchant_name": context.get("merchant_name_display") or context.get("merchant_name") or "",
        "merchant_domain": context.get("merchant_domain") or "",
        "processor": context.get("processor") or "unknown",
        "distress_type": context.get("distress_type") or "unknown",
        "queue_eligibility_class": queue_class or "unknown",
        "queue_eligibility_reason": context.get("queue_eligibility_reason") or "",
        "queue_quality_score": int(context.get("queue_quality_score") or 0),
        "queue_reason_codes": reason_codes,
        "icp_fit_score": int(context.get("icp_fit_score") or 0),
        "icp_fit_label": context.get("icp_fit_label") or "",
        "icp_fit_reason": context.get("icp_fit_reason") or "",
        "high_conviction_prospect": bool(context.get("high_conviction_prospect")),
        "contact_email": context.get("contact_email") or "",
        "contact_quality_label": context.get("contact_quality_label") or "",
        "contact_trust_score": int(context.get("contact_trust_score") or 0),
        "contact_reason": context.get("contact_reason") or "",
        "contact_reason_codes": contact_reason_codes,
        "operator_ready": bool(operator_ready),
        "operator_block_reason": "" if operator_ready else _humanize_filter_reason(block_reason),
        "outreach_status": context.get("outreach_status") or context.get("current_status") or "no_outreach",
        "approval_state": context.get("approval_state") or "",
        "why_now": context.get("why_now") or "",
    }


def create_gmail_signal_command(thread_id: str):
    return create_gmail_signal(thread_id=thread_id)


def show_gmail_thread_intelligence_command(thread_id: str):
    return show_gmail_thread_intelligence(thread_id=thread_id)


def run_gmail_triage_cycle_command(query: str = "label:INBOX newer_than:30d", limit: int = 15):
    return run_gmail_triage_cycle(query=query, limit=limit)


def show_channel_audit_log_command(channel: str = "", limit: int = 25):
    return show_channel_audit_log(channel=channel, limit=limit)


def set_channel_mode_command(channel: str, mode: str):
    return set_channel_mode(channel=channel, mode=mode)


def set_channel_kill_switch_command(channel: str, enabled: bool):
    return set_channel_kill_switch(channel=channel, enabled=enabled)


def show_reasoning_decision_log_command(limit: int = 25, task_type: str = "", item_id: str = ""):
    return show_reasoning_decision_log(limit=limit, task_type=task_type, item_id=item_id)


def show_reasoning_decision_command(decision_id: int):
    return show_reasoning_decision(decision_id)


def rerun_reasoning_task_command(task_type: str, item_id: str):
    return rerun_reasoning_task(task_type, item_id)


def show_reasoning_metrics_command():
    return show_reasoning_metrics()


def _latest_reasoning_decision(*, task_type: str, item_id: str) -> dict | None:
    rows = list_reasoning_decisions(limit=1, task_type=task_type, item_id=item_id)
    return rows[0] if rows else None


def show_reasoning_scoreboard_command() -> dict:
    metrics = show_reasoning_metrics()
    provider_rows = sorted(
        metrics.get("reasoning_provider_performance_24h", []),
        key=lambda row: (
            -int(row.get("total_calls") or 0),
            float(row.get("avg_latency_ms") or 0.0),
        ),
    )
    opportunity_rows = list_reasoning_decisions(limit=10, task_type="opportunity_scoring_refinement")
    latest_opportunity = next((row for row in opportunity_rows if str(row.get("item_id") or "").isdigit()), None)
    if latest_opportunity is None and opportunity_rows:
        latest_opportunity = opportunity_rows[0]
    return {
        "reasoning_status": metrics.get("reasoning_status") or "unknown",
        "tier1_calls_24h": int(metrics.get("reasoning_tier1_calls_24h") or 0),
        "tier2_calls_24h": int(metrics.get("reasoning_tier2_calls_24h") or 0),
        "rule_only_24h": int(metrics.get("reasoning_rule_only_decisions_24h") or 0),
        "failures_24h": int(metrics.get("reasoning_failures_24h") or 0),
        "fallbacks_24h": int(metrics.get("reasoning_fallbacks_24h") or 0),
        "avg_latency_ms": float(metrics.get("avg_reasoning_latency_ms") or 0.0),
        "last_reasoning_error": metrics.get("last_reasoning_error") or "",
        "top_providers": provider_rows[:3],
        "latest_opportunity_refinement": {
            "decision_id": latest_opportunity.get("id"),
            "item_id": latest_opportunity.get("item_id"),
            "provider": latest_opportunity.get("provider") or "",
            "model": latest_opportunity.get("model") or "",
            "routing_decision": latest_opportunity.get("routing_decision") or "",
            "success": bool(latest_opportunity.get("success")),
            "error": latest_opportunity.get("error") or "",
            "latency_ms": float(latest_opportunity.get("latency_ms") or 0.0),
            "created_at": latest_opportunity.get("created_at") or "",
        } if latest_opportunity else None,
    }


def show_opportunity_reasoning_command(opportunity_id: int) -> dict:
    context = get_outreach_context_snapshot(opportunity_id=int(opportunity_id))
    if not context:
        return {"error": "Opportunity not found"}
    with engine.connect() as conn:
        row = _latest_signal_row_for_merchant(conn, int(context.get("merchant_id") or -1))
    signal_id = (row or {}).get("signal_id")
    if not signal_id:
        return {
            "error": "Opportunity has no linked signal",
            "opportunity_id": int(opportunity_id),
            "merchant_name": context.get("merchant_name_display") or context.get("merchant_name") or "",
            "merchant_domain": context.get("merchant_domain") or "",
        }
    decisions = list_reasoning_decisions(
        limit=3,
        task_type="opportunity_scoring_refinement",
        item_id=str(signal_id),
    )
    latest = decisions[0] if decisions else None
    validated = (latest or {}).get("validated_output_json") or {}
    final_decision = (latest or {}).get("final_decision_json") or {}
    return {
        "status": "ok",
        "opportunity_id": int(context.get("opportunity_id") or opportunity_id),
        "signal_id": int(signal_id),
        "merchant_name": context.get("merchant_name_display") or context.get("merchant_name") or "",
        "merchant_domain": context.get("merchant_domain") or "",
        "decision_count": len(decisions),
        "latest_decision": {
            "decision_id": latest.get("id"),
            "routing_decision": latest.get("routing_decision") or "",
            "provider": latest.get("provider") or "",
            "model": latest.get("model") or "",
            "success": bool(latest.get("success")),
            "error": latest.get("error") or "",
            "fallback_used": bool(latest.get("fallback_used")),
            "latency_ms": float(latest.get("latency_ms") or 0.0),
            "created_at": latest.get("created_at") or "",
            "validated_output": validated,
            "final_decision": final_decision,
            "model_contributions": sorted(validated.keys()),
        } if latest else None,
        "recent_decisions": [
            {
                "decision_id": row.get("id"),
                "routing_decision": row.get("routing_decision") or "",
                "provider": row.get("provider") or "",
                "model": row.get("model") or "",
                "success": bool(row.get("success")),
                "error": row.get("error") or "",
                "latency_ms": float(row.get("latency_ms") or 0.0),
                "created_at": row.get("created_at") or "",
            }
            for row in decisions
        ],
    }


def run_reasoning_evaluation_command():
    return run_reasoning_evaluation()


def list_distress_patterns_command(limit: int = 10):
    return list_distress_patterns(limit=limit)


def list_actionable_patterns_command(limit: int = 10):
    result = list_distress_patterns(limit=limit)
    patterns = [
        row
        for row in result.get("patterns", [])
        if row.get("actionable") or row.get("trend_label") == "rising"
    ]
    return {"count": len(patterns), "patterns": patterns[:limit]}


def list_merchant_profiles_command(limit: int = 10):
    return get_top_merchant_profiles(limit=limit)


def show_merchant_intelligence_profile_command(merchant_id: int | None = None, domain: str = ""):
    profile = get_merchant_intelligence_profile(merchant_id=merchant_id, domain=domain or None)
    return {"profile": profile} if profile else {"error": "Merchant profile not found"}


def show_conversion_intelligence_command(limit: int = 10):
    return get_conversion_intelligence(limit=limit)


def show_best_conversion_patterns_command(limit: int = 10):
    result = get_conversion_intelligence(limit=max(limit, 25))
    patterns = sorted(
        result.get("patterns", []),
        key=lambda row: (
            0 if row.get("wins", 0) > 0 else 1,
            -float(row.get("win_rate", 0.0)),
            -int(row.get("wins", 0)),
            -float(row.get("best_chance_score", 0.0)),
            -int(row.get("pending", 0)),
        ),
    )
    return {"count": min(len(patterns), limit), "patterns": patterns[:limit]}


def mark_opportunity_outcome_command(opportunity_id: int, outcome_status: str, outcome_reason: str = ""):
    return mark_opportunity_outcome(opportunity_id=opportunity_id, outcome_status=outcome_status, outcome_reason=outcome_reason or None)


def show_opportunity_operator_action_command(opportunity_id: int):
    return get_opportunity_operator_action(opportunity_id=opportunity_id)


def mark_opportunity_operator_action_command(opportunity_id: int, selected_action: str, action_reason: str = ""):
    return mark_opportunity_operator_action(
        opportunity_id=opportunity_id,
        selected_action=selected_action,
        action_reason=action_reason or None,
    )


def list_opportunities_by_operator_action_command(
    selected_action: str = "",
    unselected_only: bool = False,
    mismatched_only: bool = False,
    limit: int = 10,
):
    return list_opportunities_by_operator_action(
        selected_action=selected_action or None,
        unselected_only=bool(unselected_only),
        mismatched_only=bool(mismatched_only),
        limit=limit,
    )


def list_merchants_by_operator_action_command(
    selected_action: str = "",
    unselected_only: bool = False,
    mismatched_only: bool = False,
    limit: int = 10,
):
    return list_merchants_by_operator_action(
        selected_action=selected_action or None,
        unselected_only=bool(unselected_only),
        mismatched_only=bool(mismatched_only),
        limit=limit,
    )


def sync_payflux_intelligence_command():
    return sync_payflux_intelligence()


def get_outreach_recommendation_command(opportunity_id: int | None = None, merchant_domain: str = ""):
    return recommend_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        merchant_domain=merchant_domain or None,
    )


def get_contact_intelligence_command(opportunity_id: int | None = None, merchant_domain: str = ""):
    return get_contact_intelligence_for_opportunity(
        opportunity_id=opportunity_id,
        merchant_domain=merchant_domain or None,
    )


def draft_outreach_for_opportunity_command(
    opportunity_id: int | None = None,
    merchant_domain: str = "",
    outreach_type: str = "",
    allow_best_effort: bool = False,
):
    return draft_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        merchant_domain=merchant_domain or None,
        outreach_type=outreach_type or None,
        allow_best_effort=bool(allow_best_effort),
    )


def draft_follow_up_for_opportunity_command(opportunity_id: int, follow_up_type: str = "follow_up_nudge"):
    context = get_outreach_context_snapshot(opportunity_id=opportunity_id)
    resolved_type = follow_up_type or "follow_up_nudge"
    if context and str(context.get("outreach_status") or "").strip().lower() == "replied":
        resolved_type = "reply_follow_up"
    return draft_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        merchant_domain=None,
        outreach_type=resolved_type,
    )


def approve_outreach_for_opportunity_command(
    opportunity_id: int,
    notes: str = "",
    approved_by: str = "",
    approval_source: str = "operator_command",
):
    draft_view = show_local_outreach_draft_command(opportunity_id=opportunity_id)
    result = approve_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        notes=notes or "",
        approved_by=approved_by or "",
        approval_source=approval_source or "operator_command",
    )
    if result.get("error"):
        return result
    draft = (draft_view or {}).get("draft") or {}
    context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
    user_id = get_primary_operator_user_id()
    if draft and user_id:
        result["critic_review"] = review_outreach_draft(user_id, draft=draft, context=context or {})
        result["critic_rewrite_preview"] = _build_critic_rewrite_preview(
            opportunity_id=opportunity_id,
            critic_review=result["critic_review"],
        )
    return result


def rewrite_outreach_draft_command(
    opportunity_id: int,
    style: str = "",
    instructions: str = "",
    rewritten_by: str = "",
    rewrite_source: str = "operator_command",
):
    return rewrite_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        style=style or "",
        instructions=instructions or "",
        rewritten_by=rewritten_by or "",
        rewrite_source=rewrite_source or "operator_command",
    )


def apply_critic_rewrite_command(opportunity_id: int):
    draft_view = show_local_outreach_draft_command(opportunity_id=opportunity_id)
    draft = (draft_view or {}).get("draft") or {}
    critic_review = (draft_view or {}).get("critic_review") or {}
    if not draft or not critic_review:
        return {"error": "Draft critic review is not available for this opportunity"}
    preview = _build_critic_rewrite_preview(opportunity_id=opportunity_id, critic_review=critic_review)
    if preview.get("error"):
        return preview
    return rewrite_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        style=preview.get("rewrite_style") or "sharper",
        instructions=preview.get("instructions") or "",
    )


def send_outreach_for_opportunity_command(
    opportunity_id: int,
    approved_by: str = "",
    approval_source: str = "operator_command",
):
    draft_view = show_local_outreach_draft_command(opportunity_id=opportunity_id)
    result = send_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        approved_by=approved_by or "",
        approval_source=approval_source or "operator_command",
    )
    if result.get("error"):
        return result
    draft = (draft_view or {}).get("draft") or {}
    context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
    user_id = get_primary_operator_user_id()
    if draft and user_id:
        result["critic_review"] = review_outreach_draft(user_id, draft=draft, context=context or {})
    return result


def mark_outreach_outcome_command(opportunity_id: int, outcome_status: str, notes: str = ""):
    return update_outreach_outcome(
        opportunity_id=opportunity_id,
        outcome_status=outcome_status,
        notes=notes or "",
    )


def list_outreach_awaiting_approval_command(limit: int = 10):
    return list_outreach_awaiting_approval(limit=limit)


def show_local_outreach_drafts_command(limit: int = 5):
    return list_outreach_awaiting_approval(limit=max(1, int(limit)))


def show_local_outreach_draft_command(opportunity_id: int | None = None):
    if opportunity_id:
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
            return {"error": "Local outreach draft not found", "opportunity_id": int(opportunity_id)}
        record = dict(row)
        metadata = record.get("metadata_json")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        context = get_selected_outreach_for_opportunity(opportunity_id=int(opportunity_id))
        user_id = get_primary_operator_user_id()
        critic_review = review_outreach_draft(user_id, draft={
            "opportunity_id": int(record.get("opportunity_id") or 0),
            "merchant_domain": record.get("merchant_domain") or "",
            "contact_email": record.get("contact_email") or "",
            "selected_play": record.get("selected_play") or "",
            "outreach_type": record.get("outreach_type") or "",
            "subject": record.get("subject") or "",
            "body": record.get("body") or "",
            "status": record.get("status") or "",
            "approval_state": record.get("approval_state") or "",
            "why_now": (context or {}).get("why_now") or "",
        }, context=context or {}) if user_id else {}
        preview = _build_critic_rewrite_preview(
            opportunity_id=int(opportunity_id),
            critic_review=critic_review,
        ) if critic_review else {}
        return {
            "status": "ok",
            "draft": {
                "opportunity_id": int(record.get("opportunity_id") or 0),
                "merchant_domain": record.get("merchant_domain") or "",
                "contact_email": record.get("contact_email") or "",
                "contact_name": record.get("contact_name") or "",
                "channel": record.get("channel") or "gmail",
                "selected_play": record.get("selected_play") or "",
                "outreach_type": record.get("outreach_type") or "",
                "subject": record.get("subject") or "",
                "body": record.get("body") or "",
                "status": record.get("status") or "",
                "approval_state": record.get("approval_state") or "",
                "draft_message_id": record.get("draft_message_id") or "",
                "gmail_thread_id": record.get("gmail_thread_id") or "",
                "gmail_draft_error": metadata.get("gmail_draft_error") or "",
                "updated_at": str(record.get("updated_at") or ""),
                "why_now": context.get("why_now") or "",
                "queue_quality_score": int(context.get("queue_quality_score") or 0),
                "contact_quality_label": context.get("contact_quality_label") or "",
                "contact_trust_score": int(context.get("contact_trust_score") or 0),
            },
            "critic_review": critic_review,
            "critic_rewrite_preview": preview,
        }

    drafts = show_local_outreach_drafts_command(limit=1)
    cases = list(drafts.get("cases") or [])
    if not cases:
        return {"status": "empty", "count": 0, "draft": None}
    return show_local_outreach_draft_command(opportunity_id=int(cases[0].get("opportunity_id") or 0))


def list_sent_outreach_needing_follow_up_command(limit: int = 10):
    return list_sent_outreach_needing_follow_up(limit=limit)


def list_send_eligible_outreach_leads_command(limit: int = 10):
    return list_send_eligible_outreach_leads(limit=limit)


def list_blocked_outreach_leads_command(limit: int = 10):
    return list_blocked_outreach_leads(limit=limit)


def show_sendable_queue_command(limit: int = 10):
    return list_send_eligible_outreach_leads(limit=max(1, int(limit)))


def show_contact_blocked_queue_command(limit: int = 10):
    return list_blocked_outreach_leads(limit=max(1, int(limit)))


def advance_contact_acquisition_command(limit: int = 10):
    blocked = show_contact_blocked_queue_command(limit=max(1, int(limit)))
    cases = list(blocked.get("cases") or [])
    top = cases[0] if cases else {}
    opportunity_id = int(top.get("opportunity_id") or 0)
    if not opportunity_id:
        return {
            "status": "blocked",
            "reason": "no_contact_blocked_case",
            "queue_snapshot": blocked,
        }

    context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
    if context.get("error"):
        return {
            "status": "blocked",
            "reason": "contact_context_unavailable",
            "queue_snapshot": blocked,
            "error": context.get("error"),
        }

    merchant_id = int(context.get("merchant_id") or 0)
    distress_type = str(context.get("distress_type") or "").strip()
    selected_action = str(context.get("selected_play") or context.get("recommended_play") or "").strip()
    if merchant_id <= 0:
        return {
            "status": "blocked",
            "reason": "missing_merchant_id",
            "queue_snapshot": blocked,
            "opportunity_id": opportunity_id,
        }

    try:
        from runtime.intelligence.contact_discovery import discover_contacts_for_merchant
    except ImportError:
        from intelligence.contact_discovery import discover_contacts_for_merchant

    created = int(
        discover_contacts_for_merchant(
            merchant_id,
            opportunity_id=opportunity_id,
            distress_topic=distress_type,
            selected_action=selected_action,
        )
        or 0
    )
    refreshed = get_contact_intelligence_for_opportunity(opportunity_id=opportunity_id)
    save_event(
        "contact_acquisition_advanced",
        {
            "opportunity_id": opportunity_id,
            "merchant_id": merchant_id,
            "merchant_domain": context.get("merchant_domain") or "",
            "created": created,
            "contact_state": refreshed.get("contact_state") or "",
            "contact_email": refreshed.get("contact_email") or "",
            "contact_send_eligible": bool(refreshed.get("contact_send_eligible")),
        },
    )
    return {
        "status": "ok",
        "opportunity_id": opportunity_id,
        "merchant_id": merchant_id,
        "merchant_domain": context.get("merchant_domain") or "",
        "created": created,
        "contact_state": refreshed.get("contact_state") or "",
        "contact_email": refreshed.get("contact_email") or "",
        "contact_send_eligible": bool(refreshed.get("contact_send_eligible")),
        "contact_reason": refreshed.get("contact_reason") or "",
        "contact_deepening_summary": refreshed.get("contact_deepening_summary") or "",
        "next_contact_move": refreshed.get("next_contact_move") or "",
        "queue_snapshot": blocked,
    }


def show_selected_outreach_command(opportunity_id: int | None = None, merchant_domain: str = ""):
    return get_selected_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        merchant_domain=merchant_domain or None,
    )


def show_opportunity_workbench_command(opportunity_id: int | None = None, merchant_domain: str = ""):
    context = get_selected_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        merchant_domain=merchant_domain or None,
    )
    if context.get("error"):
        return context
    draft = show_local_outreach_draft_command(opportunity_id=int(context.get("opportunity_id") or 0))
    learning = get_outreach_learning_signal(
        selected_play=context.get("selected_play") or context.get("recommended_play") or "",
        processor=context.get("processor") or "",
        distress_type=context.get("distress_type") or "",
    )
    return {
        "status": "ok",
        "opportunity_id": int(context.get("opportunity_id") or 0),
        "merchant_name": context.get("merchant_name_display") or context.get("merchant_name") or "",
        "merchant_domain": context.get("merchant_domain") or "",
        "processor": context.get("processor") or "unknown",
        "distress_type": context.get("distress_type") or "unknown",
        "selected_play": context.get("selected_play") or context.get("recommended_play") or "",
        "queue_quality_score": int(context.get("queue_quality_score") or 0),
        "queue_eligibility_reason": context.get("queue_eligibility_reason") or "",
        "contact_email": context.get("contact_email") or "",
        "contact_quality_label": context.get("contact_quality_label") or "",
        "contact_trust_score": int(context.get("contact_trust_score") or 0),
        "contact_reason": context.get("contact_reason") or "",
        "why_now": context.get("why_now") or "",
        "next_contact_move": context.get("next_contact_move") or "",
        "outreach_status": context.get("current_status") or context.get("outreach_status") or "no_outreach",
        "approval_state": context.get("approval_state") or "",
        "outcome_status": context.get("outcome_status") or "pending",
        "learning_signal": learning,
        "reasoning": show_opportunity_reasoning_command(int(context.get("opportunity_id") or 0)),
        "draft": draft.get("draft") if draft.get("status") == "ok" else None,
    }


def show_outreach_metrics_command():
    return get_outreach_execution_metrics(refresh_state=False)


def show_revenue_scoreboard_command(refresh: bool = True):
    heartbeat = get_value_heartbeat(refresh=bool(refresh))
    queue = show_top_queue_opportunities_command(limit=3)
    learning = summarize_outreach_learning(limit=25)
    execution = get_latest_execution_plan(refresh=False)
    brain = get_latest_brain_state(refresh=False)
    critic = show_latest_brain_review(refresh=False)
    return {
        "heartbeat": heartbeat,
        "top_queue": queue,
        "learning": learning,
        "execution": execution,
        "brain": brain,
        "critic": critic,
        "reasoning": show_reasoning_scoreboard_command(),
    }


def _build_critic_rewrite_preview(*, opportunity_id: int, critic_review: dict) -> dict:
    if not critic_review:
        return {}
    sharpen = [str(item).strip() for item in (critic_review.get("sharpen") or []) if str(item).strip()]
    if not sharpen:
        return {}
    instructions = " ".join(sharpen[:3])
    return preview_rewrite_outreach_for_opportunity(
        opportunity_id=opportunity_id,
        style="sharper",
        instructions=instructions,
    )


def show_outreach_learning_command(limit: int = 10):
    return list_outreach_learning(limit=max(1, int(limit)))


def show_reply_review_queue_command(limit: int = 5):
    rows = list_sent_outreach_needing_follow_up(limit=max(10, int(limit) * 3)).get("cases", [])
    cases = []
    for row in rows:
        if str(row.get("status") or "").strip().lower() != "replied":
            continue
        thread_id = row.get("gmail_thread_id") or ""
        intelligence = get_gmail_thread_intelligence(thread_id) if thread_id else None
        suggestion = _suggest_reply_review(row, intelligence or {})
        cases.append(
            {
                "opportunity_id": int(row.get("opportunity_id") or 0),
                "merchant_domain": row.get("merchant_domain") or "",
                "contact_email": row.get("contact_email") or "",
                "gmail_thread_id": thread_id,
                "reply_received_at": row.get("replied_at") or "",
                "selected_play": row.get("selected_play") or "",
                "thread_category": (intelligence or {}).get("thread_category") or "",
                "reply_priority": (intelligence or {}).get("reply_priority") or "",
                "snippet": (intelligence or {}).get("snippet") or "",
                "reply_intent": ((intelligence or {}).get("metadata") or {}).get("reply_intent") or "",
                "buying_intent": ((intelligence or {}).get("metadata") or {}).get("buying_intent") or "",
                "reply_intent_reason": ((intelligence or {}).get("metadata") or {}).get("reply_intent_reason") or "",
                "suggested_next_move": suggestion.get("suggested_next_move") or "",
                "suggested_outcome": suggestion.get("suggested_outcome") or "pending",
                "suggestion_reason": suggestion.get("reason") or "",
                "confidence": suggestion.get("confidence") or 0.0,
            }
        )
        if len(cases) >= max(1, int(limit)):
            break
    return {"count": len(cases), "cases": cases}


def show_reply_review_command(opportunity_id: int):
    context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
    if context.get("error"):
        return context
    thread_id = context.get("gmail_thread_id") or ""
    intelligence = get_gmail_thread_intelligence(thread_id) if thread_id else None
    suggestion = _suggest_reply_review(context, intelligence or {})
    reply_learning = get_outreach_learning_signal(
        selected_play=context.get("selected_play") or context.get("recommended_play") or "",
        processor=context.get("processor") or "",
        distress_type=context.get("distress_type") or "",
        outreach_type="reply_follow_up",
    )
    return {
        "status": "ok",
        "opportunity_id": int(context.get("opportunity_id") or 0),
        "merchant_domain": context.get("merchant_domain") or "",
        "contact_email": context.get("contact_email") or "",
        "gmail_thread_id": thread_id,
        "outreach_status": context.get("outreach_status") or "",
        "selected_play": context.get("selected_play") or context.get("recommended_play") or "",
        "reply_received_at": context.get("replied_at") or "",
        "thread_intelligence": intelligence or {},
        "reply_intent": ((intelligence or {}).get("metadata") or {}).get("reply_intent") or "",
        "buying_intent": ((intelligence or {}).get("metadata") or {}).get("buying_intent") or "",
        "reply_intent_reason": ((intelligence or {}).get("metadata") or {}).get("reply_intent_reason") or "",
        "suggestion": suggestion,
        "reply_learning_signal": reply_learning,
    }


def show_outcome_review_queue_command(limit: int = 5):
    reply_queue = show_reply_review_queue_command(limit=max(10, int(limit) * 3)).get("cases", [])
    cases = []
    for case in reply_queue:
        suggested_outcome = str(case.get("suggested_outcome") or "pending").strip().lower()
        confidence = float(case.get("confidence") or 0.0)
        if suggested_outcome in {"won", "lost", "ignored"} and confidence >= 0.75:
            cases.append(case)
        if len(cases) >= max(1, int(limit)):
            break
    return {"count": len(cases), "cases": cases}


def apply_suggested_outcome_command(opportunity_id: int, notes: str = ""):
    review = show_reply_review_command(opportunity_id=opportunity_id)
    if review.get("error"):
        return review
    suggestion = review.get("suggestion") or {}
    suggested_outcome = str(suggestion.get("suggested_outcome") or "pending").strip().lower()
    confidence = float(suggestion.get("confidence") or 0.0)
    if suggested_outcome not in {"won", "lost", "ignored"}:
        return {"error": "There is no high-confidence suggested outcome to apply for this opportunity"}
    if confidence < 0.75:
        return {"error": "Suggested outcome confidence is too low to apply automatically"}
    merged_notes = notes.strip()
    if suggestion.get("reason"):
        merged_notes = f"{merged_notes} | {suggestion.get('reason')}".strip(" |")
    result = mark_outreach_outcome_command(
        opportunity_id=int(opportunity_id),
        outcome_status=suggested_outcome,
        notes=merged_notes,
    )
    return {
        **result,
        "applied_from_suggestion": True,
        "suggested_outcome_confidence": confidence,
    }


def run_reply_outcome_monitor_command(send_update: bool = True, reply_limit: int = 5, outcome_limit: int = 5):
    from runtime.ops.reply_outcome_monitor import run_reply_outcome_monitor

    return run_reply_outcome_monitor(
        send_update=bool(send_update),
        reply_limit=max(1, int(reply_limit)),
        outcome_limit=max(1, int(outcome_limit)),
    )


def run_reply_draft_monitor_command(send_update: bool = True, limit: int = 5):
    from runtime.ops.reply_draft_monitor import run_reply_draft_monitor

    return run_reply_draft_monitor(
        send_update=bool(send_update),
        limit=max(1, int(limit)),
    )


def sync_outreach_execution_state_command(limit: int = 50):
    return sync_outreach_execution_state(limit=limit)


def show_action_envelopes_command(limit: int = 20, review_status: str = ""):
    return list_action_envelopes(limit=limit, review_status=review_status or "")


def show_security_incidents_command(limit: int = 20, status: str = ""):
    return list_security_incidents(limit=limit, status=status or "")


def show_value_heartbeat_command(refresh: bool = False):
    return get_value_heartbeat(refresh=bool(refresh))


def show_prospect_scout_command(refresh: bool = False):
    heartbeat = get_value_heartbeat(refresh=bool(refresh))
    return {
        "status": "ok",
        "candidates_considered": int(heartbeat.get("daily_candidates_considered") or 0),
        "new_real_prospects": _coerce_cached_list(heartbeat.get("overnight_best_new_prospects") or []),
        "target_slate": _coerce_cached_list(heartbeat.get("daily_target_slate") or []),
        "outreach_worthy_targets": _coerce_cached_list(heartbeat.get("daily_outreach_worthy_targets") or []),
        "scout_report": _coerce_cached_list(heartbeat.get("prospect_scout_report") or []),
        "primary_bottleneck": heartbeat.get("primary_bottleneck") or "",
    }


def show_daily_operator_briefing_command() -> str:
    from runtime.ops.operator_briefings import build_daily_operator_briefing

    return build_daily_operator_briefing()


def show_execution_plan_command(refresh: bool = False):
    return get_latest_execution_plan(refresh=bool(refresh))


def run_mission_execution_loop_command(send_update: bool = True):
    return run_mission_execution_loop(send_update=bool(send_update))


def show_brain_state_command(refresh: bool = False):
    return get_latest_brain_state(refresh=bool(refresh))


def run_strategic_deliberation_command(send_update: bool = True):
    return run_strategic_deliberation_loop(send_update=bool(send_update))


def show_brain_critic_command(refresh: bool = False):
    return show_latest_brain_review(refresh=bool(refresh))


def run_brain_critic_command(send_update: bool = True):
    return run_brain_critic_loop(send_update=bool(send_update))


def _brain_alignment(case: dict, brain: dict | None = None) -> dict[str, Any]:
    brain = brain or {}
    if not brain:
        return {"bonus": 0, "reason": ""}

    merchant_name = str(case.get("merchant_name") or "").strip().lower()
    merchant_domain = str(case.get("merchant_domain") or "").strip().lower()
    queue = str(case.get("queue") or "").strip().lower()
    recommended_action = str(case.get("recommended_action") or "").strip().lower()
    next_step = str(case.get("next_step") or "").strip().lower()
    strongest_case = str(brain.get("strongest_case") or "").strip().lower()
    strategic_bet = str(brain.get("strategic_bet") or "").strip().lower()
    bottleneck = str(brain.get("main_bottleneck") or "").strip().lower()
    decisive_action = str(brain.get("next_decisive_action") or "").strip().lower()
    ignore_list = [str(item).strip().lower() for item in (brain.get("distractions_to_ignore") or []) if str(item).strip()]

    bonus = 0
    reasons: list[str] = []

    if strongest_case and (
        (merchant_domain and merchant_domain in strongest_case)
        or (merchant_name and merchant_name in strongest_case)
    ):
        bonus += 120
        reasons.append("matches strongest case")

    if decisive_action and (
        (merchant_domain and merchant_domain in decisive_action)
        or (merchant_name and merchant_name in decisive_action)
        or (recommended_action and recommended_action.replace("_", " ") in decisive_action)
    ):
        bonus += 60
        reasons.append("supports decisive action")

    if strategic_bet and (
        (merchant_domain and merchant_domain in strategic_bet)
        or (merchant_name and merchant_name in strategic_bet)
        or (recommended_action and recommended_action.replace("_", " ") in strategic_bet)
    ):
        bonus += 45
        reasons.append("fits strategic bet")

    queue_label = str(case.get("queue_label") or "").lower()

    if "send execution" in bottleneck:
        if queue == "send_eligible" and "approved draft" in queue_label:
            bonus += 95
            reasons.append("directly attacks send bottleneck")
        elif queue == "awaiting_approval":
            bonus -= 30
            reasons.append("one step behind the active send bottleneck")
    elif "operator approval" in bottleneck or "approval gate" in bottleneck:
        if queue == "awaiting_approval":
            bonus += 80
            reasons.append("directly attacks approval bottleneck")
        elif queue == "send_eligible":
            bonus -= 20
            reasons.append("less direct than approval-ready work")
    elif "reply review" in bottleneck and queue == "replied":
        bonus += 70
        reasons.append("directly attacks reply bottleneck")
    elif "follow up" in bottleneck and queue == "follow_up_needed":
        bonus += 70
        reasons.append("directly attacks follow-up bottleneck")
    elif "draft creation" in bottleneck and queue == "send_eligible":
        bonus += 65
        reasons.append("directly attacks draft-creation bottleneck")
    elif "opportunity qualification" in bottleneck and queue == "needs_action_selection":
        bonus += 55
        reasons.append("directly attacks qualification bottleneck")

    ignore_blob = " ".join(ignore_list)
    if ignore_blob:
        if "expanding the candidate pool" in ignore_blob and queue == "needs_action_selection":
            bonus -= 35
            reasons.append("deprioritized by ignore list")
        if "approved draft is sent" in ignore_blob and queue in {"send_eligible", "needs_action_selection"}:
            bonus -= 25
            reasons.append("less urgent than active draft")
        if "volume is not the problem" in ignore_blob and queue == "needs_action_selection":
            bonus -= 20
            reasons.append("queue expansion is not the current problem")

    if not reasons:
        return {"bonus": 0, "reason": ""}
    return {"bonus": bonus, "reason": "; ".join(reasons[:3])}


def _top_queue_candidate_score(case: dict, brain: dict | None = None) -> int:
    queue = str(case.get("queue") or "")
    queue_rank = {
        "follow_up_needed": 350,
        "replied": 325,
        "awaiting_approval": 300,
        "send_eligible": 200,
        "needs_action_selection": 100,
    }.get(queue, 0)
    urgency_rank = {
        "high": 30,
        "medium": 20,
        "low": 10,
    }.get(str(case.get("urgency") or "low"), 0)
    queue_quality = int(case.get("queue_quality_score") or 0)
    icp_fit = int(case.get("icp_fit_score") or 0)
    contact_trust = int(case.get("contact_trust_score") or 0)
    learning_bonus = int(case.get("learning_confidence_bonus") or 0)
    high_conviction_bonus = 20 if case.get("high_conviction_prospect") else 0
    brain_alignment = _brain_alignment(case, brain)
    case["brain_alignment_bonus"] = int(brain_alignment.get("bonus") or 0)
    case["brain_alignment_reason"] = str(brain_alignment.get("reason") or "")
    return (
        queue_rank
        + urgency_rank
        + queue_quality
        + icp_fit
        + min(contact_trust, 25)
        + learning_bonus
        + high_conviction_bonus
        + int(case["brain_alignment_bonus"] or 0)
    )


def _build_top_queue_candidate_from_outreach(queue: str, context: dict) -> dict:
    learning = get_outreach_learning_signal(
        selected_play=context.get("selected_play") or context.get("recommended_play") or "",
        processor=context.get("processor") or "",
        distress_type=context.get("distress_type") or "",
    )
    outreach_status = str(context.get("outreach_status") or "").strip().lower()
    approval_state = str(context.get("approval_state") or "").strip().lower()
    is_ready_to_send = queue == "send_eligible" and outreach_status == "draft_ready" and approval_state == "approved"
    return {
        "opportunity_id": int(context.get("opportunity_id") or 0),
        "merchant_name": context.get("merchant_name_display") or context.get("merchant_name") or "",
        "merchant_domain": context.get("merchant_domain") or "",
        "queue": queue,
        "queue_label": {
            "awaiting_approval": "approval-ready draft",
            "send_eligible": "send-eligible lead",
        }.get(queue, queue.replace("_", " ")) if not is_ready_to_send else "approved draft ready to send",
        "opportunity_status": context.get("opportunity_status") or "unknown",
        "processor": context.get("processor") or "unknown",
        "distress_type": context.get("distress_type") or "unknown",
        "recommended_action": context.get("recommended_play") or "",
        "selected_action": context.get("selected_play") or "",
        "urgency": context.get("urgency") or "low",
        "queue_quality_score": int(context.get("queue_quality_score") or 0),
        "icp_fit_score": int(context.get("icp_fit_score") or 0),
        "icp_fit_label": context.get("icp_fit_label") or "",
        "icp_fit_reason": context.get("icp_fit_reason") or "",
        "high_conviction_prospect": bool(context.get("high_conviction_prospect")),
        "contact_trust_score": int(context.get("contact_trust_score") or 0),
        "contact_email": context.get("contact_email") or "",
        "contact_quality_label": context.get("contact_quality_label") or "",
        "why_now": context.get("why_now") or context.get("contact_reason") or "",
        "learning_confidence_bonus": int(learning.get("confidence_bonus") or 0),
        "learning_records": int(learning.get("records") or 0),
        "learning_win_rate": float(learning.get("win_rate") or 0.0),
        "learning_recent_lessons": list(learning.get("recent_lessons") or []),
        "next_step": (
            "Review the draft and approve it for send."
            if queue == "awaiting_approval"
            else "Open the approved draft, do a final check, and send it."
            if is_ready_to_send
            else "Review the reply and draft the next response."
            if queue == "replied"
            else "Draft the next follow-up touch."
            if queue == "follow_up_needed"
            else f"Draft outreach for the {str(context.get('recommended_play') or 'next').replace('_', ' ')} play."
        ),
    }


def _build_top_queue_candidate_from_action_case(case: dict) -> dict:
    reason = case.get("action_reason") or ""
    recommended_action = case.get("recommended_action") or ""
    return {
        "opportunity_id": int(case.get("opportunity_id") or 0),
        "merchant_name": case.get("merchant_name") or "",
        "merchant_domain": case.get("merchant_domain") or "",
        "queue": "needs_action_selection",
        "queue_label": "needs action selection",
        "opportunity_status": case.get("opportunity_status") or "pending_review",
        "processor": case.get("processor") or "unknown",
        "distress_type": case.get("distress_type") or "unknown",
        "recommended_action": recommended_action,
        "selected_action": case.get("selected_action") or "",
        "urgency": "medium",
        "queue_quality_score": int(case.get("queue_quality_score") or 0),
        "icp_fit_score": int(case.get("icp_fit_score") or 0),
        "icp_fit_label": case.get("icp_fit_label") or "",
        "icp_fit_reason": case.get("icp_fit_reason") or "",
        "high_conviction_prospect": bool(case.get("high_conviction_prospect")),
        "contact_trust_score": 0,
        "contact_email": "",
        "contact_quality_label": "",
        "why_now": reason or f"The current best play is {str(recommended_action).replace('_', ' ')}.",
        "next_step": f"Select {str(recommended_action).replace('_', ' ')} as the operator action and continue the review.",
    }


def _build_top_queue_candidate_from_best_available(context: dict, *, source_candidate: dict | None = None, has_review_draft: bool = False) -> dict:
    reason = (
        context.get("queue_eligibility_reason")
        or (source_candidate or {}).get("queue_eligibility_reason")
        or "This is the strongest current merchant-backed case even though it is not fully send-ready yet."
    )
    next_step = (
        "Review the draft Meridian prepared from the strongest current signal, tighten the facts, and decide whether to deepen contact discovery."
        if has_review_draft
        else "Review the strongest current merchant-backed signal and draft the first message Meridian should use once the contact path is clearer."
    )
    return {
        "opportunity_id": int(context.get("opportunity_id") or 0),
        "merchant_name": context.get("merchant_name_display") or context.get("merchant_name") or (source_candidate or {}).get("merchant_name") or "",
        "merchant_domain": context.get("merchant_domain") or (source_candidate or {}).get("merchant_domain") or "",
        "queue": "best_available_review",
        "queue_label": "best available review case",
        "opportunity_status": context.get("opportunity_status") or "pending_review",
        "processor": context.get("processor") or (source_candidate or {}).get("processor") or "unknown",
        "distress_type": context.get("distress_type") or (source_candidate or {}).get("distress_topic") or "unknown",
        "recommended_action": context.get("recommended_play") or "",
        "selected_action": context.get("selected_play") or "",
        "urgency": context.get("urgency") or (source_candidate or {}).get("urgency_label") or "low",
        "queue_quality_score": int(context.get("queue_quality_score") or (source_candidate or {}).get("queue_quality_score") or 0),
        "icp_fit_score": int(context.get("icp_fit_score") or (source_candidate or {}).get("icp_fit_score") or 0),
        "icp_fit_label": context.get("icp_fit_label") or "",
        "icp_fit_reason": context.get("icp_fit_reason") or "",
        "high_conviction_prospect": bool(context.get("high_conviction_prospect") or (source_candidate or {}).get("high_conviction_prospect")),
        "contact_trust_score": int(context.get("contact_trust_score") or 0),
        "contact_email": context.get("contact_email") or "",
        "contact_quality_label": context.get("contact_quality_label") or "",
        "why_now": context.get("why_now") or (source_candidate or {}).get("why_now") or reason,
        "learning_confidence_bonus": 0,
        "learning_records": 0,
        "learning_win_rate": 0.0,
        "learning_recent_lessons": [],
        "next_step": next_step,
    }


def _promote_best_available_signal_candidate() -> dict | None:
    recent_review_ids: list[int] = []
    with engine.connect() as conn:
        recent_rows = conn.execute(
            text(
                """
                SELECT id
                FROM merchant_opportunities
                WHERE status IN ('pending_review', 'approved', 'outreach_pending')
                ORDER BY created_at DESC
                LIMIT 10
                """
            )
        ).mappings().all()
        recent_review_ids = [int(row.get("id") or 0) for row in recent_rows if int(row.get("id") or 0) > 0]

    review_candidates: list[tuple[float, dict, dict]] = []
    seen_domains: set[str] = set()
    for opportunity_id in recent_review_ids:
        context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
        if context.get("error"):
            continue
        queue_class = str(context.get("queue_eligibility_class") or "").strip().lower()
        if queue_class not in {"operator_review_only", "research_only", "outreach_eligible"}:
            continue
        if queue_class == "outreach_eligible" and bool(context.get("contact_send_eligible")):
            continue
        merchant_key = str(context.get("merchant_domain") or context.get("merchant_name") or opportunity_id).strip().lower()
        if merchant_key in seen_domains:
            continue
        seen_domains.add(merchant_key)
        draft = show_local_outreach_draft_command(opportunity_id=opportunity_id)
        if draft.get("status") != "ok":
            draft = draft_outreach_for_opportunity_command(
                opportunity_id=opportunity_id,
                allow_best_effort=True,
            )
        refreshed_context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
        desired_play = str(refreshed_context.get("selected_play") or refreshed_context.get("recommended_play") or "").strip().lower()
        current_draft_play = str(((draft.get("draft") or {}).get("selected_play") if isinstance(draft, dict) else "") or "").strip().lower()
        if desired_play and current_draft_play and current_draft_play != desired_play:
            draft = draft_outreach_for_opportunity_command(
                opportunity_id=opportunity_id,
                allow_best_effort=True,
            )
            refreshed_context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
        refreshed_queue_class = str(refreshed_context.get("queue_eligibility_class") or queue_class).strip().lower()
        score = float(
            int(refreshed_context.get("queue_quality_score") or 0) * 2
            + int(refreshed_context.get("icp_fit_score") or 0)
            + (20 if refreshed_queue_class in {"operator_review_only", "outreach_eligible"} else 0)
            + (10 if draft.get("status") == "ok" else 0)
        )
        review_candidates.append((score, refreshed_context, draft))

    if review_candidates:
        review_candidates.sort(key=lambda item: item[0], reverse=True)
        _, context, draft = review_candidates[0]
        return {
            "created": False,
            "source_candidate": {},
            "draft": draft,
            "context": context,
        }

    with engine.connect() as conn:
        ranked, _ = _build_signal_candidate_pool(conn, limit=1, rejected_limit=0)
        if not ranked:
            return None
        candidate = dict(ranked[0])
        signal_row = conn.execute(
            text(
                """
                SELECT s.id AS signal_id,
                       s.merchant_id,
                       s.content,
                       s.source,
                       m.canonical_name,
                       m.domain,
                       m.industry,
                       m.status AS merchant_status,
                       m.validation_source,
                       m.confidence_score,
                       m.domain_confidence,
                       (
                           SELECT COUNT(*)
                           FROM merchant_contacts mc
                           WHERE mc.merchant_id = m.id
                             AND mc.email IS NOT NULL
                             AND mc.email != ''
                             AND COALESCE(mc.confidence, 0) >= 0.85
                             AND COALESCE(mc.email_verified, FALSE) = TRUE
                       ) AS send_eligible_contact_count,
                       (
                           SELECT MAX(
                               CASE
                                   WHEN COALESCE(mc.confidence_score, 0) > 0 THEN COALESCE(mc.confidence_score, 0)
                                   ELSE ROUND(COALESCE(mc.confidence, 0) * 100)
                               END
                           )
                           FROM merchant_contacts mc
                           WHERE mc.merchant_id = m.id
                             AND mc.email IS NOT NULL
                             AND mc.email != ''
                       ) AS best_contact_trust_score
                FROM signals s
                JOIN merchants m ON m.id = s.merchant_id
                WHERE s.id = :signal_id
                LIMIT 1
                """
            ),
            {"signal_id": int(candidate.get("signal_id") or 0)},
        ).mappings().first()
        if not signal_row or not signal_row.get("merchant_id"):
            return None

        promotion_ok, promotion_reason = _best_available_promotion_gate(candidate, dict(signal_row))
        if not promotion_ok:
            save_event(
                "best_available_opportunity_suppressed",
                {
                    "signal_id": int(candidate.get("signal_id") or 0),
                    "merchant_id": int(signal_row.get("merchant_id") or 0),
                    "merchant_domain": signal_row.get("domain") or candidate.get("merchant_domain") or "",
                    "reason": promotion_reason,
                },
            )
            return None

        merchant_id = int(signal_row.get("merchant_id") or 0)
        existing = conn.execute(
            text(
                """
                SELECT id
                FROM merchant_opportunities
                WHERE merchant_id = :merchant_id
                  AND created_at >= NOW() - INTERVAL '14 days'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"merchant_id": merchant_id},
        ).scalar()
        created = False
        if existing:
            opportunity_id = int(existing)
        else:
            candidate_distress = str(candidate.get("distress_topic") or "").strip().lower()
            distress_topic = normalize_distress_topic(
                signal_row.get("content") or ""
                if candidate_distress in {"", "unknown"}
                else candidate_distress
            )
            processor = str(candidate.get("processor") or "unknown").strip() or "unknown"
            strategy = {
                "source": "best_available_review",
                "signal_id": int(candidate.get("signal_id") or 0),
                "why_now": candidate.get("why_now") or "",
                "queue_eligibility_reason": candidate.get("queue_eligibility_reason") or "",
                "operator_note": "Auto-promoted from Meridian's best available merchant-backed signal because the active queue was empty.",
            }
            row = conn.execute(
                text(
                    """
                    INSERT INTO merchant_opportunities (
                        merchant_id, merchant_domain, processor, distress_topic,
                        sales_strategy, outreach_draft, checkout_url, opportunity_score, status
                    )
                    VALUES (
                        :merchant_id, :merchant_domain, :processor, :distress_topic,
                        :sales_strategy, '', '', :opportunity_score, 'pending_review'
                    )
                    RETURNING id
                    """
                ),
                {
                    "merchant_id": merchant_id,
                    "merchant_domain": signal_row.get("domain") or candidate.get("merchant_domain") or "",
                    "processor": processor,
                    "distress_topic": distress_topic,
                    "sales_strategy": json.dumps(strategy),
                    "opportunity_score": float(
                        int(candidate.get("queue_quality_score") or 0)
                        + int(candidate.get("icp_fit_score") or 0)
                        + int(candidate.get("commercial_readiness_score") or 0)
                    ),
                },
            ).first()
            conn.commit()
            opportunity_id = int(row[0])
            created = True
            save_event(
                "best_available_opportunity_promoted",
                {
                    "opportunity_id": opportunity_id,
                    "merchant_id": merchant_id,
                    "signal_id": int(candidate.get("signal_id") or 0),
                    "merchant_domain": signal_row.get("domain") or candidate.get("merchant_domain") or "",
                    "queue_quality_score": int(candidate.get("queue_quality_score") or 0),
                    "icp_fit_score": int(candidate.get("icp_fit_score") or 0),
                },
            )

    draft = show_local_outreach_draft_command(opportunity_id=opportunity_id)
    if draft.get("status") != "ok":
        draft = draft_outreach_for_opportunity_command(
            opportunity_id=opportunity_id,
            allow_best_effort=True,
        )
    context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
    if context.get("error"):
        return None
    return {
        "created": created,
        "source_candidate": candidate,
        "draft": draft,
        "context": context,
    }


def _operator_ready_context(context: dict) -> tuple[bool, str]:
    processor = str(context.get("processor") or "unknown").strip().lower()
    distress = str(context.get("distress_type") or "unknown").strip().lower()
    best_play = str(context.get("recommended_play") or context.get("selected_play") or "clarify_distress").strip().lower()
    queue_class = str(context.get("queue_eligibility_class") or "").strip().lower()
    quality_score = int(context.get("queue_quality_score") or 0)
    icp_fit_score = int(context.get("icp_fit_score") or 0)
    contact_email = str(context.get("contact_email") or "").strip()
    contact_send_eligible = bool(context.get("contact_send_eligible"))
    high_conviction = bool(context.get("high_conviction_prospect"))

    if queue_class and queue_class != "outreach_eligible":
        return False, f"queue class {queue_class}"
    if not contact_email or not contact_send_eligible:
        return False, "no trusted operator contact path"
    if processor == "unknown" and distress == "unknown":
        return False, "processor and distress are both unknown"
    if best_play == "clarify_distress" and distress == "unknown":
        return False, "best play is clarify_distress without canonical distress"
    if icp_fit_score < 50:
        return False, "icp fit is too weak"
    if best_play == "clarify_distress" and quality_score < 45:
        return False, "best play is clarify_distress without strong signal quality"
    if not high_conviction and quality_score < 55:
        return False, "not yet a high-conviction PayFlux prospect"
    if quality_score < 35:
        return False, "queue quality score is too low"
    return True, ""


def _best_available_promotion_gate(candidate: dict, signal_row: dict) -> tuple[bool, str]:
    merchant = {
        "id": signal_row.get("merchant_id"),
        "canonical_name": signal_row.get("canonical_name") or "",
        "domain": signal_row.get("domain") or "",
        "status": signal_row.get("merchant_status") or "",
        "validation_source": signal_row.get("validation_source") or "",
        "confidence_score": int(signal_row.get("confidence_score") or 0),
        "domain_confidence": signal_row.get("domain_confidence") or "",
    }
    quality = evaluate_opportunity_queue_quality(
        opportunity={
            "merchant_id": signal_row.get("merchant_id"),
            "merchant_domain": merchant.get("domain") or candidate.get("merchant_domain") or "",
            "merchant_name": signal_row.get("canonical_name") or "",
            "processor": candidate.get("processor") or "unknown",
            "distress_topic": candidate.get("distress_topic") or "unknown",
            "contact_email": candidate.get("contact_email") or "",
        },
        merchant=merchant,
        signal={"content": signal_row.get("content") or ""},
    )
    if quality.get("eligibility_class") != "outreach_eligible":
        return False, f"queue class {quality.get('eligibility_class') or 'unknown'}"
    if int(quality.get("quality_score") or 0) < 65:
        return False, "queue quality score is too low"
    if int(candidate.get("commercial_readiness_score") or 0) < 65:
        return False, "commercial readiness is too low"
    if int(candidate.get("icp_fit_score") or 0) < 68:
        return False, "icp fit is too weak"
    if not bool(candidate.get("high_conviction_prospect")):
        return False, "not yet a high-conviction PayFlux prospect"
    if int(signal_row.get("send_eligible_contact_count") or 0) <= 0:
        return False, "no trusted operator contact path"
    if int(signal_row.get("best_contact_trust_score") or 0) < 70:
        return False, "no trusted operator contact path"
    return True, ""


def show_top_queue_opportunities_command(limit: int = 3):
    top_limit = max(1, int(limit))
    fetch_limit = max(10, top_limit * 4)
    seen: set[int] = set()
    candidates: list[dict] = []
    suppressed = 0
    suppression_reasons: dict[str, int] = {}
    brain = get_latest_brain_state(refresh=False)

    follow_ups = list_sent_outreach_needing_follow_up(limit=fetch_limit)
    for row in follow_ups.get("cases", []):
        opportunity_id = int(row.get("opportunity_id") or 0)
        if not opportunity_id or opportunity_id in seen:
            continue
        context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
        if context.get("error"):
            continue
        candidate = _build_top_queue_candidate_from_outreach(str(row.get("status") or "follow_up_needed"), context)
        candidate["rank_score"] = _top_queue_candidate_score(candidate, brain)
        candidates.append(candidate)
        seen.add(opportunity_id)

    awaiting = list_outreach_awaiting_approval(limit=fetch_limit)
    for row in awaiting.get("cases", []):
        opportunity_id = int(row.get("opportunity_id") or 0)
        if not opportunity_id or opportunity_id in seen:
            continue
        context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
        if context.get("error"):
            continue
        ready, reason = _operator_ready_context(context)
        if not ready:
            suppressed += 1
            suppression_reasons[reason] = suppression_reasons.get(reason, 0) + 1
            continue
        candidate = _build_top_queue_candidate_from_outreach("awaiting_approval", context)
        candidate["rank_score"] = _top_queue_candidate_score(candidate, brain)
        candidates.append(candidate)
        seen.add(opportunity_id)

    send_eligible = list_send_eligible_outreach_leads(limit=fetch_limit)
    for row in send_eligible.get("cases", []):
        opportunity_id = int(row.get("opportunity_id") or 0)
        if not opportunity_id or opportunity_id in seen:
            continue
        context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
        if context.get("error"):
            continue
        ready, reason = _operator_ready_context(context)
        if not ready:
            suppressed += 1
            suppression_reasons[reason] = suppression_reasons.get(reason, 0) + 1
            continue
        candidate = _build_top_queue_candidate_from_outreach("send_eligible", context)
        candidate["rank_score"] = _top_queue_candidate_score(candidate, brain)
        candidates.append(candidate)
        seen.add(opportunity_id)

    uncovered = list_opportunities_by_operator_action_command(unselected_only=True, limit=fetch_limit)
    for row in uncovered.get("cases", []):
        opportunity_id = int(row.get("opportunity_id") or 0)
        if not opportunity_id or opportunity_id in seen:
            continue
        context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
        if context.get("error"):
            continue
        ready, reason = _operator_ready_context(context)
        if not ready:
            suppressed += 1
            suppression_reasons[reason] = suppression_reasons.get(reason, 0) + 1
            continue
        candidate = _build_top_queue_candidate_from_action_case(
            {
                **row,
                "recommended_action": context.get("recommended_play") or row.get("recommended_action"),
                "processor": context.get("processor") or row.get("processor"),
                "distress_type": context.get("distress_type") or row.get("distress_type"),
                "queue_quality_score": int(context.get("queue_quality_score") or row.get("queue_quality_score") or 0),
                "icp_fit_score": int(context.get("icp_fit_score") or row.get("icp_fit_score") or 0),
                "icp_fit_label": context.get("icp_fit_label") or row.get("icp_fit_label") or "",
                "icp_fit_reason": context.get("icp_fit_reason") or row.get("icp_fit_reason") or "",
                "high_conviction_prospect": bool(context.get("high_conviction_prospect") or row.get("high_conviction_prospect")),
                "action_reason": row.get("action_reason") or context.get("queue_eligibility_reason") or "",
            }
        )
        candidate["rank_score"] = _top_queue_candidate_score(candidate, brain)
        candidates.append(candidate)
        seen.add(opportunity_id)

    candidates.sort(key=lambda item: (int(item.get("rank_score") or 0), int(item.get("opportunity_id") or 0)), reverse=True)
    ranked: list[dict] = []
    seen_merchants: set[str] = set()
    for candidate in candidates:
        merchant_key = str(candidate.get("merchant_domain") or candidate.get("merchant_name") or candidate.get("opportunity_id") or "").strip().lower()
        if merchant_key and merchant_key in seen_merchants:
            continue
        if merchant_key:
            seen_merchants.add(merchant_key)
        ranked.append(candidate)
        if len(ranked) >= top_limit:
            break
    if not ranked:
        best_available = _promote_best_available_signal_candidate()
        if best_available:
            fallback = _build_top_queue_candidate_from_best_available(
                best_available.get("context") or {},
                source_candidate=best_available.get("source_candidate") or {},
                has_review_draft=(best_available.get("draft") or {}).get("status") == "ok",
            )
            fallback["rank_score"] = max(40, int(fallback.get("queue_quality_score") or 0) + int(fallback.get("icp_fit_score") or 0))
            ranked.append(fallback)
            candidates.append(fallback)
    return {
        "count": len(ranked),
        "total_candidates_seen": len(candidates),
        "suppressed_weak_candidates": suppressed,
        "suppression_reasons": suppression_reasons,
        "brain_state": {
            "strategic_bet": brain.get("strategic_bet") or "",
            "main_bottleneck": brain.get("main_bottleneck") or "",
            "next_decisive_action": brain.get("next_decisive_action") or "",
        },
        "top_opportunities": ranked,
    }


def advance_top_queue_opportunity_command() -> dict:
    top = show_top_queue_opportunities_command(limit=1)
    cases = list(top.get("top_opportunities") or [])
    if not cases:
        return {
            "status": "blocked",
            "reason": "no_operator_ready_top_case",
            "queue_snapshot": top,
        }

    case = cases[0]
    opportunity_id = int(case.get("opportunity_id") or 0)
    if not opportunity_id:
        return {
            "status": "blocked",
            "reason": "top_case_missing_opportunity_id",
            "queue_snapshot": top,
        }

    recommended_action = str(case.get("recommended_action") or "").strip()
    selection = None
    if recommended_action:
        selection = mark_opportunity_operator_action_command(
            opportunity_id=opportunity_id,
            selected_action=recommended_action,
            action_reason="Auto-selected from top queue opportunity ranking",
        )

    recommendation = get_outreach_recommendation_command(opportunity_id=opportunity_id)
    if recommendation.get("error"):
        return {
            "status": "blocked",
            "reason": "outreach_recommendation_error",
            "top_case": case,
            "selection": selection,
            "recommendation": recommendation,
        }

    if case.get("queue") == "best_available_review":
        draft = draft_outreach_for_opportunity_command(
            opportunity_id=opportunity_id,
            allow_best_effort=True,
        )
        return {
            "status": "drafted_best_available_review",
            "top_case": case,
            "selection": selection,
            "recommendation": recommendation,
            "draft": draft,
            "next_step": case.get("next_step") or "Review the best available case and tighten the draft.",
        }

    if case.get("queue") in {"follow_up_needed", "replied"}:
        follow_up_type = "reply_follow_up" if case.get("queue") == "replied" else "follow_up_nudge"
        draft = draft_outreach_for_opportunity_command(opportunity_id=opportunity_id, outreach_type=follow_up_type)
        return {
            "status": "drafted_follow_up",
            "top_case": case,
            "selection": selection,
            "recommendation": recommendation,
            "draft": draft,
        }

    if recommendation.get("should_proceed_now"):
        draft = draft_outreach_for_opportunity_command(opportunity_id=opportunity_id)
        return {
            "status": "drafted",
            "top_case": case,
            "selection": selection,
            "recommendation": recommendation,
            "draft": draft,
        }

    return {
        "status": "needs_followup_work",
        "top_case": case,
        "selection": selection,
        "recommendation": recommendation,
        "brain_alignment_reason": case.get("brain_alignment_reason") or "",
        "next_step": recommendation.get("next_contact_move") or recommendation.get("wait_reason") or "Review the case manually.",
    }


def _suggest_reply_review(case: dict, intelligence: dict) -> dict:
    category = str((intelligence or {}).get("thread_category") or "").strip().lower()
    reply_priority = str((intelligence or {}).get("reply_priority") or "").strip().lower()
    metadata = (intelligence or {}).get("metadata") or {}
    sender_is_automated = bool(metadata.get("sender_is_automated"))
    reason_codes = list(metadata.get("reason_codes") or [])
    reply_intent = str(metadata.get("reply_intent") or "unknown").strip().lower()
    buying_intent = str(metadata.get("buying_intent") or "low").strip().lower()
    reply_intent_confidence = float(metadata.get("reply_intent_confidence") or 0.0)
    reply_intent_reason = str(metadata.get("reply_intent_reason") or "").strip()

    if category == "noise_system" and sender_is_automated:
        return {
            "suggested_next_move": "Mark this thread ignored or lost and stop follow-up.",
            "suggested_outcome": "ignored",
            "reason": "The reply looks automated or bounce-like rather than merchant-operated.",
            "confidence": 0.86,
        }
    if reply_intent == "negative":
        return {
            "suggested_next_move": "Mark this thread lost or ignored and stop follow-up.",
            "suggested_outcome": "lost",
            "reason": reply_intent_reason or "The contact explicitly rejected further outreach.",
            "confidence": max(0.82, reply_intent_confidence),
        }
    if reply_intent == "authority_mismatch":
        return {
            "suggested_next_move": "Ask for the right owner or route the thread to the decision-maker before pitching further.",
            "suggested_outcome": "pending",
            "reason": reply_intent_reason or "The reply suggests this contact is not the buyer.",
            "confidence": max(0.74, reply_intent_confidence),
        }
    if reply_intent == "interested":
        return {
            "suggested_next_move": "Reply now with the clearest next step while interest is live.",
            "suggested_outcome": "pending",
            "reason": reply_intent_reason or "The merchant sounds interested enough to move the conversation forward quickly.",
            "confidence": max(0.82, reply_intent_confidence),
        }
    if reply_intent == "defer":
        return {
            "suggested_next_move": "Acknowledge the timing issue, keep the thread warm, and set a clean follow-up point.",
            "suggested_outcome": "pending",
            "reason": reply_intent_reason or "The merchant is not rejecting the conversation, but timing is the blocker.",
            "confidence": max(0.72, reply_intent_confidence),
        }
    if reply_intent == "objection":
        return {
            "suggested_next_move": "Address the objection directly before trying to push the next step.",
            "suggested_outcome": "pending",
            "reason": reply_intent_reason or "The merchant pushed back with a real objection instead of going silent.",
            "confidence": max(0.7, reply_intent_confidence),
        }
    if category == "merchant_distress":
        return {
            "suggested_next_move": "Review the reply now and draft a reply follow-up while the merchant is engaged.",
            "suggested_outcome": "pending",
            "reason": reply_intent_reason or "The reply still looks like live merchant payment distress, so the conversation is active.",
            "confidence": max(0.74 if reply_priority == "high" else 0.62, reply_intent_confidence),
        }
    if "test_thread" in reason_codes:
        return {
            "suggested_next_move": "Ignore this thread because it looks like a test path.",
            "suggested_outcome": "ignored",
            "reason": "The reply was classified as a test thread.",
            "confidence": 0.9,
        }
    return {
        "suggested_next_move": "Review the reply manually before sending anything else.",
        "suggested_outcome": "pending",
        "reason": reply_intent_reason or "The reply does not cleanly resolve to a merchant-distress win or an automated bounce.",
        "confidence": max(0.5, reply_intent_confidence),
    }
