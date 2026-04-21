from __future__ import annotations

from datetime import datetime

from runtime.channels.base import DraftAction
from runtime.channels.gmail_adapter import GmailAdapter
from runtime.channels.gmail_triage import sync_gmail_thread_intelligence
from runtime.channels.policy import evaluate_send_policy
from runtime.channels.reddit_adapter import RedditAdapter
from runtime.channels.store import (
    get_gmail_thread_intelligence,
    get_channel_metrics_snapshot,
    get_channel_settings,
    list_gmail_thread_intelligence,
    get_recent_audit_log,
    log_action,
    record_journal_entry,
    set_channel_kill_switch as store_set_channel_kill_switch,
    set_channel_mode as store_set_channel_mode,
)
from runtime.health.telemetry import clear_component_fields, get_component_state, heartbeat, record_component_state, utc_now_iso
from runtime.safety.control_plane import create_action_envelope, inspect_untrusted_content


def _adapter(channel: str):
    if channel == "gmail":
        return GmailAdapter()
    if channel == "reddit":
        return RedditAdapter()
    raise ValueError(f"Unsupported channel: {channel}")


def _record_channel_status(channel: str, status: str, **fields):
    if status == "healthy" and not fields.get("last_channel_error"):
        clear_component_fields(channel, "last_channel_error")
        clear_component_fields("channel_actions", "last_channel_error")
    heartbeat(channel, ttl=900, status=status, **fields)
    snapshot = get_channel_metrics_snapshot()
    component_fields = {
        "action_queue_depth": snapshot["action_queue_depth"],
        "approval_queue_depth": snapshot["approval_queue_depth"],
        "last_action_sent_at": snapshot["last_action_sent_at"],
        **fields,
    }
    record_component_state(
        "channel_actions",
        ttl=900,
        **component_fields,
    )


def _sync_gmail_target(target, *, create_signal: bool = True, force: bool = False):
    intelligence = sync_gmail_thread_intelligence(target, create_signal=create_signal, force=force)
    target.metadata["gmail_thread_intelligence"] = intelligence
    target.metadata.update(
        {
            "thread_category": intelligence.get("thread_category"),
            "processor": intelligence.get("processor"),
            "distress_type": intelligence.get("distress_type"),
            "reply_priority": intelligence.get("reply_priority"),
            "draft_recommended": intelligence.get("draft_recommended"),
            "signal_eligible": intelligence.get("signal_eligible"),
            "opportunity_eligible": intelligence.get("opportunity_eligible"),
            "test_thread": intelligence.get("test_thread"),
        }
    )
    return intelligence


def _priority_sort_key(entry: dict):
    order = {"high": 0, "medium": 1, "low": 2, "none": 3}
    triaged_at = entry.get("triaged_at", "")
    try:
        triaged_ts = datetime.fromisoformat(str(triaged_at).replace("Z", "+00:00")).timestamp()
    except Exception:
        triaged_ts = 0
    return (
        order.get(entry.get("reply_priority", "none"), 3),
        -float(entry.get("confidence", 0.0) or 0.0),
        -triaged_ts,
    )


def _inspect_target_security(channel: str, target) -> dict | None:
    inspection = inspect_untrusted_content(
        source_channel=channel,
        source_id=target.target_id,
        thread_id=target.thread_id,
        title=target.title,
        body=target.body,
        metadata={
            "author": target.author,
            "account_identity": target.account_identity,
        },
    )
    if inspection.get("detected"):
        target.metadata["security_incident_detected"] = True
        target.metadata["security_markers"] = inspection.get("markers", [])
        target.metadata["security_severity"] = inspection.get("severity")
        target.metadata["security_incident_id"] = inspection.get("incident_id")
    return inspection if inspection.get("detected") else None


def _draft_channel_action(channel: str, target_id: str, **kwargs) -> dict:
    try:
        adapter = _adapter(channel)
        settings = get_channel_settings(channel)
        target = adapter.read_context(target_id, **kwargs)
        security_incident = _inspect_target_security(channel, target)
        intelligence = None
        if channel == "gmail":
            intelligence = _sync_gmail_target(target, create_signal=True, force=kwargs.get("force_triage", False))
            allow_customer_support = bool(kwargs.get("allow_customer_support", False))
            draftable = intelligence.get("thread_category") == "merchant_distress" or (
                allow_customer_support and intelligence.get("thread_category") == "customer_support"
            )
            if not draftable:
                reason = "gmail_thread_not_draftable"
                log_action(
                    channel=channel,
                    action_type="draft",
                    approval_state="blocked",
                    mode=settings.get("mode", "approval_required"),
                    result="blocked",
                    account_identity=target.account_identity,
                    thread_id=target.thread_id,
                    target_id=target.target_id,
                    confidence=intelligence.get("confidence"),
                    rationale=reason,
                    metadata={**target.metadata, "gmail_thread_intelligence": intelligence, "policy_reason": reason},
                )
                _record_channel_status(
                    "gmail",
                    "healthy",
                    gmail_status="healthy",
                    gmail_triage_status="healthy",
                    last_gmail_triage_at=intelligence.get("triaged_at"),
                    last_gmail_triage_error="",
                    last_channel_action="draft_blocked",
                    last_channel_action_at=utc_now_iso(),
                )
                return {
                    "channel": "gmail",
                    "status": "blocked",
                    "reason": reason,
                    "thread_category": intelligence.get("thread_category"),
                    "draft_recommended": intelligence.get("draft_recommended"),
                    "security_incident": security_incident,
                }
        draft = adapter.draft_action(target, **kwargs)
        if security_incident:
            draft.metadata["security_markers"] = security_incident.get("markers", [])
            draft.metadata["security_severity"] = security_incident.get("severity")
            draft.metadata["security_incident_id"] = security_incident.get("incident_id")
        log_action(
            channel=channel,
            action_type="draft",
            approval_state="approval_required" if settings.get("mode") != "auto_send_high_confidence" else "ready",
            mode=settings.get("mode", "approval_required"),
            result="drafted",
            account_identity=target.account_identity,
            thread_id=target.thread_id,
            target_id=target.target_id,
            confidence=draft.confidence,
            rationale=draft.rationale,
            draft_text=draft.text,
            metadata={**target.metadata, **draft.metadata, "gmail_thread_intelligence": intelligence} if intelligence else {**target.metadata, **draft.metadata},
        )
        envelope = create_action_envelope(
            channel=channel,
            action_type="draft",
            envelope_status="drafted",
            review_status="pending_review",
            risk_level=security_incident.get("severity", "medium") if security_incident else "medium",
            target_id=target.target_id,
            thread_id=target.thread_id,
            account_identity=target.account_identity,
            confidence=draft.confidence,
            summary=f"{channel} draft for {target.thread_id or target.target_id}",
            rationale=draft.rationale,
            draft_text=draft.text,
            metadata={
                **target.metadata,
                **draft.metadata,
                "gmail_thread_intelligence": intelligence,
            } if intelligence else {**target.metadata, **draft.metadata},
        )
        _record_channel_status(
            channel,
            "healthy",
            **{
                f"{channel}_status": "healthy",
                "gmail_triage_status": "healthy" if channel == "gmail" else None,
                "last_gmail_triage_at": intelligence.get("triaged_at") if intelligence else None,
                "last_gmail_triage_error": "" if intelligence else None,
                "last_channel_action": "drafted",
                "last_channel_action_at": utc_now_iso(),
            },
        )
        return {
            "channel": channel,
            "mode": settings.get("mode", "approval_required"),
            "target_id": target.target_id,
            "thread_id": target.thread_id,
            "subject": getattr(draft, "subject", ""),
            "draft_text": draft.text,
            "rationale": draft.rationale,
            "confidence": draft.confidence,
            "metadata": {**draft.metadata, "gmail_thread_intelligence": intelligence} if intelligence else draft.metadata,
            "action_envelope": envelope,
            "security_incident": security_incident,
        }
    except Exception as exc:
        _record_channel_status(
            channel,
            "degraded",
            **{
                f"{channel}_status": "degraded",
                "last_channel_error": str(exc),
                "last_gmail_triage_error": str(exc) if channel == "gmail" else None,
                "last_channel_action": "draft_error",
                "last_channel_action_at": utc_now_iso(),
            },
        )
        return {"channel": channel, "status": "error", "error": str(exc)}


def _send_channel_action(
    channel: str,
    target_id: str,
    human_approved: bool = False,
    approved_by: str = "",
    approval_source: str = "",
    **kwargs,
) -> dict:
    try:
        adapter = _adapter(channel)
        target = adapter.read_context(target_id, **kwargs)
        security_incident = _inspect_target_security(channel, target)
        intelligence = None
        if channel == "gmail":
            intelligence = _sync_gmail_target(target, create_signal=True, force=kwargs.get("force_triage", False))
            allow_customer_support = bool(kwargs.get("allow_customer_support", False))
            sendable = intelligence.get("thread_category") == "merchant_distress" or (
                allow_customer_support and intelligence.get("thread_category") == "customer_support"
            )
            if not sendable:
                reason = "gmail_thread_not_sendable"
                log_action(
                    channel=channel,
                    action_type="send",
                    approval_state="blocked",
                    mode=get_channel_settings(channel).get("mode", "approval_required"),
                    result="blocked",
                    account_identity=target.account_identity,
                    thread_id=target.thread_id,
                    target_id=target.target_id,
                    confidence=intelligence.get("confidence"),
                    rationale=reason,
                    metadata={**target.metadata, "gmail_thread_intelligence": intelligence, "policy_reason": reason},
                )
                _record_channel_status(
                    "gmail",
                    "healthy",
                    gmail_status="healthy",
                    gmail_triage_status="healthy",
                    last_gmail_triage_at=intelligence.get("triaged_at"),
                    last_gmail_triage_error="",
                    last_channel_action="send_blocked",
                    last_channel_action_at=utc_now_iso(),
                )
                return {
                    "channel": "gmail",
                    "status": "blocked",
                    "reason": reason,
                    "thread_category": intelligence.get("thread_category"),
                    "security_incident": security_incident,
                }
        draft: DraftAction = adapter.draft_action(target, **kwargs)
        if security_incident:
            draft.metadata["security_markers"] = security_incident.get("markers", [])
            draft.metadata["security_severity"] = security_incident.get("severity")
            draft.metadata["security_incident_id"] = security_incident.get("incident_id")
        if approved_by:
            draft.metadata["approved_by"] = approved_by
        if approval_source:
            draft.metadata["approval_source"] = approval_source
        decision = evaluate_send_policy(channel=channel, target=target, draft=draft, human_approved=human_approved)

        if decision.status in {"blocked", "requires_approval", "skipped_duplicate"}:
            result_label = "approval_required" if decision.status == "requires_approval" else decision.status
            approval_metadata = {
                **target.metadata,
                **draft.metadata,
                "gmail_thread_intelligence": intelligence,
                "policy_reason": decision.reason,
                "approval_required": decision.status == "requires_approval",
                "approval_command": (
                    f"/approve_gmail_send {target.thread_id}"
                    if channel == "gmail"
                    else f"/approve_reddit_send {target.target_id}"
                ),
            }
            log_action(
                channel=channel,
                action_type="send",
                approval_state=decision.approval_state,
                mode=decision.mode,
                result=result_label,
                account_identity=target.account_identity,
                thread_id=target.thread_id,
                target_id=target.target_id,
                confidence=draft.confidence,
                rationale=draft.rationale,
                draft_text=draft.text,
                idempotency_key="",
                metadata=approval_metadata,
            )
            envelope = create_action_envelope(
                channel=channel,
                action_type="send",
                envelope_status=decision.status,
                review_status="pending_review" if decision.status == "requires_approval" else "reviewed",
                risk_level=security_incident.get("severity", "medium") if security_incident else "medium",
                target_id=target.target_id,
                thread_id=target.thread_id,
                account_identity=target.account_identity,
                confidence=draft.confidence,
                summary=f"{channel} send for {target.thread_id or target.target_id}",
                rationale=decision.reason,
                draft_text=draft.text,
                metadata=approval_metadata,
            )
            _record_channel_status(
                channel,
                "healthy",
                **{
                    f"{channel}_status": "degraded" if decision.status == "blocked" else "healthy",
                    "last_channel_error": decision.reason if decision.status == "blocked" else "",
                    "gmail_triage_status": "healthy" if channel == "gmail" else None,
                    "last_gmail_triage_at": intelligence.get("triaged_at") if intelligence else None,
                    "last_gmail_triage_error": "" if intelligence else None,
                    "last_channel_action": decision.status,
                    "last_channel_action_at": utc_now_iso(),
                },
            )
            return {
                "channel": channel,
                "status": decision.status,
                "reason": decision.reason,
                "mode": decision.mode,
                "confidence": draft.confidence,
                "draft_text": draft.text,
                "approval_state": decision.approval_state,
                "approval_command": approval_metadata["approval_command"],
                "action_envelope": envelope,
                "security_incident": security_incident,
            }

        result = adapter.send_action(draft, **kwargs)
        success_metadata = {
            **target.metadata,
            **draft.metadata,
            **result.payload,
            "gmail_thread_intelligence": intelligence,
            "approved_by": approved_by or "",
            "approval_source": approval_source or "",
        }
        log_action(
            channel=channel,
            action_type="send",
            approval_state=decision.approval_state,
            mode=decision.mode,
            result=result.status,
            account_identity=target.account_identity,
            thread_id=target.thread_id,
            target_id=target.target_id,
            confidence=draft.confidence,
            rationale=draft.rationale,
            draft_text=draft.text,
            final_text=draft.text,
            idempotency_key=decision.idempotency_key,
            error=result.error,
            metadata=success_metadata,
            sent=result.ok,
        )
        envelope = create_action_envelope(
            channel=channel,
            action_type="send",
            envelope_status="sent" if result.ok else result.status,
            review_status="approved" if human_approved else "reviewed",
            risk_level=security_incident.get("severity", "medium") if security_incident else "medium",
            target_id=target.target_id,
            thread_id=target.thread_id,
            account_identity=target.account_identity,
            confidence=draft.confidence,
            summary=f"{channel} send for {target.thread_id or target.target_id}",
            rationale=draft.rationale,
            draft_text=draft.text,
            metadata=success_metadata,
        )
        if channel == "reddit":
            record_journal_entry(
                channel="reddit",
                external_id=target.thread_id,
                last_action_type="proactive_reply",
                account_identity=target.account_identity,
                thread_id=target.thread_id,
                metadata={"target_id": target.target_id},
            )
        if channel == "gmail":
            record_journal_entry(
                channel="gmail",
                external_id=target.thread_id,
                last_action_type="thread_reply",
                account_identity=target.account_identity,
                thread_id=target.thread_id,
                metadata={"target_id": target.target_id, "recipient": draft.metadata.get("recipient", "")},
            )
        _record_channel_status(
            channel,
            "healthy",
            **{
                f"{channel}_status": "healthy",
                "gmail_triage_status": "healthy" if channel == "gmail" else None,
                "last_gmail_triage_at": intelligence.get("triaged_at") if intelligence else None,
                "last_gmail_triage_error": "" if intelligence else None,
                "last_channel_action": "sent",
                "last_channel_action_at": utc_now_iso(),
                "last_action_sent_at": utc_now_iso(),
            },
        )
        return {
            "channel": channel,
            "status": result.status,
            "target_id": target.target_id,
            "thread_id": target.thread_id,
            "confidence": draft.confidence,
            "remote_id": result.remote_id,
            "approval_state": decision.approval_state,
            "action_envelope": envelope,
            "security_incident": security_incident,
        }
    except Exception as exc:
        log_action(
            channel=channel,
            action_type="send",
            approval_state="blocked",
            mode=get_channel_settings(channel).get("mode", "approval_required"),
            result="error",
            target_id=target_id,
            error=str(exc),
            metadata={"exception": str(exc)},
        )
        _record_channel_status(
            channel,
            "degraded",
            **{
                f"{channel}_status": "degraded",
                "last_channel_error": str(exc),
                "last_gmail_triage_error": str(exc) if channel == "gmail" else None,
                "last_channel_action": "send_error",
                "last_channel_action_at": utc_now_iso(),
            },
        )
        return {"channel": channel, "status": "error", "error": str(exc)}


def list_reddit_candidates(query: str = "", limit: int = 10) -> dict:
    try:
        adapter = _adapter("reddit")
        targets = adapter.list_targets(query=query, limit=limit)
        _record_channel_status("reddit", "healthy", reddit_status="healthy", last_channel_action="listen", last_channel_action_at=utc_now_iso())
        return {
            "channel": "reddit",
            "mode": get_channel_settings("reddit").get("mode", "approval_required"),
            "count": len(targets),
            "candidates": [
                {
                    "target_id": target.target_id,
                    "thread_id": target.thread_id,
                    "title": target.title,
                    "author": target.author,
                    "subreddit": target.metadata.get("subreddit", ""),
                    "permalink": target.metadata.get("permalink", ""),
                    "confidence_hint": target.metadata.get("confidence_hint", 0.0),
                }
                for target in targets
            ],
        }
    except Exception as exc:
        _record_channel_status("reddit", "degraded", reddit_status="degraded", last_channel_error=str(exc), last_channel_action="listen_error", last_channel_action_at=utc_now_iso())
        return {"channel": "reddit", "status": "error", "error": str(exc)}


def draft_reddit_reply(target_id: str, permalink: str = "") -> dict:
    return _draft_channel_action("reddit", target_id, permalink=permalink)


def send_reddit_reply(
    target_id: str,
    permalink: str = "",
    human_approved: bool = False,
    approved_by: str = "",
    approval_source: str = "",
) -> dict:
    return _send_channel_action(
        "reddit",
        target_id,
        human_approved=human_approved,
        approved_by=approved_by,
        approval_source=approval_source,
        permalink=permalink,
    )


def list_gmail_threads(query: str = "label:INBOX", limit: int = 10) -> dict:
    try:
        adapter = _adapter("gmail")
        adapter.ensure_default_labels()
        targets = adapter.list_targets(query=query, limit=limit)
        triaged_threads = []
        for target in targets:
            intelligence = _sync_gmail_target(target, create_signal=True)
            if not intelligence.get("cached"):
                log_action(
                    channel="gmail",
                    action_type="triage",
                    approval_state="recorded",
                    mode=get_channel_settings("gmail").get("mode", "approval_required"),
                    result="triaged",
                    account_identity=target.account_identity,
                    thread_id=target.thread_id,
                    target_id=target.target_id,
                    confidence=intelligence.get("confidence"),
                    rationale=intelligence.get("thread_category"),
                    metadata={**target.metadata, "gmail_thread_intelligence": intelligence},
                )
            triaged_threads.append(
                {
                    "thread_id": target.thread_id,
                    "subject": target.title,
                    "from": target.metadata.get("sender_email", ""),
                    "labels": target.metadata.get("labels", []),
                    "thread_category": intelligence.get("thread_category"),
                    "confidence": intelligence.get("confidence"),
                    "processor": intelligence.get("processor"),
                    "distress_type": intelligence.get("distress_type"),
                    "reply_priority": intelligence.get("reply_priority"),
                    "draft_recommended": intelligence.get("draft_recommended"),
                    "signal_eligible": intelligence.get("signal_eligible"),
                    "opportunity_eligible": intelligence.get("opportunity_eligible"),
                    "test_thread": intelligence.get("test_thread"),
                    "triaged_at": intelligence.get("triaged_at"),
                }
            )
        triaged_threads.sort(key=_priority_sort_key)
        last_triage_at = max((item.get("triaged_at") for item in triaged_threads if item.get("triaged_at")), default=None)
        _record_channel_status(
            "gmail",
            "healthy",
            gmail_status="healthy",
            gmail_triage_status="healthy",
            last_gmail_triage_at=last_triage_at,
            last_gmail_triage_error="",
            last_channel_action="read",
            last_channel_action_at=utc_now_iso(),
        )
        return {
            "channel": "gmail",
            "mode": get_channel_settings("gmail").get("mode", "approval_required"),
            "count": len(triaged_threads),
            "merchant_distress_count": sum(1 for item in triaged_threads if item.get("thread_category") == "merchant_distress"),
            "reply_worthy_count": sum(1 for item in triaged_threads if item.get("draft_recommended")),
            "threads": triaged_threads,
        }
    except Exception as exc:
        _record_channel_status(
            "gmail",
            "degraded",
            gmail_status="degraded",
            gmail_triage_status="degraded",
            last_channel_error=str(exc),
            last_gmail_triage_error=str(exc),
            last_channel_action="read_error",
            last_channel_action_at=utc_now_iso(),
        )
        return {"channel": "gmail", "status": "error", "error": str(exc)}


def draft_gmail_reply(thread_id: str) -> dict:
    return _draft_channel_action("gmail", thread_id)


def send_gmail_reply(
    thread_id: str,
    human_approved: bool = False,
    approved_by: str = "",
    approval_source: str = "",
) -> dict:
    return _send_channel_action(
        "gmail",
        thread_id,
        human_approved=human_approved,
        approved_by=approved_by,
        approval_source=approval_source,
    )


def draft_gmail_distress_reply(thread_id: str) -> dict:
    return _draft_channel_action("gmail", thread_id)


def create_gmail_signal(thread_id: str) -> dict:
    try:
        adapter = _adapter("gmail")
        target = adapter.read_context(thread_id)
        intelligence = _sync_gmail_target(target, create_signal=True, force=True)
        result = {
            "channel": "gmail",
            "thread_id": thread_id,
            "thread_category": intelligence.get("thread_category"),
            "signal_id": intelligence.get("signal_id"),
            "opportunity_id": intelligence.get("opportunity_id"),
            "signal_eligible": intelligence.get("signal_eligible"),
            "opportunity_eligible": intelligence.get("opportunity_eligible"),
        }
        return result
    except Exception as exc:
        _record_channel_status(
            "gmail",
            "degraded",
            gmail_status="degraded",
            gmail_triage_status="degraded",
            last_channel_error=str(exc),
            last_gmail_triage_error=str(exc),
            last_channel_action="signal_error",
            last_channel_action_at=utc_now_iso(),
        )
        return {"channel": "gmail", "status": "error", "error": str(exc)}


def show_gmail_thread_intelligence(thread_id: str) -> dict:
    intelligence = get_gmail_thread_intelligence(thread_id)
    if intelligence:
        return {"channel": "gmail", "thread_id": thread_id, "intelligence": intelligence}
    try:
        adapter = _adapter("gmail")
        target = adapter.read_context(thread_id)
        intelligence = _sync_gmail_target(target, create_signal=True, force=True)
        return {"channel": "gmail", "thread_id": thread_id, "intelligence": intelligence}
    except Exception as exc:
        return {"channel": "gmail", "status": "error", "error": str(exc)}


def list_gmail_triage(limit: int = 25) -> dict:
    rows = list_gmail_thread_intelligence(limit=limit)
    rows.sort(key=_priority_sort_key)
    return {
        "channel": "gmail",
        "count": len(rows),
        "merchant_distress_count": sum(1 for row in rows if row.get("thread_category") == "merchant_distress"),
        "reply_worthy_count": sum(1 for row in rows if row.get("draft_recommended")),
        "threads": rows,
    }


def list_gmail_merchant_distress(limit: int = 25) -> dict:
    rows = list_gmail_thread_intelligence(category="merchant_distress", only_reply_worthy=True, limit=limit)
    rows.sort(key=_priority_sort_key)
    return {
        "channel": "gmail",
        "count": len(rows),
        "threads": rows,
    }


def run_gmail_triage_cycle(query: str | None = None, limit: int | None = None) -> dict:
    gmail_query = query or "label:INBOX newer_than:30d"
    gmail_limit = int(limit or 15)
    adapter = _adapter("gmail")
    adapter.ensure_default_labels()
    targets = adapter.list_targets(query=gmail_query, limit=gmail_limit)
    triaged = 0
    merchant_distress = 0
    signals_created = 0
    opportunities_created = 0
    triage_times = []
    for target in targets:
        intelligence = _sync_gmail_target(target, create_signal=True)
        triaged += 1
        merchant_distress += 1 if intelligence.get("thread_category") == "merchant_distress" else 0
        signals_created += 1 if intelligence.get("signal_id") else 0
        opportunities_created += 1 if intelligence.get("opportunity_id") else 0
        if intelligence.get("triaged_at"):
            triage_times.append(intelligence.get("triaged_at"))
    last_triage_at = max(triage_times, default=None)
    _record_channel_status(
        "gmail",
        "healthy",
        gmail_status="healthy",
        gmail_triage_status="healthy",
        last_gmail_triage_at=last_triage_at,
        last_gmail_triage_error="",
        last_channel_action="triage",
        last_channel_action_at=utc_now_iso(),
    )
    return {
        "channel": "gmail",
        "triaged_threads": triaged,
        "merchant_distress_threads": merchant_distress,
        "signals_recorded": signals_created,
        "opportunities_recorded": opportunities_created,
        "query": gmail_query,
        "limit": gmail_limit,
    }


def show_channel_audit_log(channel: str = "", limit: int = 25) -> dict:
    entries = get_recent_audit_log(channel=channel or None, limit=limit)
    snapshot = get_channel_metrics_snapshot()
    return {
        "entries": entries,
        "approval_queue_depth": snapshot["approval_queue_depth"],
        "action_queue_depth": snapshot["action_queue_depth"],
        "last_action_sent_at": snapshot["last_action_sent_at"],
    }


def set_channel_mode(channel: str, mode: str) -> dict:
    settings = store_set_channel_mode(channel, mode, updated_by="operator")
    current_state = get_component_state(channel)
    _record_channel_status(channel, current_state.get("status", "configured"), mode=mode)
    return settings


def set_channel_kill_switch(channel: str, enabled: bool) -> dict:
    settings = store_set_channel_kill_switch(channel, enabled, updated_by="operator")
    current_state = get_component_state(channel)
    current_status = current_state.get("status", "configured")
    current_channel_status = current_state.get(f"{channel}_status")
    fields = {"kill_switch": enabled}
    if enabled:
        fields[f"{channel}_status"] = "degraded"
    elif current_channel_status:
        fields[f"{channel}_status"] = current_channel_status
    _record_channel_status(channel, "degraded" if enabled else current_status, **fields)
    return settings
