from __future__ import annotations

from memory.structured.db import engine, save_event
from runtime.health.telemetry import record_component_state, utc_now_iso
from runtime.ops.agent_workloop import send_telegram
from runtime.ops.outreach_execution import (
    ensure_outreach_execution_tables,
    evaluate_auto_send_readiness,
    get_selected_outreach_for_opportunity,
    send_outreach_for_opportunity,
    sync_outreach_execution_state,
)
from sqlalchemy import text


def _render_message(sent_cases: list[dict]) -> str:
    lines = ["Meridian auto-sent high-confidence Gmail outreach"]
    for case in sent_cases[:3]:
        label = case.get("merchant_domain") or f"opportunity {case.get('opportunity_id')}"
        lines.append("")
        lines.append(f"- Opportunity {case.get('opportunity_id')} — {label}")
        lines.append(f"  Contact: {case.get('contact_email') or 'unknown'}")
        lines.append(f"  Confidence: {case.get('auto_send_confidence')}")
        lines.append(f"  Why: {case.get('why_now') or 'High-confidence merchant distress with trusted contact.'}")
        lines.append(f"  Thread: {case.get('gmail_thread_id') or 'unknown'}")
        lines.append(f"  Follow-up due: {case.get('follow_up_due_at') or 'not scheduled'}")
    return "\n".join(lines).strip()


def run_auto_send_high_confidence_outreach(send_update: bool = True, limit: int = 5) -> dict:
    sync_outreach_execution_state(limit=max(50, int(limit) * 10))
    ensure_outreach_execution_tables()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT opportunity_id
                FROM opportunity_outreach_actions
                WHERE channel = 'gmail'
                  AND status = 'draft_ready'
                  AND approval_state = 'approved'
                ORDER BY updated_at ASC
                LIMIT :limit
                """
            ),
            {"limit": max(1, int(limit) * 4)},
        ).mappings().fetchall()

    sent_cases: list[dict] = []
    skipped_cases: list[dict] = []

    for row in rows:
        opportunity_id = int(row.get("opportunity_id") or 0)
        if not opportunity_id:
            continue
        context = get_selected_outreach_for_opportunity(opportunity_id=opportunity_id)
        if context.get("error"):
            skipped_cases.append({"opportunity_id": opportunity_id, "reason": context.get("error")})
            continue

        outreach_type = str(context.get("outreach_type") or "initial_outreach").strip().lower()
        readiness = evaluate_auto_send_readiness(context, context, outreach_type=outreach_type)
        if not readiness.get("eligible"):
            skipped_cases.append(
                {
                    "opportunity_id": opportunity_id,
                    "merchant_domain": context.get("merchant_domain") or "",
                    "reason": readiness.get("reason") or "not_auto_send_ready",
                    "confidence": readiness.get("confidence") or 0.0,
                }
            )
            continue

        result = send_outreach_for_opportunity(
            opportunity_id=opportunity_id,
            approved_by="meridian",
            approval_source="auto_send_high_confidence",
        )
        if result.get("error"):
            skipped_cases.append(
                {
                    "opportunity_id": opportunity_id,
                    "merchant_domain": context.get("merchant_domain") or "",
                    "reason": result.get("error") or "send_failed",
                    "confidence": readiness.get("confidence") or 0.0,
                }
            )
            continue

        sent_cases.append(
            {
                "opportunity_id": opportunity_id,
                "merchant_domain": context.get("merchant_domain") or "",
                "contact_email": context.get("contact_email") or "",
                "auto_send_confidence": readiness.get("confidence") or 0.0,
                "why_now": context.get("why_now") or "",
                "gmail_thread_id": result.get("gmail_thread_id") or "",
                "follow_up_due_at": result.get("follow_up_due_at") or "",
            }
        )
        if len(sent_cases) >= max(1, int(limit)):
            break

    message = ""
    if send_update and sent_cases:
        message = _render_message(sent_cases)
        send_telegram(message)

    result = {
        "status": "ok",
        "checked_at": utc_now_iso(),
        "candidates_checked": len(rows),
        "auto_sent_count": len(sent_cases),
        "sent_cases": sent_cases,
        "skipped_cases": skipped_cases[:10],
        "message_sent": bool(message),
        "message_preview": message,
    }
    record_component_state(
        "auto_send_monitor",
        ttl=1800,
        last_run_at=result["checked_at"],
        last_candidates_checked=result["candidates_checked"],
        last_auto_sent_count=result["auto_sent_count"],
        last_message_sent=result["message_sent"],
    )
    save_event(
        "auto_send_monitor_run",
        {
            "candidates_checked": result["candidates_checked"],
            "auto_sent_count": result["auto_sent_count"],
            "message_sent": result["message_sent"],
        },
    )
    return result
