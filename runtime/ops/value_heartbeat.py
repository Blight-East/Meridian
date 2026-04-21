from __future__ import annotations

import json

from sqlalchemy import text

from memory.structured.db import engine, save_event
from runtime.channels.store import get_channel_metrics_snapshot
from runtime.intelligence.commercial_qualification import assess_commercial_readiness
from runtime.health.telemetry import get_component_state, record_component_state, utc_now_iso
from runtime.intelligence.opportunity_queue_quality import (
    ELIGIBILITY_OPERATOR_REVIEW_ONLY,
    ELIGIBILITY_OUTREACH,
    evaluate_opportunity_queue_quality,
)
from runtime.ops.outreach_execution import get_outreach_execution_metrics
from runtime.qualification.lead_qualifier import qualify_lead
from runtime.safety.control_plane import list_security_incidents


def _humanize_scout_reason(reason: str) -> str:
    value = str(reason or "").strip().replace("_", " ")
    return value or "unknown reason"


_SCOUT_SUPPRESSED_DOMAIN_SUFFIXES = (
    "googleapis.com",
    "gstatic.com",
    "googleusercontent.com",
    "dynamics.com",
    "onelink.me",
    "app.link",
    "linktr.ee",
    "lnk.bio",
    "myjobhelper.com",
    "brief.barchart.com",
    "play.google.com",
)
_SCOUT_SUPPRESSED_CONTENT_MARKERS = (
    "newsletter",
    "spring newsletter",
    "pre-approved",
    "you're pre-approved",
    "special offer",
    "weekly ad",
    "coupon",
    "credit warrior",
    "plan the perfect",
    "aegean escape",
    "needs a nap",
    "you are cordially invited",
    "job might be right for you",
    "ziprecruiter",
    "merchant candidate: fonts",
)

_DAILY_CANDIDATES_CONSIDERED = 20
_DAILY_TARGET_SLATE_LIMIT = 5
_DAILY_OUTREACH_WORTHY_LIMIT = 2


def _truncate_brief(text: str, limit: int = 120) -> str:
    value = " ".join(str(text or "").strip().split())
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."


def _humanize_live_thread_status(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "reply_live":
        return "a fresh merchant reply waiting"
    if normalized == "follow_up_due":
        return "an overdue follow-up waiting now"
    if normalized == "awaiting_reply":
        return "a sent thread waiting for a merchant reply"
    if normalized in {"", "none"}:
        return "no live thread"
    return normalized.replace("_", " ")


def _security_guardrail_summary(incident_payload: dict[str, object]) -> tuple[str, str]:
    incidents = list((incident_payload or {}).get("incidents") or [])
    count = int((incident_payload or {}).get("count") or 0)
    if count <= 0:
        return "", ""

    first = incidents[0] if incidents else {}
    title = _truncate_brief(str(first.get("title") or ""))
    summary = _truncate_brief(str(first.get("summary") or ""))
    incident_type = str(first.get("incident_type") or "").strip().replace("_", " ")
    source_channel = str(first.get("source_channel") or "").strip().lower()

    headline = title or summary
    if not headline:
        headline = f"{incident_type or 'security incident'} on {source_channel or 'an untrusted channel'}".strip()

    guidance = (
        f"{count} open security incident{'s' if count != 1 else ''}; review {headline} before increasing autonomy."
    )
    return guidance, headline


def _meets_signal_candidate_scout_bar(
    *,
    eligibility: str,
    icp_fit_score: int,
    quality_score: int,
) -> tuple[bool, str, str]:
    if eligibility == ELIGIBILITY_OUTREACH:
        if icp_fit_score < 60:
            return (
                False,
                "icp_fit_too_low",
                f"ICP fit only scored {icp_fit_score}, which is below the scout threshold for direct outreach.",
            )
        if quality_score < 60:
            return (
                False,
                "queue_quality_too_low",
                f"Queue quality only scored {quality_score}, so it is still too weak to surface for direct outreach.",
            )
        return (True, "", "")
    if eligibility == ELIGIBILITY_OPERATOR_REVIEW_ONLY:
        if icp_fit_score < 45:
            return (
                False,
                "icp_fit_too_low",
                f"ICP fit only scored {icp_fit_score}, so it is still too weak even for operator review.",
            )
        if quality_score < 40:
            return (
                False,
                "queue_quality_too_low",
                f"Queue quality only scored {quality_score}, so the evidence is still too weak even for operator review.",
            )
        return (True, "", "")
    return (False, "queue_class_not_actionable", "This queue class is not eligible for the scout slate.")


def _skip_obvious_scout_rejection(*, merchant_domain: str, reason: str, content_preview: str, signal_source: str) -> bool:
    domain = str(merchant_domain or "").strip().lower()
    content = str(content_preview or "").strip().lower()
    source = str(signal_source or "").strip().lower()
    if any(domain == suffix or domain.endswith(f".{suffix}") for suffix in _SCOUT_SUPPRESSED_DOMAIN_SUFFIXES):
        return True
    if any(marker in content for marker in _SCOUT_SUPPRESSED_CONTENT_MARKERS):
        return True
    if "gmail" in source and str(reason or "").strip().lower() in {
        "domain_unpromotable",
        "historical_or_hypothetical",
        "identity_weak",
        "icp_fit_too_low",
    }:
        if any(marker in content for marker in ("sender: silversea", "sender: afterpay", "sender: barchart", "newsletter")):
            return True
    return False


def _build_signal_candidate_pool(
    conn,
    *,
    limit: int,
    seen_domains: set[str] | None = None,
    rejected_limit: int = 3,
) -> tuple[list[dict], list[dict]]:
    seen_domains = set(seen_domains or set())
    rows = conn.execute(
        text(
            """
            SELECT s.id AS signal_id,
                   s.priority_score,
                   s.detected_at,
                   s.source,
                   s.content,
                   m.id AS merchant_id,
                   m.canonical_name,
                   m.domain,
                   m.domain_confidence,
                   m.confidence_score,
                   m.status AS merchant_status
            FROM signals s
            JOIN merchants m ON m.id = s.merchant_id
            LEFT JOIN qualified_leads q ON q.signal_id = s.id
            LEFT JOIN merchant_opportunities mo
              ON mo.merchant_id = m.id
             AND mo.created_at >= NOW() - INTERVAL '14 days'
            WHERE s.detected_at >= NOW() - INTERVAL '48 hours'
              AND q.signal_id IS NULL
              AND mo.id IS NULL
              AND COALESCE(m.domain, '') != ''
            ORDER BY s.detected_at DESC
            LIMIT 60
            """
        )
    ).mappings().all()

    ranked: list[tuple[float, dict]] = []
    rejected: list[tuple[float, dict]] = []
    for row in rows:
        merchant_domain = str(row.get("domain") or "").strip().lower()
        if not merchant_domain or merchant_domain in seen_domains:
            continue
        content = str(row.get("content") or "")
        quality = evaluate_opportunity_queue_quality(
            opportunity={
                "merchant_id": row.get("merchant_id"),
                "merchant_domain": merchant_domain,
                "merchant_name": row.get("canonical_name") or "",
            },
            merchant={
                "canonical_name": row.get("canonical_name") or "",
                "domain": merchant_domain,
                "domain_confidence": row.get("domain_confidence") or "",
                "status": row.get("merchant_status") or "",
                "confidence_score": row.get("confidence_score") or 0,
            },
            signal={"content": content, "source": row.get("source") or ""},
        )
        qualification = qualify_lead(content, row.get("priority_score") or 0)
        eligibility = str(quality.get("eligibility_class") or "")
        icp_fit_score = int(quality.get("icp_fit_score") or qualification.get("icp_fit_score") or 0)
        quality_score = int(quality.get("quality_score") or 0)
        source_confidence = int(row.get("confidence_score") or 0)
        commercial = assess_commercial_readiness(
            distress_type=quality.get("parsed_distress_type") or "unknown",
            processor=quality.get("parsed_processor") or qualification.get("processor") or "unknown",
            content=content,
            why_now=content[:220],
            contact_email="",
            contact_trust_score=0,
            target_roles=[],
            target_role_reason="",
            icp_fit_score=icp_fit_score,
            queue_quality_score=quality_score,
            revenue_detected=bool(qualification.get("revenue_detected")),
        )
        base_score = (
            quality_score
            + icp_fit_score
            + min(int(float(row.get("priority_score") or 0)), 30)
            + min(source_confidence, 20)
            + int(commercial.get("commercial_readiness_score") or 0)
        )
        if qualification.get("disqualifier_reason"):
            if _skip_obvious_scout_rejection(
                merchant_domain=merchant_domain,
                reason=qualification.get("disqualifier_reason") or "",
                content_preview=content[:180],
                signal_source=row.get("source") or "",
            ):
                continue
            rejected.append(
                (
                    float(base_score),
                    {
                        "merchant_domain": merchant_domain,
                        "merchant_name": row.get("canonical_name") or "",
                        "signal_id": int(row.get("signal_id") or 0),
                        "reason": qualification.get("disqualifier_reason") or "",
                        "why": f"Lead qualification rejected it as {_humanize_scout_reason(qualification.get('disqualifier_reason'))}.",
                        "content_preview": content[:180],
                        "signal_source": row.get("source") or "",
                    },
                )
            )
            continue
        if eligibility not in {ELIGIBILITY_OUTREACH, ELIGIBILITY_OPERATOR_REVIEW_ONLY}:
            if _skip_obvious_scout_rejection(
                merchant_domain=merchant_domain,
                reason=eligibility or "not_outreach_ready",
                content_preview=content[:180],
                signal_source=row.get("source") or "",
            ):
                continue
            rejected.append(
                (
                    float(base_score),
                    {
                        "merchant_domain": merchant_domain,
                        "merchant_name": row.get("canonical_name") or "",
                        "signal_id": int(row.get("signal_id") or 0),
                        "reason": eligibility or "not_outreach_ready",
                        "why": quality.get("eligibility_reason") or "Queue quality kept it out of the prospect set.",
                        "content_preview": content[:180],
                        "signal_source": row.get("source") or "",
                    },
                )
            )
            continue
        meets_scout_bar, scout_reason, scout_why = _meets_signal_candidate_scout_bar(
            eligibility=eligibility,
            icp_fit_score=icp_fit_score,
            quality_score=quality_score,
        )
        if not meets_scout_bar:
            if _skip_obvious_scout_rejection(
                merchant_domain=merchant_domain,
                reason=scout_reason,
                content_preview=content[:180],
                signal_source=row.get("source") or "",
            ):
                continue
            rejected.append(
                (
                    float(base_score),
                    {
                        "merchant_domain": merchant_domain,
                        "merchant_name": row.get("canonical_name") or "",
                        "signal_id": int(row.get("signal_id") or 0),
                        "reason": scout_reason,
                        "why": scout_why,
                        "content_preview": content[:180],
                        "signal_source": row.get("source") or "",
                    },
                )
            )
            continue

        high_conviction = bool(quality.get("high_conviction_prospect")) or (eligibility == ELIGIBILITY_OUTREACH and icp_fit_score >= 70 and quality_score >= 70)
        ranked.append(
            (
                float(base_score + (10 if high_conviction else 0)),
                {
                    "prospect_type": "signal_candidate",
                    "opportunity_id": 0,
                    "signal_id": int(row.get("signal_id") or 0),
                    "merchant_name": row.get("canonical_name") or "",
                    "merchant_domain": merchant_domain,
                    "contact_email": "",
                    "status": "signal_candidate",
                    "approval_state": "",
                    "processor": quality.get("parsed_processor") or qualification.get("processor") or "unknown",
                    "distress_topic": quality.get("parsed_distress_type") or "unknown",
                    "why_now": content[:220],
                    "content_preview": content[:220],
                    "signal_source": row.get("source") or "",
                    "queue_eligibility_reason": quality.get("eligibility_reason") or "",
                    "queue_quality_score": quality_score,
                    "selected_play": "",
                    "icp_fit_score": icp_fit_score,
                    "high_conviction_prospect": high_conviction,
                    "contact_trust_score": 0,
                    "source_confidence_score": source_confidence,
                    "urgency_score": int(commercial.get("urgency_score") or 0),
                    "urgency_label": commercial.get("urgency_label") or "low",
                    "urgency_summary": commercial.get("urgency_summary") or "",
                    "buying_authority_score": int(commercial.get("buying_authority_score") or 0),
                    "buying_authority_label": commercial.get("buying_authority_label") or "low",
                    "buying_authority_summary": commercial.get("buying_authority_summary") or "",
                    "cash_impact_score": int(commercial.get("cash_impact_score") or 0),
                    "cash_impact_label": commercial.get("cash_impact_label") or "low",
                    "cash_impact_hypothesis": commercial.get("cash_impact_hypothesis") or "",
                    "commercial_readiness_score": int(commercial.get("commercial_readiness_score") or 0),
                    "commercial_readiness_label": commercial.get("commercial_readiness_label") or "low",
                },
            )
        )
        seen_domains.add(merchant_domain)

    ranked.sort(key=lambda item: item[0], reverse=True)
    rejected.sort(key=lambda item: item[0], reverse=True)
    return (
        [row for _, row in ranked[: max(1, int(limit))]],
        [row for _, row in rejected[: max(1, int(rejected_limit))]],
    )


def _recent_high_conviction_prospects(conn, limit: int = 3) -> list[dict]:
    query_limit = max(max(1, int(limit)) * 3, 8)
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT ON (oa.opportunity_id)
                   oa.opportunity_id,
                   oa.merchant_domain,
                   oa.contact_email,
                   oa.status,
                   oa.approval_state,
                   oa.updated_at,
                   mo.processor,
                   mo.distress_topic,
                   COALESCE(NULLIF(oa.metadata_json ->> 'why_now', ''), oa.notes, '') AS why_now,
                   COALESCE(NULLIF(oa.metadata_json ->> 'queue_eligibility_reason', ''), '') AS queue_eligibility_reason,
                   COALESCE(NULLIF(oa.metadata_json ->> 'selected_play', ''), '') AS selected_play,
                   COALESCE((oa.metadata_json ->> 'icp_fit_score')::int, 0) AS icp_fit_score,
                    COALESCE((oa.metadata_json ->> 'high_conviction_prospect')::boolean, FALSE) AS high_conviction_prospect,
                   COALESCE((oa.metadata_json ->> 'contact_trust_score')::int, 0) AS contact_trust_score,
                   COALESCE(oa.metadata_json ->> 'queue_eligibility_class', '') AS queue_eligibility_class
            FROM opportunity_outreach_actions oa
            JOIN merchant_opportunities mo ON mo.id = oa.opportunity_id
            WHERE COALESCE(oa.updated_at, oa.created_at) >= NOW() - INTERVAL '24 hours'
              AND oa.contact_email IS NOT NULL
              AND oa.contact_email != ''
              AND oa.status NOT IN ('sent', 'replied', 'follow_up_needed')
              AND COALESCE(oa.metadata_json ->> 'queue_eligibility_class', '') = 'outreach_eligible'
              AND (
                    COALESCE((oa.metadata_json ->> 'high_conviction_prospect')::boolean, FALSE) = TRUE
                    OR COALESCE((oa.metadata_json ->> 'icp_fit_score')::int, 0) >= 70
                  )
            ORDER BY oa.opportunity_id,
                     COALESCE((oa.metadata_json ->> 'high_conviction_prospect')::boolean, FALSE) DESC,
                     COALESCE((oa.metadata_json ->> 'icp_fit_score')::int, 0) DESC,
                     COALESCE((oa.metadata_json ->> 'contact_trust_score')::int, 0) DESC,
                     COALESCE(oa.updated_at, oa.created_at) DESC
            LIMIT :limit
            """
        ),
        {"limit": query_limit},
    ).mappings().all()
    prospects = [
        {
            "prospect_type": "outreach_candidate",
            "opportunity_id": int(row.get("opportunity_id") or 0),
            "signal_id": 0,
            "merchant_name": "",
            "merchant_domain": row.get("merchant_domain") or "",
            "contact_email": row.get("contact_email") or "",
            "status": row.get("status") or "unknown",
            "approval_state": row.get("approval_state") or "",
            "processor": row.get("processor") or "unknown",
            "distress_topic": row.get("distress_topic") or "unknown",
            "why_now": row.get("why_now") or "",
            "content_preview": "",
            "signal_source": "",
            "queue_eligibility_reason": row.get("queue_eligibility_reason") or "",
            "queue_quality_score": 0,
            "selected_play": row.get("selected_play") or "",
            "icp_fit_score": int(row.get("icp_fit_score") or 0),
            "high_conviction_prospect": bool(row.get("high_conviction_prospect")),
            "contact_trust_score": int(row.get("contact_trust_score") or 0),
            "source_confidence_score": 0,
        }
        for row in rows
    ]
    for prospect in prospects:
        commercial = assess_commercial_readiness(
            distress_type=prospect.get("distress_topic") or "unknown",
            processor=prospect.get("processor") or "unknown",
            content=prospect.get("why_now") or "",
            why_now=prospect.get("why_now") or "",
            contact_email=prospect.get("contact_email") or "",
            contact_trust_score=int(prospect.get("contact_trust_score") or 0),
            target_roles=[],
            target_role_reason="",
            icp_fit_score=int(prospect.get("icp_fit_score") or 0),
            queue_quality_score=75 if bool(prospect.get("high_conviction_prospect")) else 60,
            revenue_detected=False,
        )
        prospect.update(commercial)
    prospects.sort(
        key=lambda row: (
            int(row.get("commercial_readiness_score") or 0),
            int(row.get("icp_fit_score") or 0),
            int(row.get("contact_trust_score") or 0),
            1 if row.get("high_conviction_prospect") else 0,
        ),
        reverse=True,
    )
    seen_domains = {str(row.get("merchant_domain") or "").strip().lower() for row in prospects if str(row.get("merchant_domain") or "").strip()}
    if len(prospects) < query_limit:
        fallback, _ = _build_signal_candidate_pool(conn, limit=query_limit - len(prospects), seen_domains=seen_domains)
        prospects.extend(fallback)
    return prospects[:query_limit]


def _prospect_scout_report(conn, limit: int = 3) -> list[dict]:
    _, rejected = _build_signal_candidate_pool(conn, limit=1, rejected_limit=max(1, int(limit)))
    return rejected


def _daily_target_stage(row: dict) -> str:
    contact_email = str(row.get("contact_email") or "").strip()
    contact_trust = int(row.get("contact_trust_score") or 0)
    commercial = int(row.get("commercial_readiness_score") or 0)
    authority = int(row.get("buying_authority_score") or 0)
    urgency = int(row.get("urgency_score") or 0)
    cash_impact = int(row.get("cash_impact_score") or 0)
    distress = str(row.get("distress_topic") or "").strip().lower()
    if (
        contact_email
        and distress not in {"", "unknown"}
        and contact_trust >= 70
        and commercial >= 68
        and authority >= 45
        and urgency >= 60
        and cash_impact >= 58
    ):
        return "outreach_worthy"
    return "review_only"


def _daily_target_rejection(row: dict) -> tuple[str, str]:
    distress = str(row.get("distress_topic") or "").strip().lower()
    commercial = int(row.get("commercial_readiness_score") or 0)
    authority = int(row.get("buying_authority_score") or 0)
    urgency = int(row.get("urgency_score") or 0)
    cash_impact = int(row.get("cash_impact_score") or 0)
    icp_fit = int(row.get("icp_fit_score") or 0)
    queue_quality = int(row.get("queue_quality_score") or 0)
    contact_email = str(row.get("contact_email") or "").strip()
    if distress in {"", "unknown"}:
        return (
            "distress_unclassified",
            "The distress is still too vague to treat as a revenue target.",
        )
    if commercial < 58:
        return (
            "commercial_readiness_too_low",
            f"Commercial readiness only scored {commercial}, so it still looks interesting rather than commercially ready.",
        )
    if urgency < 58:
        return (
            "urgency_too_low",
            f"Urgency only scored {urgency}, so the pain does not look active enough yet.",
        )
    if cash_impact < 55:
        return (
            "cash_impact_too_low",
            f"Cash-impact only scored {cash_impact}, so the downside still looks too soft for a top slate slot.",
        )
    if icp_fit < 68:
        return (
            "icp_fit_too_low",
            f"ICP fit only scored {icp_fit}, which is below Meridian's revenue-target bar.",
        )
    if queue_quality and queue_quality < 60:
        return (
            "queue_quality_too_low",
            f"Queue quality only scored {queue_quality}, so the evidence is still too weak.",
        )
    if row.get("prospect_type") != "signal_candidate" and not contact_email:
        return (
            "no_contact_path",
            "There is no trusted contact path yet, so Meridian cannot turn this into outreach cleanly.",
        )
    if contact_email and authority < 35:
        return (
            "buying_authority_too_low",
            f"Buying authority only scored {authority}, so the contact path still looks weak.",
        )
    return (
        "not_top_slate",
        "It passed basic qualification, but it still is not strong enough to make Meridian's daily target slate.",
    )


def _build_daily_target_slate(conn) -> dict:
    considered = _recent_high_conviction_prospects(conn, limit=_DAILY_CANDIDATES_CONSIDERED)[:_DAILY_CANDIDATES_CONSIDERED]
    survivors: list[tuple[float, dict]] = []
    almost: list[tuple[float, dict]] = []
    for row in considered:
        row_data = dict(row)
        reject_reason, reject_why = _daily_target_rejection(row_data)
        if reject_reason == "not_top_slate":
            row_data["target_stage"] = _daily_target_stage(row_data)
            row_data["target_reason"] = (
                "This is strong enough for direct outreach now."
                if row_data["target_stage"] == "outreach_worthy"
                else "This is strong enough to stay on the review slate, but not strong enough for direct outreach yet."
            )
            score = (
                int(row_data.get("commercial_readiness_score") or 0) * 3
                + int(row_data.get("urgency_score") or 0) * 2
                + int(row_data.get("buying_authority_score") or 0)
                + int(row_data.get("cash_impact_score") or 0)
                + int(row_data.get("contact_trust_score") or 0)
            )
            survivors.append((float(score), row_data))
            continue
        almost.append(
            (
                float(
                    int(row_data.get("commercial_readiness_score") or 0)
                    + int(row_data.get("urgency_score") or 0)
                    + int(row_data.get("cash_impact_score") or 0)
                ),
                {
                    "merchant_domain": row_data.get("merchant_domain") or "",
                    "merchant_name": row_data.get("merchant_name") or "",
                    "signal_id": int(row_data.get("signal_id") or 0),
                    "reason": reject_reason,
                    "why": reject_why,
                    "content_preview": row_data.get("content_preview") or row_data.get("why_now") or "",
                    "signal_source": row_data.get("signal_source") or "",
                },
            )
        )

    survivors.sort(key=lambda item: item[0], reverse=True)
    slate = [row for _, row in survivors[:_DAILY_TARGET_SLATE_LIMIT]]
    outreach_worthy = [row for row in slate if row.get("target_stage") == "outreach_worthy"][:_DAILY_OUTREACH_WORTHY_LIMIT]

    if len(almost) < 3:
        fallback_scout = _prospect_scout_report(conn, limit=3)
        for case in fallback_scout:
            almost.append(
                (
                    0.0,
                    {
                        "merchant_domain": case.get("merchant_domain") or "",
                        "merchant_name": case.get("merchant_name") or "",
                        "signal_id": int(case.get("signal_id") or 0),
                        "reason": case.get("reason") or "",
                        "why": case.get("why") or "",
                        "content_preview": case.get("content_preview") or "",
                        "signal_source": case.get("signal_source") or "",
                    },
                )
            )
    almost.sort(key=lambda item: item[0], reverse=True)
    seen = set()
    scout: list[dict] = []
    for _, row in almost:
        key = (
            str(row.get("merchant_domain") or "").strip().lower(),
            int(row.get("signal_id") or 0),
            str(row.get("reason") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        scout.append(row)
        if len(scout) >= 3:
            break

    return {
        "candidates_considered": len(considered),
        "target_slate": slate,
        "outreach_worthy_targets": outreach_worthy,
        "scout_report": scout,
    }


def _live_thread_snapshot(conn) -> dict:
    row = conn.execute(
        text(
            """
            SELECT oa.opportunity_id,
                   oa.merchant_domain,
                   oa.contact_email,
                   oa.status,
                   oa.approval_state,
                   oa.gmail_thread_id,
                   oa.sent_at,
                   oa.replied_at,
                   oa.follow_up_due_at
            FROM opportunity_outreach_actions oa
            WHERE (
                    oa.status IN ('sent', 'replied', 'follow_up_needed')
                    OR (oa.status = 'draft_ready' AND oa.approval_state = 'sent')
                  )
              AND oa.gmail_thread_id IS NOT NULL
              AND oa.gmail_thread_id != ''
            ORDER BY
                CASE
                    WHEN oa.status = 'replied' THEN 0
                    WHEN oa.status = 'follow_up_needed' THEN 1
                    ELSE 2
                END,
                COALESCE(oa.replied_at, oa.sent_at, oa.updated_at, oa.created_at) DESC
            LIMIT 1
            """
        )
    ).mappings().first()
    if not row:
        return {
            "present": False,
            "status": "none",
            "merchant_domain": "",
            "contact_email": "",
            "gmail_thread_id": "",
            "follow_up_due_at": "",
            "protected": False,
            "next_step": "Surface the next real merchant thread.",
        }

    status = str(row.get("status") or "unknown").strip().lower()
    follow_up_due_at = row.get("follow_up_due_at")
    if status == "replied":
        label = "reply_live"
        next_step = "Review the reply immediately and draft the next response."
        protected = True
    elif status == "follow_up_needed":
        label = "follow_up_due"
        next_step = "Send the scheduled follow-up on time so the thread does not go cold."
        protected = True
    else:
        label = "awaiting_reply"
        next_step = (
            "Monitor for a reply and be ready to follow up on the due date."
            if follow_up_due_at
            else "Monitor for a reply and schedule the next follow-up."
        )
        protected = bool(row.get("gmail_thread_id"))

    return {
        "present": True,
        "opportunity_id": int(row.get("opportunity_id") or 0),
        "status": label,
        "merchant_domain": row.get("merchant_domain") or "",
        "contact_email": row.get("contact_email") or "",
        "gmail_thread_id": row.get("gmail_thread_id") or "",
        "follow_up_due_at": follow_up_due_at.isoformat() if follow_up_due_at else "",
        "protected": protected,
        "next_step": next_step,
    }


def _suppression_snapshot(conn) -> dict:
    counts = conn.execute(
        text(
            """
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'lead_qualification_suppressed') AS lead_suppressed,
                COUNT(*) FILTER (WHERE event_type = 'opportunity_extraction_suppressed') AS opportunity_suppressed
            FROM events
            WHERE created_at >= NOW() - INTERVAL '24 hours'
              AND event_type IN ('lead_qualification_suppressed', 'opportunity_extraction_suppressed')
            """
        )
    ).mappings().first() or {}
    reason_row = conn.execute(
        text(
            """
            SELECT COALESCE(
                       NULLIF(data ->> 'disqualifier_reason', ''),
                       NULLIF(data ->> 'suppression_reason', ''),
                       NULLIF(data ->> 'reason', ''),
                       'unknown'
                   ) AS reason,
                   COUNT(*) AS count
            FROM events
            WHERE created_at >= NOW() - INTERVAL '24 hours'
              AND event_type IN ('lead_qualification_suppressed', 'opportunity_extraction_suppressed')
            GROUP BY 1
            ORDER BY COUNT(*) DESC, reason ASC
            LIMIT 1
            """
        )
    ).mappings().first() or {}
    lead_suppressed = int(counts.get("lead_suppressed") or 0)
    opportunity_suppressed = int(counts.get("opportunity_suppressed") or 0)
    return {
        "suppressed_leads_24h": lead_suppressed,
        "suppressed_opportunities_24h": opportunity_suppressed,
        "queue_sharpened_24h": lead_suppressed + opportunity_suppressed,
        "top_suppression_reason": reason_row.get("reason") or "",
    }


def collect_value_metrics() -> dict:
    with engine.connect() as conn:
        leads_24h = conn.execute(
            text("SELECT COUNT(*) FROM qualified_leads WHERE created_at >= NOW() - INTERVAL '24 hours'")
        ).scalar() or 0
        opps_pending = conn.execute(
            text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'pending_review'")
        ).scalar() or 0
        opps_approved = conn.execute(
            text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'approved'")
        ).scalar() or 0
        opps_sent = conn.execute(
            text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'outreach_sent'")
        ).scalar() or 0
        opps_converted = conn.execute(
            text("SELECT COUNT(*) FROM merchant_opportunities WHERE status = 'converted'")
        ).scalar() or 0
        drafts_24h = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM channel_action_audit_log
                WHERE action_type = 'draft' AND created_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        sends_24h = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM channel_action_audit_log
                WHERE action_type = 'send' AND sent_at >= NOW() - INTERVAL '24 hours'
                """
            )
        ).scalar() or 0
        live_thread = _live_thread_snapshot(conn)
        target_slate = _build_daily_target_slate(conn)
        suppression = _suppression_snapshot(conn)

    channel_snapshot = get_channel_metrics_snapshot()
    outreach_metrics = get_outreach_execution_metrics(refresh_state=False)
    open_incident_payload = list_security_incidents(limit=3, status="open")
    open_incidents = int(open_incident_payload.get("count", 0) or 0)
    conversion_rate = round((float(opps_converted) / float(opps_sent) * 100.0), 2) if opps_sent else 0.0
    follow_ups_due = int(outreach_metrics.get("follow_ups_due", 0) or 0)
    replies_open = int(outreach_metrics.get("outreach_replied_open", 0) or 0)
    with engine.connect() as conn:
        outcome_review_ready = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM opportunity_outreach_actions oa
                JOIN gmail_thread_intelligence gti
                  ON gti.thread_id = oa.gmail_thread_id
                WHERE oa.status = 'replied'
                  AND gti.thread_category = 'noise_system'
                  AND COALESCE((gti.metadata_json ->> 'sender_is_automated')::boolean, FALSE) = TRUE
                """
            )
        ).scalar() or 0
        ready_to_send = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM opportunity_outreach_actions
                WHERE status = 'draft_ready'
                  AND approval_state = 'approved'
                """
            )
        ).scalar() or 0
    drafts_waiting = int(outreach_metrics.get("outreach_awaiting_approval", 0) or 0)
    send_ready = int(outreach_metrics.get("send_eligible_leads", 0) or 0)
    action_ready_count = int(ready_to_send) + follow_ups_due + replies_open + send_ready
    overnight_successes: list[str] = []
    overnight_failures: list[str] = []

    if live_thread.get("present") and live_thread.get("protected"):
        overnight_successes.append("live_thread_protected")
    elif live_thread.get("present"):
        overnight_failures.append("live_thread_unprotected")

    recent_prospects = list(target_slate.get("target_slate") or [])
    outreach_worthy_targets = list(target_slate.get("outreach_worthy_targets") or [])
    if outreach_worthy_targets:
        overnight_successes.append("new_real_prospects")
    else:
        overnight_failures.append("no_new_real_prospects")

    if int(suppression.get("queue_sharpened_24h") or 0) > 0:
        overnight_successes.append("queue_sharpened")
    else:
        overnight_failures.append("no_queue_sharpening")

    if action_ready_count > 0:
        overnight_successes.append("action_ready")
    else:
        overnight_failures.append("no_action_ready")

    if len(overnight_successes) >= 3:
        overnight_status = "healthy"
    elif overnight_successes:
        overnight_status = "partial"
    else:
        overnight_status = "stalled"

    primary_bottleneck = "queue_generation"
    next_revenue_move = "Create one credible opportunity."
    security_guardrail_active = open_incidents > 0
    security_guidance, security_guardrail_headline = _security_guardrail_summary(open_incident_payload)
    if outcome_review_ready > 0:
        primary_bottleneck = "outcome_capture"
        next_revenue_move = "Apply the high-confidence suggested outcome on the bounce-like reply threads."
    elif replies_open > 0:
        primary_bottleneck = "reply_review"
        next_revenue_move = "Review the strongest merchant reply and draft the next response while the conversation is live."
    elif follow_ups_due > 0:
        primary_bottleneck = "follow_up_execution"
        next_revenue_move = "Send the overdue follow-up on the strongest replied or sent case."
    elif ready_to_send > 0:
        primary_bottleneck = "send_execution"
        next_revenue_move = "Review the strongest approved draft and send it."
    elif int(outreach_metrics.get("blocked_no_contact", 0) or 0) > 0 and send_ready <= 0:
        primary_bottleneck = "contact_acquisition"
        next_revenue_move = "Find one trusted same-domain merchant contact so a real outreach send can happen."
    elif drafts_waiting > 0:
        primary_bottleneck = "operator_approval"
        next_revenue_move = "Review, tighten, and approve the strongest outreach draft."
    elif send_ready > 0:
        primary_bottleneck = "draft_creation"
        next_revenue_move = "Turn the best send-eligible lead into an approval-ready draft."
    elif opps_pending > 0:
        primary_bottleneck = "opportunity_qualification"
        next_revenue_move = "Promote one pending opportunity into a specific operator action."
    elif leads_24h <= 0:
        primary_bottleneck = "lead_generation"
        next_revenue_move = "Restore fresh qualified lead generation."

    return {
        "measured_at": utc_now_iso(),
        "qualified_leads_24h": int(leads_24h),
        "opportunities_pending_review": int(opps_pending),
        "opportunities_approved": int(opps_approved),
        "opportunities_outreach_sent": int(opps_sent),
        "opportunities_converted": int(opps_converted),
        "channel_drafts_24h": int(drafts_24h),
        "channel_sends_24h": int(sends_24h),
        "approval_queue_depth": int(channel_snapshot.get("approval_queue_depth", 0) or 0),
        "action_queue_depth": int(channel_snapshot.get("action_queue_depth", 0) or 0),
        "open_security_incidents": int(open_incidents),
        "autonomy_guardrail_active": bool(security_guardrail_active),
        "security_guidance": security_guidance,
        "security_guardrail_headline": security_guardrail_headline,
        "conversion_rate_percent": conversion_rate,
        "outreach_awaiting_approval": drafts_waiting,
        "reply_review_needed": replies_open,
        "outcome_review_ready": int(outcome_review_ready),
        "follow_ups_due": follow_ups_due,
        "outreach_ready_to_send": int(ready_to_send),
        "send_eligible_leads": send_ready,
        "reachable_contact_leads": send_ready,
        "send_blocked_no_contact": int(outreach_metrics.get("blocked_no_contact", 0) or 0),
        "send_blocked_weak_contact": int(outreach_metrics.get("blocked_weak_contact", 0) or 0),
        "contact_blocked_opportunities": int(outreach_metrics.get("blocked_no_contact", 0) or 0)
        + int(outreach_metrics.get("blocked_weak_contact", 0) or 0),
        "suppressed_from_blocked_queue_24h": int(outreach_metrics.get("suppressed_from_blocked_queue_24h", 0) or 0),
        "revenue_truth": {
            "sendable_leads": send_ready,
            "contact_blocked": int(outreach_metrics.get("blocked_no_contact", 0) or 0)
            + int(outreach_metrics.get("blocked_weak_contact", 0) or 0),
            "approved_to_send": int(ready_to_send),
            "drafts_waiting": drafts_waiting,
            "sends_24h": int(sends_24h),
            "replies_open": replies_open,
            "wins": int(opps_converted),
        },
        "live_thread_present": bool(live_thread.get("present")),
        "live_thread_status": live_thread.get("status") or "none",
        "live_thread_status_label": _humanize_live_thread_status(live_thread.get("status") or "none"),
        "live_thread_merchant_domain": live_thread.get("merchant_domain") or "",
        "live_thread_contact_email": live_thread.get("contact_email") or "",
        "live_thread_follow_up_due_at": live_thread.get("follow_up_due_at") or "",
        "live_thread_protected": bool(live_thread.get("protected")),
        "live_thread_next_step": live_thread.get("next_step") or "",
        "daily_candidates_considered": int(target_slate.get("candidates_considered") or 0),
        "overnight_new_real_prospects": len(outreach_worthy_targets),
        "overnight_best_new_prospects": recent_prospects,
        "daily_target_slate": recent_prospects,
        "daily_outreach_worthy_targets": outreach_worthy_targets,
        "daily_outreach_worthy_count": len(outreach_worthy_targets),
        "prospect_scout_report": list(target_slate.get("scout_report") or []),
        "overnight_queue_sharpened_24h": int(suppression.get("queue_sharpened_24h") or 0),
        "overnight_top_suppression_reason": suppression.get("top_suppression_reason") or "",
        "overnight_action_ready_count": action_ready_count,
        "overnight_contract_status": overnight_status,
        "overnight_successes": overnight_successes,
        "overnight_failures": overnight_failures,
        "primary_bottleneck": primary_bottleneck,
        "next_revenue_move": next_revenue_move,
        "active_revenue_loop": "qualify -> draft -> approve -> send -> follow_up -> close",
    }


def record_value_heartbeat() -> dict:
    metrics = collect_value_metrics()
    record_component_state("value", ttl=900, **metrics)
    save_event("value_heartbeat_recorded", metrics)
    return metrics


def get_value_heartbeat(refresh: bool = False) -> dict:
    if refresh:
        return record_value_heartbeat()
    state = get_component_state("value")
    if state:
        return state
    return record_value_heartbeat()
