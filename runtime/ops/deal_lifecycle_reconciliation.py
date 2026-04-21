from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from memory.structured.db import engine, save_event
from runtime.health.telemetry import record_component_state, utc_now_iso
from runtime.ops.deal_lifecycle import (
    DealStage,
    _infer_stage_from_existing,
)

logger = logging.getLogger("meridian.deal_lifecycle.reconciliation")

_ACTIVE_OPPORTUNITY_STATUSES = {"pending_review", "approved", "outreach_pending", "outreach_sent"}
_STALE_OPPORTUNITY_STATUSES = {"rejected", "revoked", "revoked_non_aligned"}
_TERMINAL_STAGES = {
    DealStage.outcome_won.value,
    DealStage.outcome_lost.value,
    DealStage.outcome_ignored.value,
}


def reconcile_deal_lifecycle(*, limit: int = 500) -> dict[str, Any]:
    """Compare lifecycle rows against legacy opportunity/outreach state."""
    with engine.connect() as conn:
        lifecycle_total = int(conn.execute(text("SELECT COUNT(*) FROM deal_lifecycle")).scalar() or 0)
        opportunities_total = int(conn.execute(text("SELECT COUNT(*) FROM merchant_opportunities")).scalar() or 0)
        rows = conn.execute(
            text(
                """
                SELECT
                    mo.id AS opportunity_id,
                    mo.status AS opp_status,
                    mo.merchant_domain,
                    mo.processor AS opp_processor,
                    mo.distress_topic,
                    oa.status AS outreach_status,
                    oa.approval_state AS outreach_approval,
                    oa.outcome_status,
                    oa.contact_email,
                    dl.current_stage,
                    dl.priority_score,
                    dl.processor AS lifecycle_processor,
                    dl.distress_type AS lifecycle_distress_type,
                    dl.contact_trust_score,
                    dl.icp_fit_score,
                    dl.commercial_readiness_score
                FROM merchant_opportunities mo
                LEFT JOIN opportunity_outreach_actions oa ON oa.opportunity_id = mo.id
                LEFT JOIN deal_lifecycle dl ON dl.opportunity_id = mo.id
                ORDER BY mo.created_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()

    missing_active = 0
    rejected_active = 0
    stage_mismatches = 0
    sparse_active = 0
    mismatch_examples: list[dict[str, Any]] = []

    for row in rows:
        opp_status = str(row.get("opp_status") or "").strip().lower()
        lifecycle_stage = str(row.get("current_stage") or "").strip().lower()
        expected_stage = _infer_stage_from_existing(
            opp_status=opp_status,
            outreach_status=row.get("outreach_status"),
            outreach_approval=row.get("outreach_approval"),
            outcome_status=row.get("outcome_status"),
        ).value

        if opp_status in _ACTIVE_OPPORTUNITY_STATUSES and not lifecycle_stage:
            missing_active += 1
            if len(mismatch_examples) < 10:
                mismatch_examples.append(
                    {
                        "opportunity_id": int(row["opportunity_id"]),
                        "merchant_domain": row.get("merchant_domain") or "",
                        "issue": "missing_lifecycle_row",
                        "opp_status": opp_status,
                    }
                )
            continue

        if lifecycle_stage and lifecycle_stage not in _TERMINAL_STAGES:
            if opp_status == "rejected":
                rejected_active += 1
                if len(mismatch_examples) < 10:
                    mismatch_examples.append(
                        {
                            "opportunity_id": int(row["opportunity_id"]),
                            "merchant_domain": row.get("merchant_domain") or "",
                            "issue": "rejected_active_in_lifecycle",
                            "lifecycle_stage": lifecycle_stage,
                        }
                    )

            if lifecycle_stage != expected_stage:
                stage_mismatches += 1
                if len(mismatch_examples) < 10:
                    mismatch_examples.append(
                        {
                            "opportunity_id": int(row["opportunity_id"]),
                            "merchant_domain": row.get("merchant_domain") or "",
                            "issue": "stage_mismatch",
                            "expected_stage": expected_stage,
                            "lifecycle_stage": lifecycle_stage,
                            "opp_status": opp_status,
                            "outreach_status": row.get("outreach_status") or "",
                            "outcome_status": row.get("outcome_status") or "",
                        }
                    )

            if (
                str(row.get("lifecycle_processor") or "unknown").strip().lower() == "unknown"
                or str(row.get("lifecycle_distress_type") or "unknown").strip().lower() == "unknown"
                or int(row.get("contact_trust_score") or 0) == 0
                or int(row.get("icp_fit_score") or 0) == 0
                or int(row.get("commercial_readiness_score") or 0) == 0
            ):
                sparse_active += 1

    drift_total = missing_active + rejected_active + stage_mismatches
    status = "healthy" if drift_total == 0 else ("warning" if drift_total < 25 else "degraded")
    coverage = round(100 * lifecycle_total / max(opportunities_total, 1), 1)

    result = {
        "status": status,
        "checked_rows": len(rows),
        "lifecycle_total": lifecycle_total,
        "opportunities_total": opportunities_total,
        "coverage_percent": coverage,
        "missing_active": missing_active,
        "rejected_active": rejected_active,
        "stage_mismatches": stage_mismatches,
        "sparse_active_rows": sparse_active,
        "examples": mismatch_examples,
        "checked_at": utc_now_iso(),
    }

    record_component_state(
        "deal_lifecycle_reconciliation",
        ttl=3600,
        reconciliation_status=status,
        reconciliation_checked_rows=len(rows),
        lifecycle_total=lifecycle_total,
        opportunities_total=opportunities_total,
        lifecycle_coverage_percent=coverage,
        missing_active=missing_active,
        rejected_active=rejected_active,
        stage_mismatches=stage_mismatches,
        sparse_active_rows=sparse_active,
        last_checked_at=result["checked_at"],
    )
    save_event("deal_lifecycle_reconciliation", result)
    logger.info("deal_lifecycle reconciliation: %s", result)
    return result


def cleanup_stale_lifecycle_deals(*, limit: int = 100) -> dict[str, Any]:
    """Safely retire legacy ghost deals that are already revoked/rejected and have no live outreach history."""
    from runtime.ops.deal_lifecycle import transition_deal

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    dl.opportunity_id,
                    dl.merchant_domain,
                    dl.current_stage,
                    mo.status AS opp_status,
                    oa.status AS outreach_status,
                    oa.sent_message_id,
                    oa.gmail_thread_id
                FROM deal_lifecycle dl
                JOIN merchant_opportunities mo ON mo.id = dl.opportunity_id
                LEFT JOIN opportunity_outreach_actions oa ON oa.opportunity_id = dl.opportunity_id
                WHERE dl.current_stage IN ('signal_detected', 'lead_qualified', 'opportunity_created')
                  AND LOWER(COALESCE(mo.status, '')) IN ('rejected', 'revoked', 'revoked_non_aligned')
                  AND COALESCE(oa.sent_message_id, '') = ''
                  AND COALESCE(oa.gmail_thread_id, '') = ''
                  AND (
                        oa.status IS NULL
                        OR oa.status = ''
                        OR oa.status = 'no_outreach'
                  )
                ORDER BY dl.priority_score DESC, dl.updated_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()

    cleaned = 0
    errors = 0
    examples: list[dict[str, Any]] = []
    for row in rows:
        try:
            transition_deal(
                int(row["opportunity_id"]),
                DealStage.outcome_ignored,
                actor="lifecycle_hygiene",
                metadata={
                    "cleanup_reason": "legacy_revoked_or_rejected_without_live_outreach",
                    "legacy_opportunity_status": str(row.get("opp_status") or ""),
                },
            )
            cleaned += 1
            if len(examples) < 10:
                examples.append(
                    {
                        "opportunity_id": int(row["opportunity_id"]),
                        "merchant_domain": row.get("merchant_domain") or "",
                        "legacy_status": row.get("opp_status") or "",
                    }
                )
        except Exception as exc:
            errors += 1
            logger.warning("lifecycle cleanup error for opp %s: %s", row.get("opportunity_id"), exc)

    result = {
        "status": "ok" if errors == 0 else "warning",
        "scanned": len(rows),
        "cleaned": cleaned,
        "errors": errors,
        "examples": examples,
        "cleaned_at": utc_now_iso(),
    }
    record_component_state(
        "deal_lifecycle_cleanup",
        ttl=3600,
        last_cleanup_at=result["cleaned_at"],
        last_cleanup_scanned=len(rows),
        last_cleanup_cleaned=cleaned,
        last_cleanup_errors=errors,
    )
    save_event("deal_lifecycle_cleanup", result)
    logger.info("deal_lifecycle cleanup: %s", result)
    return result


def repair_unknown_distress_deals(*, limit: int = 150) -> dict[str, Any]:
    """Repair active lifecycle rows whose distress is still unknown using deterministic evidence."""
    from runtime.intelligence.distress_normalization import normalize_distress_topic
    from runtime.ops.deal_lifecycle import refresh_deal_details
    from runtime.ops.outreach_execution import _infer_distress_type

    play_to_distress = {
        "compliance_remediation": "verification_review",
        "chargeback_mitigation": "chargeback_issue",
        "payout_acceleration": "payouts_delayed",
        "reserve_negotiation": "reserve_hold",
        "onboarding_assistance": "processor_switch_intent",
    }

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    dl.opportunity_id,
                    dl.merchant_domain,
                    dl.current_stage,
                    dl.processor,
                    dl.contact_trust_score,
                    dl.icp_fit_score,
                    dl.commercial_readiness_score,
                    COALESCE(mo.distress_topic, '') AS opp_distress_topic,
                    COALESCE(oa.selected_play, '') AS selected_play,
                    LEFT(COALESCE(s.content, ''), 4000) AS signal_content
                FROM deal_lifecycle dl
                LEFT JOIN merchant_opportunities mo ON mo.id = dl.opportunity_id
                LEFT JOIN opportunity_outreach_actions oa ON oa.opportunity_id = dl.opportunity_id
                LEFT JOIN LATERAL (
                    SELECT s2.content
                    FROM signals s2
                    WHERE s2.merchant_id = dl.merchant_id
                       OR s2.id IN (
                            SELECT ms.signal_id
                            FROM merchant_signals ms
                            WHERE ms.merchant_id = dl.merchant_id
                       )
                    ORDER BY s2.detected_at DESC
                    LIMIT 1
                ) s ON TRUE
                WHERE dl.current_stage NOT IN ('outcome_won', 'outcome_lost', 'outcome_ignored')
                  AND dl.distress_type = 'unknown'
                ORDER BY dl.priority_score DESC, dl.updated_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()

    repaired = 0
    errors = 0
    examples: list[dict[str, Any]] = []
    for row in rows:
        opp_distress_topic = str(row.get("opp_distress_topic") or "")
        selected_play = str(row.get("selected_play") or "").strip().lower()
        signal_content = str(row.get("signal_content") or "")

        inferred = normalize_distress_topic(opp_distress_topic)
        if inferred == "unknown":
            inferred = normalize_distress_topic(_infer_distress_type(signal_content))
        if inferred == "unknown" and selected_play in play_to_distress:
            inferred = play_to_distress[selected_play]
        if inferred == "unknown":
            continue

        try:
            refreshed = refresh_deal_details(
                int(row["opportunity_id"]),
                actor="distress_repair",
                update_fields={
                    "distress_type": inferred,
                    "merchant_domain": row.get("merchant_domain") or "",
                    "processor": row.get("processor") or "unknown",
                    "contact_trust_score": int(row.get("contact_trust_score") or 0),
                    "icp_fit_score": int(row.get("icp_fit_score") or 0),
                    "commercial_readiness_score": int(row.get("commercial_readiness_score") or 0),
                },
                metadata={
                    "repair_reason": "deterministic_unknown_distress_repair",
                    "source_opp_distress_topic": opp_distress_topic,
                    "source_selected_play": selected_play,
                },
            )
            repaired += 1
            if len(examples) < 10:
                examples.append(
                    {
                        "opportunity_id": int(row["opportunity_id"]),
                        "merchant_domain": row.get("merchant_domain") or "",
                        "distress_type": refreshed.get("distress_type") or inferred,
                    }
                )
        except Exception as exc:
            errors += 1
            logger.warning("unknown distress repair error for opp %s: %s", row.get("opportunity_id"), exc)

    result = {
        "status": "ok" if errors == 0 else "warning",
        "scanned": len(rows),
        "repaired": repaired,
        "errors": errors,
        "examples": examples,
        "repaired_at": utc_now_iso(),
    }
    record_component_state(
        "deal_lifecycle_distress_repair",
        ttl=3600,
        last_repair_at=result["repaired_at"],
        last_repair_scanned=len(rows),
        last_repair_repaired=repaired,
        last_repair_errors=errors,
    )
    save_event("deal_lifecycle_unknown_distress_repair", result)
    logger.info("deal_lifecycle unknown distress repair: %s", result)
    return result


def cleanup_generic_unknown_distress_deals(*, limit: int = 150) -> dict[str, Any]:
    """Retire generic early-stage complaint noise that still has no concrete distress evidence."""
    from runtime.intelligence.distress_normalization import normalize_distress_topic
    from runtime.ops.deal_lifecycle import transition_deal
    from runtime.ops.outreach_execution import _infer_distress_type

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    dl.opportunity_id,
                    dl.merchant_domain,
                    dl.current_stage,
                    dl.processor,
                    dl.contact_trust_score,
                    COALESCE(mo.status, '') AS opp_status,
                    COALESCE(mo.distress_topic, '') AS opp_distress_topic,
                    COALESCE(oa.status, '') AS outreach_status,
                    COALESCE(oa.sent_message_id, '') AS sent_message_id,
                    COALESCE(oa.gmail_thread_id, '') AS gmail_thread_id,
                    LEFT(COALESCE(s.content, ''), 4000) AS signal_content
                FROM deal_lifecycle dl
                JOIN merchant_opportunities mo ON mo.id = dl.opportunity_id
                LEFT JOIN opportunity_outreach_actions oa ON oa.opportunity_id = dl.opportunity_id
                LEFT JOIN LATERAL (
                    SELECT s2.content
                    FROM signals s2
                    WHERE s2.merchant_id = dl.merchant_id
                       OR s2.id IN (
                            SELECT ms.signal_id
                            FROM merchant_signals ms
                            WHERE ms.merchant_id = dl.merchant_id
                       )
                    ORDER BY s2.detected_at DESC
                    LIMIT 1
                ) s ON TRUE
                WHERE dl.current_stage IN ('signal_detected', 'lead_qualified', 'opportunity_created')
                  AND dl.distress_type = 'unknown'
                  AND LOWER(COALESCE(mo.distress_topic, '')) IN ('consumer complaints', 'unclassified merchant distress', 'contact_discovered')
                  AND COALESCE(oa.sent_message_id, '') = ''
                  AND COALESCE(oa.gmail_thread_id, '') = ''
                  AND (
                        oa.status IS NULL
                        OR oa.status = ''
                        OR oa.status = 'no_outreach'
                  )
                ORDER BY dl.priority_score DESC, dl.updated_at DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()

    cleaned = 0
    errors = 0
    examples: list[dict[str, Any]] = []
    for row in rows:
        signal_content = str(row.get("signal_content") or "")
        inferred = normalize_distress_topic(_infer_distress_type(signal_content))
        if inferred != "unknown":
            continue
        try:
            transition_deal(
                int(row["opportunity_id"]),
                DealStage.outcome_ignored,
                actor="distress_hygiene",
                metadata={
                    "cleanup_reason": "generic_unknown_distress_without_live_outreach",
                    "legacy_opportunity_status": str(row.get("opp_status") or ""),
                    "legacy_distress_topic": str(row.get("opp_distress_topic") or ""),
                },
            )
            cleaned += 1
            if len(examples) < 10:
                examples.append(
                    {
                        "opportunity_id": int(row["opportunity_id"]),
                        "merchant_domain": row.get("merchant_domain") or "",
                        "legacy_distress_topic": row.get("opp_distress_topic") or "",
                    }
                )
        except Exception as exc:
            errors += 1
            logger.warning("generic unknown distress cleanup error for opp %s: %s", row.get("opportunity_id"), exc)

    result = {
        "status": "ok" if errors == 0 else "warning",
        "scanned": len(rows),
        "cleaned": cleaned,
        "errors": errors,
        "examples": examples,
        "cleaned_at": utc_now_iso(),
    }
    record_component_state(
        "deal_lifecycle_distress_cleanup",
        ttl=3600,
        last_distress_cleanup_at=result["cleaned_at"],
        last_distress_cleanup_scanned=len(rows),
        last_distress_cleanup_cleaned=cleaned,
        last_distress_cleanup_errors=errors,
    )
    save_event("deal_lifecycle_unknown_distress_cleanup", result)
    logger.info("deal_lifecycle unknown distress cleanup: %s", result)
    return result
