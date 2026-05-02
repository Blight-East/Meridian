"""
Curated outreach proposer — controlled validation mode.

Runs on a schedule, selects up to N merchant_opportunities per calendar day
that pass a set of targeting filters, and calls the existing
`draft_outreach_for_opportunity` generator to produce an approval-gated
outreach row. All rows are tagged `notes LIKE 'curated_proposer%'` so the
validation dashboard can measure outcome by this cohort.

Design rules:
- Zero net-new tables. All state lives in `opportunity_outreach_actions.notes`.
- Daily hard cap enforced here (not in SQL). Default 5.
- Negative-feedback loop: any domain that shows up in a prior row's notes with
  `rejected_reason=` is blocklisted on future runs.
- Post-generation sanity check: if draft body contains "Unclassified" the row
  is immediately archived so it can never be approved.
- Gmail send path is unchanged — runtime/channels/policy.py gates on
  GMAIL_CHANNEL_MODE=approval_required, which the .env enforces.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from runtime.ops.outreach_execution import draft_outreach_for_opportunity

logger = logging.getLogger("scheduler.curated_proposer")

_ENGINE = create_engine(
    "postgresql://postgres@127.0.0.1/agent_flux",
    pool_pre_ping=True,
    pool_recycle=300,
)

# Hardcoded rejections the extractor keeps producing. Anything matching these
# cannot be a real merchant in our ICP.
DOMAIN_BLOCKLIST: frozenset[str] = frozenset({
    # Generic English-word strings the entity extractor pulled from article
    # bodies / legal footers. Grows as we catch more.
    "policy.com", "disclaimer.com", "chargeback.com", "cryptocurrencies.com",
    "something.com", "whats.com", "example.com", "namely.com", "lavabit.com",
    "wolfire.com", "replace.com", "every.to", "domain.com",
    # Major brands never in our ICP.
    "apple.com", "adobe.com", "photoshop.com", "stripe.com", "google.com",
    "amazon.com", "shopify.com", "paypal.com", "visa.com", "mastercard.com",
    "microsoft.com", "facebook.com", "meta.com", "netflix.com",
    # Banks / processors / infra.
    "scotiabank.com", "chase.com", "bankofamerica.com", "omise.co",
    "quite.com", "brooksbrothers.com",
})

# Placeholder email fragments that should never reach the approval queue.
PLACEHOLDER_EMAIL_FRAGMENTS: tuple[str, ...] = (
    "example@", "@example.com", "@domain.com", "you@", "name@", "test@",
    "noreply@", "no-reply@", "@yourdomain", "@yoursite",
)

PROPOSER_TAG = "curated_proposer"


def _today_utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _count_today(conn) -> int:
    """
    Count proposer-tagged rows for today. Uses the date string embedded in
    notes ('curated_proposer YYYY-MM-DD') rather than created_at, because
    draft_outreach_for_opportunity UPSERTS onto existing rows — `created_at`
    can be months old.
    """
    tag = f"%{PROPOSER_TAG} {_today_utc_date()}%"
    row = conn.execute(
        text("SELECT count(*) FROM opportunity_outreach_actions WHERE notes LIKE :tag"),
        {"tag": tag},
    ).scalar()
    return int(row or 0)


def _load_rejected_domains(conn) -> set[str]:
    """Any domain whose prior row notes contains `rejected_reason=` is skipped."""
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT lower(merchant_domain) AS d
              FROM opportunity_outreach_actions
             WHERE notes LIKE '%rejected_reason=%'
               AND merchant_domain IS NOT NULL
            """
        )
    ).fetchall()
    return {r[0] for r in rows if r[0]}


def _load_recently_proposed_domains(conn, days: int = 14) -> set[str]:
    """Don't re-propose the same domain within 14 days (any channel/status)."""
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT lower(merchant_domain) AS d
              FROM opportunity_outreach_actions
             WHERE merchant_domain IS NOT NULL
               AND created_at > now() - (:days || ' days')::interval
            """
        ),
        {"days": days},
    ).fetchall()
    return {r[0] for r in rows if r[0]}


def _fetch_candidates(conn, limit: int = 50) -> list[dict]:
    """
    Candidate set: pending_review opportunities ordered by opportunity_score.
    Inline SQL filter drops the cheapest rejections (null processor,
    Unclassified distress). Python-level filter handles the rest.
    """
    rows = conn.execute(
        text(
            """
            SELECT mo.id             AS opportunity_id,
                   mo.merchant_id    AS merchant_id,
                   lower(mo.merchant_domain) AS merchant_domain,
                   mo.processor      AS processor,
                   mo.distress_topic AS distress_topic,
                   m.opportunity_score  AS opportunity_score,
                   m.urgency_score      AS urgency_score,
                   m.confidence_score   AS confidence_score,
                   mo.status         AS status
              FROM merchant_opportunities mo
              LEFT JOIN merchants m ON m.id = mo.merchant_id
             WHERE mo.status IS DISTINCT FROM 'archived'
               AND mo.status IS DISTINCT FROM 'converted'
               AND mo.merchant_domain IS NOT NULL
               AND mo.merchant_domain <> ''
               AND mo.processor IS NOT NULL
               AND lower(coalesce(mo.processor, '')) NOT IN ('', 'unknown', 'unclassified')
               AND coalesce(mo.distress_topic, '') NOT ILIKE 'unclassified%'
               -- Honor the classifier's own verdict: only real merchants.
               -- entity_class can be: merchant, platform, processor,
               -- non_merchant, unknown. We only want merchant.
               AND coalesce(m.entity_class, '') = 'merchant'
               -- Exclude opportunities that ALREADY have an outreach row
               -- (draft_outreach_for_opportunity UPSERTs; we never want to
               -- clobber prior sent/archived/approved rows).
               AND NOT EXISTS (
                 SELECT 1 FROM opportunity_outreach_actions ooa
                  WHERE ooa.opportunity_id = mo.id
               )
             ORDER BY m.opportunity_score DESC NULLS LAST, m.urgency_score DESC NULLS LAST, mo.id DESC
             LIMIT :limit
            """
        ),
        {"limit": limit},
    ).mappings().fetchall()
    return [dict(r) for r in rows]


def _passes_filters(
    candidate: dict,
    *,
    blocklist: frozenset[str],
    rejected: set[str],
    recent: set[str],
) -> tuple[bool, str]:
    domain = (candidate.get("merchant_domain") or "").lower()
    if not domain:
        return False, "no_domain"
    if domain in blocklist:
        return False, "domain_blocklisted"
    if domain in rejected:
        return False, "previously_rejected"
    if domain in recent:
        return False, "recently_proposed"
    topic = (candidate.get("distress_topic") or "").strip()
    if not topic or topic.lower().startswith("unclassified"):
        return False, "unclassified_signal"
    processor = (candidate.get("processor") or "").strip().lower()
    if processor in {"", "unknown", "unclassified", "none"}:
        return False, "no_processor"
    return True, "ok"


def _build_notes(candidate: dict, draft_result: dict) -> str:
    blocker = (draft_result.get("draft_blocker") or "").strip()
    review_only = bool(draft_result.get("review_only_draft"))
    confidence = "review_only" if review_only else "auto"
    lines = [
        f"{PROPOSER_TAG} {_today_utc_date()}",
        f"  signal: {candidate.get('distress_topic') or 'unknown'}",
        f"  processor: {candidate.get('processor') or 'unknown'}",
        f"  opportunity_score: {candidate.get('opportunity_score')}",
        f"  urgency_score: {candidate.get('urgency_score')}",
        f"  confidence: {confidence}",
        f"  source: merchant_opportunities.id={candidate.get('opportunity_id')}",
    ]
    if blocker:
        lines.append(f"  doctrine_note: {blocker}")
    return "\n".join(lines)


def _append_notes(conn, action_id: int, extra: str) -> None:
    conn.execute(
        text(
            """
            UPDATE opportunity_outreach_actions
               SET notes = coalesce(notes, '') ||
                           CASE WHEN notes IS NULL OR notes = '' THEN '' ELSE E'\n' END ||
                           :extra
             WHERE id = :id
            """
        ),
        {"id": action_id, "extra": extra},
    )


def _archive_row(conn, action_id: int, reason: str) -> None:
    conn.execute(
        text(
            """
            UPDATE opportunity_outreach_actions
               SET status = 'archived',
                   approval_state = 'archived',
                   notes = coalesce(notes, '') || E'\n' ||
                           :msg
             WHERE id = :id
            """
        ),
        {"id": action_id, "msg": f"auto_archived {_today_utc_date()}: {reason}"},
    )


def _contains_unclassified(outreach_result: dict) -> bool:
    body = (outreach_result.get("body") or "") + "\n" + (outreach_result.get("subject") or "")
    return "Unclassified" in body


def _has_placeholder_contact(outreach_result: dict) -> bool:
    email = (outreach_result.get("contact_email") or "").lower()
    if not email:
        return False
    return any(frag in email for frag in PLACEHOLDER_EMAIL_FRAGMENTS)


def run_curated_outreach_proposer(max_per_day: int = 5) -> dict:
    """
    Propose up to `max_per_day` curated outreach candidates. Idempotent per
    calendar day — re-running after the cap is hit is a no-op.

    Returns a dict summary suitable for scheduler telemetry.
    """
    summary = {
        "attempted": 0,
        "proposed": 0,
        "skipped": 0,
        "archived_unclassified": 0,
        "errors": 0,
        "remaining_capacity": 0,
        "reasons": {},
    }

    with _ENGINE.begin() as conn:
        already = _count_today(conn)
        capacity = max(0, max_per_day - already)
        summary["remaining_capacity"] = capacity
        if capacity <= 0:
            logger.info("curated_proposer daily cap reached (%s/%s)", already, max_per_day)
            return summary

        blocklist = DOMAIN_BLOCKLIST
        rejected = _load_rejected_domains(conn)
        recent = _load_recently_proposed_domains(conn, days=14)
        candidates = _fetch_candidates(conn, limit=50)

    # Draft generation is done outside the outer transaction because
    # draft_outreach_for_opportunity manages its own DB connection + commit.
    for candidate in candidates:
        if summary["proposed"] >= capacity:
            break
        summary["attempted"] += 1
        ok, reason = _passes_filters(
            candidate,
            blocklist=blocklist,
            rejected=rejected,
            recent=recent,
        )
        if not ok:
            summary["skipped"] += 1
            summary["reasons"][reason] = summary["reasons"].get(reason, 0) + 1
            continue

        opportunity_id = int(candidate["opportunity_id"])
        try:
            # allow_best_effort=True surfaces drafts the upstream doctrine
            # would otherwise refuse ("signal too weak", "contact missing",
            # "research-only"). In validation mode the operator is the gate,
            # not the doctrine — these drafts ship to the approval queue with
            # metadata.draft_blocker describing WHY doctrine was skeptical.
            result = draft_outreach_for_opportunity(
                opportunity_id=opportunity_id,
                outreach_type="initial_outreach",
                allow_best_effort=True,
            )
        except Exception:  # noqa: BLE001 — scheduler task, must never raise
            logger.exception("draft_outreach_for_opportunity failed for %s", opportunity_id)
            summary["errors"] += 1
            summary["reasons"]["draft_exception"] = summary["reasons"].get("draft_exception", 0) + 1
            continue

        # draft_outreach_for_opportunity returns {"error": "..."} for gated/unsuitable
        # candidates (e.g. "Opportunity not found", "This lead is not send-ready yet").
        # Count those as skips, not errors — they're the filter working.
        if isinstance(result, dict) and result.get("error"):
            summary["skipped"] += 1
            key = f"draft_refused:{result['error'][:40]}"
            summary["reasons"][key] = summary["reasons"].get(key, 0) + 1
            continue

        # Resolve action_id from opportunity_id (draft_outreach_for_opportunity
        # does not return it directly; the row it upserted is the most recent
        # opportunity_outreach_actions row for this opportunity).
        with _ENGINE.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id FROM opportunity_outreach_actions
                     WHERE opportunity_id = :oid
                     ORDER BY created_at DESC
                     LIMIT 1
                    """
                ),
                {"oid": opportunity_id},
            ).first()
        action_id = int(row[0]) if row else None
        if not action_id:
            summary["errors"] += 1
            summary["reasons"]["no_action_id"] = summary["reasons"].get("no_action_id", 0) + 1
            continue

        # Post-generation safety: kill the row if it has any of these red flags.
        # (UPSERT already wrote it, so we archive rather than skip.)
        reject_reason = None
        if _contains_unclassified(result):
            reject_reason = "draft contains Unclassified token"
        elif _has_placeholder_contact(result):
            reject_reason = f"placeholder contact_email: {result.get('contact_email')}"
        if reject_reason:
            with _ENGINE.begin() as conn:
                _archive_row(conn, action_id, reject_reason)
            summary["archived_unclassified"] += 1
            summary["reasons"][f"auto_archived:{reject_reason[:40]}"] = (
                summary["reasons"].get(f"auto_archived:{reject_reason[:40]}", 0) + 1
            )
            continue

        with _ENGINE.begin() as conn:
            _append_notes(conn, action_id, _build_notes(candidate, result))
            # Defensive: every proposer-created row MUST go through operator
            # approval. Force the state regardless of what GMAIL_CHANNEL_MODE
            # is — this way the proposer contract does not depend on env vars.
            # If sent_at is already set, don't touch (that would be a race).
            conn.execute(
                text(
                    """
                    UPDATE opportunity_outreach_actions
                       SET approval_state = 'approval_required',
                           status         = 'awaiting_approval'
                     WHERE id = :id
                       AND sent_at IS NULL
                    """
                ),
                {"id": action_id},
            )
            # Track domain as recently proposed so the next iteration in this
            # same run doesn't re-propose it.
        recent.add(candidate["merchant_domain"])
        summary["proposed"] += 1
        logger.info(
            "curated_proposer proposed action_id=%s domain=%s score=%s",
            action_id,
            candidate["merchant_domain"],
            candidate.get("opportunity_score"),
        )

    logger.info("curated_proposer summary: %s", summary)
    return summary


if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run_curated_outreach_proposer(), indent=2, default=str))
