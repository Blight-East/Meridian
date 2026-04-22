"""
Conversion Layer Upgrade — isolated, flag-gated helpers.

Every function in this module is opt-in via an environment flag.  When the
flag is OFF (the default), call sites short-circuit to the existing
production behaviour.  Nothing in this file alters existing DB schema beyond
additive `CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS` calls.

Flags (all default False / legacy behaviour):
  MERIDIAN_ENABLE_DYNAMIC_MSG          — inject sales_strategy into template body
  MERIDIAN_RELAX_CONTACT_CONF          — lower contact-confidence gate + role fallback
  MERIDIAN_ENABLE_URGENCY_SORT         — sort opportunity queue by urgency_score DESC
  MERIDIAN_ENABLE_FUNNEL_TRACKING      — append tracking param, record funnel events
  MERIDIAN_UPGRADE_DRY_RUN             — skip outbound Gmail send, log would-have-sent

Tunables:
  MIN_CONTACT_CONFIDENCE_RELAXED       — default 0.65 (used when RELAX flag ON)
  MERIDIAN_TRACKING_PARAM              — default "m_src=meridian"
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Iterable

_log = logging.getLogger("meridian.conversion_upgrade")


class OutreachDryRunSkipped(Exception):
    """
    Raised from inside the Gmail send helpers when
    `MERIDIAN_UPGRADE_DRY_RUN=true` to force the send path to abort BEFORE
    the caller performs any "mark as sent" DB writes.  Callers that want
    the dry-run to succeed-without-sending must catch this exception
    explicitly — otherwise they will bubble the dry-run as an error, which
    is the safe failure mode.

    Attributes mirror the would-have-been request so callers can log /
    journal them.  Never raised when the flag is OFF.
    """

    def __init__(
        self,
        *,
        to_email: str = "",
        subject: str = "",
        thread_id: str = "",
        body_preview: str = "",
    ) -> None:
        super().__init__("MERIDIAN_UPGRADE_DRY_RUN is enabled; send was skipped")
        self.to_email = to_email
        self.subject = subject
        self.thread_id = thread_id
        self.body_preview = body_preview


# ---------------------------------------------------------------------------
# Flag helpers
# ---------------------------------------------------------------------------

_TRUE = {"1", "true", "yes", "on", "y", "t"}


def _flag(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in _TRUE


def is_dynamic_msg_enabled() -> bool:
    return _flag("MERIDIAN_ENABLE_DYNAMIC_MSG")


def is_relax_contact_conf_enabled() -> bool:
    return _flag("MERIDIAN_RELAX_CONTACT_CONF")


def is_urgency_sort_enabled() -> bool:
    return _flag("MERIDIAN_ENABLE_URGENCY_SORT")


def is_funnel_tracking_enabled() -> bool:
    return _flag("MERIDIAN_ENABLE_FUNNEL_TRACKING")


def is_dry_run_enabled() -> bool:
    return _flag("MERIDIAN_UPGRADE_DRY_RUN")


def log_upgrade(msg: str, **ctx: Any) -> None:
    """All upgrade-path log lines carry the [MERIDIAN_UPGRADE] prefix."""
    try:
        suffix = " " + json.dumps(ctx, default=str) if ctx else ""
    except Exception:
        suffix = ""
    _log.info("[MERIDIAN_UPGRADE] %s%s", msg, suffix)


# ---------------------------------------------------------------------------
# Contact gating (STEP 1)
# ---------------------------------------------------------------------------

MIN_CONTACT_CONFIDENCE_RELAXED = float(
    os.getenv("MIN_CONTACT_CONFIDENCE_RELAXED", "0.65")
)

ROLE_EMAIL_PREFIXES: tuple[str, ...] = (
    "support",
    "hello",
    "founders",
    "founder",
    "info",
    "contact",
    "sales",
    "team",
    "ops",
    "operations",
)

MAX_CONTACTS_PER_MERCHANT = int(os.getenv("MERIDIAN_MAX_CONTACTS_PER_MERCHANT", "3"))


def _local_part(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[0].strip().lower()


def _domain_part(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[1].strip().lower()


def role_email_allowed(row: dict, *, merchant_domain: str | None = None) -> bool:
    """
    Role-based email pass rule (only active when RELAX flag is ON).

    Accepts a contact row if:
      - local-part is a known role prefix (support@, hello@, founders@, ...)
      - same_domain is True (email domain == merchant domain)
      - email_verified OR an MX record is known to be valid

    `row` is expected to be a dict-like mapping with the columns that the
    contact_discovery schema adds (email_verified, same_domain, email_domain).
    """
    email = str(row.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return False
    local = _local_part(email)
    if local not in ROLE_EMAIL_PREFIXES:
        return False

    # same_domain: prefer the column, fall back to comparing against merchant_domain
    same_domain = bool(row.get("same_domain"))
    if not same_domain and merchant_domain:
        same_domain = _domain_part(email) == str(merchant_domain).strip().lower()
    if not same_domain:
        return False

    if bool(row.get("email_verified")):
        return True

    # Cheap MX fallback if the column is missing/false.  Kept import-local
    # so the module stays cheap to import.
    try:
        from runtime.intelligence.domain_utils import get_mx_servers  # type: ignore

        mx = get_mx_servers(_domain_part(email))
        return bool(mx)
    except Exception:
        return False


def filter_contacts_for_outreach(
    rows: Iterable[dict],
    *,
    legacy_min_conf: float,
    merchant_domain: str | None = None,
    max_contacts: int = MAX_CONTACTS_PER_MERCHANT,
) -> list[dict]:
    """
    Return up to `max_contacts` contacts ranked by confidence DESC.

    When MERIDIAN_RELAX_CONTACT_CONF is OFF: behaves exactly like the legacy
    single-contact filter (confidence >= legacy_min_conf), only returning up
    to `max_contacts` of them from the caller-supplied rows.

    When MERIDIAN_RELAX_CONTACT_CONF is ON: also admits contacts with
    confidence >= MIN_CONTACT_CONFIDENCE_RELAXED, and admits role-based
    same-domain verified/MX-validated emails at any confidence.
    """
    items = [dict(r) for r in rows if r]
    relaxed = is_relax_contact_conf_enabled()
    accepted: list[dict] = []
    for r in items:
        try:
            conf = float(r.get("confidence") or 0.0)
        except Exception:
            conf = 0.0
        if conf >= legacy_min_conf:
            accepted.append(r)
            continue
        if not relaxed:
            continue
        if conf >= MIN_CONTACT_CONFIDENCE_RELAXED:
            accepted.append(r)
            continue
        if role_email_allowed(r, merchant_domain=merchant_domain):
            accepted.append(r)
    accepted.sort(key=lambda r: float(r.get("confidence") or 0.0), reverse=True)
    trimmed = accepted[: max(1, int(max_contacts))]
    if relaxed and trimmed:
        log_upgrade(
            "contact_gating_relaxed",
            merchant_domain=merchant_domain,
            returned=len(trimmed),
            legacy_gate=legacy_min_conf,
            relaxed_gate=MIN_CONTACT_CONFIDENCE_RELAXED,
        )
    return trimmed


# ---------------------------------------------------------------------------
# Messaging intelligence (STEP 2)
# ---------------------------------------------------------------------------


def _coerce_strategy(strategy: Any) -> dict:
    """Accept dict or JSON string; silently normalise missing fields."""
    if not strategy:
        return {}
    if isinstance(strategy, str):
        try:
            strategy = json.loads(strategy)
        except Exception:
            return {}
    if not isinstance(strategy, dict):
        return {}
    return strategy


def _first_nonempty(d: dict, *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def build_dynamic_message(
    context: dict,
    strategy: Any,
    base_template: dict,
) -> dict:
    """
    Augment a base template (subject/body/notes) with sales-strategy insight.

    Rules:
      - Preserves existing template structure.  The opening line and the CTA
        block stay in place.
      - Injects pain_point as the opening paragraph (after the greeting).
      - Injects pitch_angle as the second paragraph.
      - Injects recommended_message inline with the CTA section.
      - Accepts either the deal_sourcing schema (pain_point / pitch_angle /
        recommended_message) or the sales_reasoning schema (pain_point /
        angle / recommended_pitch).
      - If strategy missing / empty: returns base_template unchanged.
      - Never calls an external LLM.  Pure string composition.
    """
    if not is_dynamic_msg_enabled():
        return base_template

    strat = _coerce_strategy(strategy)
    if not strat:
        return base_template

    pain_point = _first_nonempty(strat, "pain_point")
    pitch_angle = _first_nonempty(strat, "pitch_angle", "angle")
    recommended = _first_nonempty(strat, "recommended_message", "recommended_pitch")

    if not any((pain_point, pitch_angle, recommended)):
        return base_template

    subject = str(base_template.get("subject") or "")
    body = str(base_template.get("body") or "")
    if not body:
        return base_template

    lines = body.split("\n")
    # The production templates always start with "Hi {contact_name},".
    # We inject after the greeting block, preserving signatures / CTAs.
    greeting_idx = 0
    for i, line in enumerate(lines[:3]):
        if line.strip().lower().startswith("hi "):
            greeting_idx = i
            break

    head = lines[: greeting_idx + 1]
    rest = lines[greeting_idx + 1 :]

    # Strip leading blank lines of `rest` so we control spacing.
    while rest and not rest[0].strip():
        rest.pop(0)

    injected: list[str] = [""]
    if pain_point:
        injected.append(pain_point)
        injected.append("")
    if pitch_angle:
        injected.append(pitch_angle)
        injected.append("")

    # Merge `recommended` into the CTA section if present.  The CTA block is
    # identified by the first line starting with "If helpful" / "If you" /
    # "If it is" / "Two focused questions".  Fall back to appending before the
    # signature ("Best,").
    merged_rest = list(rest)
    if recommended:
        inserted = False
        cta_markers = ("If helpful", "If you prefer", "If it is", "If this is")
        for i, line in enumerate(merged_rest):
            if any(line.strip().startswith(m) for m in cta_markers):
                merged_rest.insert(i, recommended)
                merged_rest.insert(i + 1, "")
                inserted = True
                break
        if not inserted:
            sig_idx = None
            for i, line in enumerate(merged_rest):
                if line.strip().startswith("Best,") or line.strip().startswith("— PayFlux"):
                    sig_idx = i
                    break
            if sig_idx is not None:
                merged_rest.insert(sig_idx, recommended)
                merged_rest.insert(sig_idx + 1, "")
            else:
                merged_rest.append("")
                merged_rest.append(recommended)

    new_body = "\n".join(head + injected + merged_rest)
    log_upgrade(
        "dynamic_message_injected",
        subject=subject[:80],
        pain_point=bool(pain_point),
        pitch_angle=bool(pitch_angle),
        recommended=bool(recommended),
        delta_chars=len(new_body) - len(body),
    )
    # Prompt-engineering playbook hook (warn_only by default — never blocks).
    # Flips to blocking behaviour only when MERIDIAN_PLAYBOOK_ENFORCEMENT=enforce.
    try:
        from runtime.ops.prompt_review import review_prompt

        verdict = review_prompt(new_body, label="dynamic_message")
        if not verdict.get("ok") and verdict.get("mode") == "enforce":
            # In enforce mode we prefer the original template to a rejected rewrite.
            log_upgrade(
                "dynamic_message_rejected_by_prompt_review",
                missing=",".join(verdict.get("missing", [])),
                score=verdict.get("score"),
            )
            return base_template
    except Exception:
        pass
    return {**base_template, "body": new_body}


# ---------------------------------------------------------------------------
# Urgency scoring (STEP 3)
# ---------------------------------------------------------------------------

PROCESSOR_URGENCY_WEIGHTS: dict[str, int] = {
    "stripe": 10,
    "paypal": 8,
    "square": 7,
}
DEFAULT_PROCESSOR_URGENCY_WEIGHT = 5


def processor_weight(processor: str | None) -> int:
    if not processor:
        return DEFAULT_PROCESSOR_URGENCY_WEIGHT
    return PROCESSOR_URGENCY_WEIGHTS.get(
        str(processor).strip().lower(),
        DEFAULT_PROCESSOR_URGENCY_WEIGHT,
    )


def compute_urgency_score(
    distress_score: float | int | None,
    recent_signal_count: int | None,
    processor: str | None,
) -> float:
    """
    urgency_score = (distress * 2) + (recent_signals * 3) + processor_weight
    Always safe to call; clamped to [0, 100].
    """
    try:
        distress = float(distress_score or 0.0)
    except Exception:
        distress = 0.0
    try:
        recent = int(recent_signal_count or 0)
    except Exception:
        recent = 0
    raw = (distress * 2.0) + (recent * 3.0) + float(processor_weight(processor))
    return max(0.0, min(100.0, raw))


# ---------------------------------------------------------------------------
# Funnel tracking (STEP 4)
# ---------------------------------------------------------------------------

TRACKING_PARAM = os.getenv("MERIDIAN_TRACKING_PARAM", "m_src=meridian").strip().lstrip("?&")

# Hosts where click-through attribution is useful.  Cheap allowlist so we
# never rewrite third-party hyperlinks.
TRACKABLE_HOSTS: tuple[str, ...] = tuple(
    h.strip().lower()
    for h in os.getenv(
        "MERIDIAN_TRACKABLE_HOSTS",
        "payflux.dev,www.payflux.dev,checkout.stripe.com",
    ).split(",")
    if h.strip()
)

_URL_RE = re.compile(r"https?://[^\s<>\)\]\}\"']+", re.IGNORECASE)


def _append_tracking_to_url(url: str) -> str:
    if not url:
        return url
    lower = url.lower()
    if not any(host in lower for host in TRACKABLE_HOSTS):
        return url
    if TRACKING_PARAM.split("=", 1)[0] + "=" in lower:
        return url
    # Preserve trailing punctuation that happens to follow URLs in prose.
    trailing = ""
    while url and url[-1] in ".,);":
        trailing = url[-1] + trailing
        url = url[:-1]
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{TRACKING_PARAM}{trailing}"


def augment_body_with_tracking(body: str) -> str:
    """Append ?m_src=meridian to every trackable URL in the body."""
    if not body or not is_funnel_tracking_enabled():
        return body
    new_body = _URL_RE.sub(lambda m: _append_tracking_to_url(m.group(0)), body)
    if new_body != body:
        log_upgrade("tracking_params_injected", delta_chars=len(new_body) - len(body))
    return new_body


def ensure_funnel_events_table() -> None:
    """Additive — creates `outreach_funnel_events` only if missing."""
    try:
        from memory.structured.db import engine  # type: ignore
        from sqlalchemy import text  # type: ignore
    except Exception as exc:  # pragma: no cover - import guard
        _log.debug("ensure_funnel_events_table skipped: %s", exc)
        return
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS outreach_funnel_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    channel TEXT NOT NULL DEFAULT 'gmail',
                    opportunity_id BIGINT,
                    merchant_id BIGINT,
                    thread_id TEXT,
                    message_id TEXT,
                    recipient TEXT,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS outreach_funnel_events_event_idx
                ON outreach_funnel_events (event_type, created_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS outreach_funnel_events_opportunity_idx
                ON outreach_funnel_events (opportunity_id)
                """
            )
        )
        conn.commit()


def record_funnel_event(
    event_type: str,
    *,
    channel: str = "gmail",
    opportunity_id: int | None = None,
    merchant_id: int | None = None,
    thread_id: str | None = None,
    message_id: str | None = None,
    recipient: str | None = None,
    metadata: dict | None = None,
) -> None:
    """
    Write a single funnel event.  No-op when MERIDIAN_ENABLE_FUNNEL_TRACKING
    is OFF.  Swallows all exceptions — instrumentation must never break the
    send path.
    """
    if not is_funnel_tracking_enabled():
        return
    try:
        ensure_funnel_events_table()
        from memory.structured.db import engine  # type: ignore
        from sqlalchemy import text  # type: ignore

        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO outreach_funnel_events
                        (event_type, channel, opportunity_id, merchant_id,
                         thread_id, message_id, recipient, metadata_json)
                    VALUES (:evt, :chan, :opp, :mid, :tid, :msg, :rcpt,
                            CAST(:meta AS JSONB))
                    """
                ),
                {
                    "evt": str(event_type).strip().lower(),
                    "chan": str(channel or "gmail").strip().lower(),
                    "opp": opportunity_id,
                    "mid": merchant_id,
                    "tid": thread_id,
                    "msg": message_id,
                    "rcpt": recipient,
                    "meta": json.dumps(metadata or {}),
                },
            )
            conn.commit()
        log_upgrade(
            "funnel_event_recorded",
            event_type=event_type,
            opportunity_id=opportunity_id,
            channel=channel,
        )
    except Exception as exc:  # pragma: no cover - best effort
        _log.debug("[MERIDIAN_UPGRADE] record_funnel_event failed: %s", exc)


def get_funnel_metrics_snapshot(window_hours: int = 24) -> dict:
    """Return open/click/bounce rates over the trailing window."""
    snapshot = {
        "window_hours": int(window_hours),
        "sent": 0,
        "opens": 0,
        "clicks": 0,
        "bounces": 0,
        "open_rate": 0.0,
        "click_rate": 0.0,
        "bounce_rate": 0.0,
        "tracking_enabled": is_funnel_tracking_enabled(),
    }
    try:
        ensure_funnel_events_table()
        from memory.structured.db import engine  # type: ignore
        from sqlalchemy import text  # type: ignore

        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT event_type, COUNT(*)::int AS cnt
                    FROM outreach_funnel_events
                    WHERE created_at >= NOW() - make_interval(hours => :hrs)
                    GROUP BY event_type
                    """
                ),
                {"hrs": int(window_hours)},
            ).fetchall()
    except Exception as exc:  # pragma: no cover - best effort
        _log.debug("[MERIDIAN_UPGRADE] get_funnel_metrics_snapshot failed: %s", exc)
        return snapshot
    counts = {str(evt).lower(): int(cnt) for evt, cnt in rows}
    snapshot["sent"] = counts.get("sent", 0)
    snapshot["opens"] = counts.get("open", 0)
    snapshot["clicks"] = counts.get("click", 0)
    snapshot["bounces"] = counts.get("bounce", 0)
    if snapshot["sent"] > 0:
        snapshot["open_rate"] = round(snapshot["opens"] / snapshot["sent"], 4)
        snapshot["click_rate"] = round(snapshot["clicks"] / snapshot["sent"], 4)
        snapshot["bounce_rate"] = round(snapshot["bounces"] / snapshot["sent"], 4)
    return snapshot


def detect_bounce_from_gmail_result(result: Any) -> bool:
    """
    Best-effort heuristic on the Gmail send API response.  Gmail returns 200
    on queue, so a real bounce surfaces later via triage; this only catches
    the obvious inline-error shapes.
    """
    if not result:
        return False
    if isinstance(result, dict):
        if result.get("labelIds") and "DELIVERY_ERROR" in set(result.get("labelIds") or []):
            return True
        if "error" in result and result["error"]:
            return True
    return False
