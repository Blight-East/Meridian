from __future__ import annotations

import re


_LIVE_URGENCY_RE = re.compile(
    r"\b(right now|currently|today|this week|ongoing|still|active|under review|on hold|frozen|paused|delayed|holding funds|cash flow|getting worse|escalating)\b",
    re.IGNORECASE,
)
_SEVERE_DISTRESS_RE = re.compile(
    r"\b(frozen|funds held|holding funds|reserve|payout delay|payouts delayed|under review|verification review|chargeback|dispute spike|processing disabled|account terminated)\b",
    re.IGNORECASE,
)
_REVENUE_HINT_RE = re.compile(
    r"\b(revenue|sales|orders|checkout|transactions|settlement|working capital|cash flow)\b",
    re.IGNORECASE,
)
_DECISION_ROLE_RE = re.compile(
    r"\b(founder|owner|ceo|finance|payments|risk|operations|ops|compliance|cfo)\b",
    re.IGNORECASE,
)
_TRUSTED_ROLE_RE = re.compile(
    r"\b(contact|info|support|team|sales)\b",
    re.IGNORECASE,
)

_DISTRESS_URGENCY_SCORES = {
    "account_frozen": 92,
    "account_terminated": 90,
    "payouts_delayed": 85,
    "reserve_hold": 83,
    "verification_review": 74,
    "chargeback_issue": 72,
    "processor_switch_intent": 58,
    "onboarding_rejected": 55,
    "unknown": 40,
}

_DISTRESS_CASH_IMPACT_LINES = {
    "account_frozen": "If processing is frozen, even a short interruption can trap materially more cash than the monthly cost.",
    "account_terminated": "If the account is being terminated, the downside can quickly exceed the monthly cost in trapped cash and recovery effort.",
    "payouts_delayed": "If payouts are already delayed, even one disrupted settlement cycle can put more cash at risk than the monthly cost.",
    "reserve_hold": "If reserve pressure is active, the trapped working capital can easily outweigh the monthly cost.",
    "verification_review": "If review pressure is active, the cost of waiting for clarity can exceed the monthly cost once payouts or processing slow down.",
    "chargeback_issue": "If chargeback pressure worsens, the combined operational and cash-flow damage can outweigh the monthly cost quickly.",
    "processor_switch_intent": "If processor instability forces a rushed switch, the cost of uncertainty can exceed the monthly cost.",
    "onboarding_rejected": "If onboarding friction blocks processing, the lost revenue window can outweigh the monthly cost.",
    "unknown": "If processor pressure is truly active, the avoided downside may still outweigh the monthly cost.",
}


def _label(score: int) -> str:
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _local_part(email: str | None) -> str:
    value = str(email or "").strip().lower()
    return value.split("@", 1)[0] if "@" in value else value


def assess_commercial_readiness(
    *,
    distress_type: str | None,
    processor: str | None,
    content: str | None = "",
    why_now: str | None = "",
    contact_email: str | None = "",
    contact_trust_score: int | float = 0,
    target_roles: list[str] | None = None,
    target_role_reason: str | None = "",
    icp_fit_score: int | float = 0,
    queue_quality_score: int | float = 0,
    revenue_detected: bool = False,
) -> dict:
    distress = str(distress_type or "unknown").strip().lower() or "unknown"
    processor_value = str(processor or "unknown").strip().lower() or "unknown"
    text = " ".join(part for part in [str(content or ""), str(why_now or ""), str(target_role_reason or "")] if part).strip()
    lowered = text.lower()
    roles = [str(role).strip().lower() for role in (target_roles or []) if str(role).strip()]
    contact_local_part = _local_part(contact_email)

    urgency = _DISTRESS_URGENCY_SCORES.get(distress, _DISTRESS_URGENCY_SCORES["unknown"])
    urgency_reasons: list[str] = []
    if _LIVE_URGENCY_RE.search(lowered):
        urgency += 12
        urgency_reasons.append("language suggests the issue is active now")
    if _SEVERE_DISTRESS_RE.search(lowered):
        urgency += 8
        urgency_reasons.append("the distress language is materially severe")
    if processor_value != "unknown":
        urgency += 4
        urgency_reasons.append(f"the signal is tied to {processor_value}")
    urgency = max(0, min(int(round(urgency)), 100))

    authority = min(max(int(round(float(contact_trust_score or 0) * 0.55)), 0), 55)
    authority_reasons: list[str] = []
    if contact_email:
        if _DECISION_ROLE_RE.search(contact_local_part):
            authority += 35
            authority_reasons.append("the contact path looks decision-maker aligned")
        elif _TRUSTED_ROLE_RE.search(contact_local_part):
            authority += 18
            authority_reasons.append("the contact path likely reaches a merchant-owned team inbox")
        else:
            authority += 10
            authority_reasons.append("there is at least a reachable merchant contact path")
    if any(role in {"founder", "owner", "finance", "payments", "risk", "operations", "ops", "compliance"} for role in roles):
        authority += 18
        authority_reasons.append("the likely buying authority sits with finance, payments, risk, or founder roles")
    authority = max(0, min(int(round(authority)), 100))

    cash_impact = 30
    cash_impact_reasons: list[str] = []
    if distress in {"account_frozen", "account_terminated", "payouts_delayed", "reserve_hold"}:
        cash_impact += 35
        cash_impact_reasons.append("this kind of distress can directly trap working capital")
    elif distress in {"verification_review", "chargeback_issue"}:
        cash_impact += 24
        cash_impact_reasons.append("this kind of distress can escalate into payout or processor pressure")
    if revenue_detected or _REVENUE_HINT_RE.search(lowered):
        cash_impact += 18
        cash_impact_reasons.append("the signal suggests real payment volume or cash-flow exposure")
    if float(icp_fit_score or 0) >= 70:
        cash_impact += 10
        cash_impact_reasons.append("the case fits the PayFlux problem tightly")
    if float(queue_quality_score or 0) >= 70:
        cash_impact += 7
        cash_impact_reasons.append("the underlying evidence quality is strong")
    cash_impact = max(0, min(int(round(cash_impact)), 100))

    commercial_readiness = int(
        round((urgency * 0.45) + (authority * 0.30) + (cash_impact * 0.25))
    )

    urgency_line = (
        f"Urgency is {_label(urgency)} ({urgency})"
        + (f" because {urgency_reasons[0]}." if urgency_reasons else ".")
    )
    authority_line = (
        f"Buying authority looks {_label(authority)} ({authority})"
        + (f" because {authority_reasons[0]}." if authority_reasons else ".")
    )
    cash_line = _DISTRESS_CASH_IMPACT_LINES.get(distress, _DISTRESS_CASH_IMPACT_LINES["unknown"])
    if cash_impact_reasons:
        cash_line = f"{cash_line} Why this matters: {cash_impact_reasons[0]}."

    return {
        "urgency_score": urgency,
        "urgency_label": _label(urgency),
        "urgency_summary": urgency_line,
        "buying_authority_score": authority,
        "buying_authority_label": _label(authority),
        "buying_authority_summary": authority_line,
        "cash_impact_score": cash_impact,
        "cash_impact_label": _label(cash_impact),
        "cash_impact_hypothesis": cash_line,
        "commercial_readiness_score": commercial_readiness,
        "commercial_readiness_label": _label(commercial_readiness),
    }
