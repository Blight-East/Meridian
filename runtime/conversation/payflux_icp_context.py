from __future__ import annotations

from typing import Any


def get_payflux_icp_context() -> dict[str, Any]:
    return {
        "source_documents": [
            "/Users/ct/payment-node/docs/SYSTEM_TAXONOMY.md",
            "/Users/ct/payment-node/docs/SYSTEM_TAXONOMY.json",
            "/Users/ct/payflux-prod/payflux-site/src/pages/Home.jsx",
            "/Users/ct/payflux-prod/payflux-site/src/pages/Pricing.jsx",
        ],
        "icp_summary": (
            "PayFlux is best sold to merchants or payment operators who are already showing signs of processor pressure "
            "and need earlier clarity, evidence, and guidance before the situation becomes a cash-flow problem."
        ),
        "primary_segments": [
            "Ecommerce merchants with visible chargeback or reserve pressure",
            "Subscription or SaaS merchants seeing payout instability, dispute growth, or account pressure",
            "Operators responsible for payments, finance, risk, or merchant operations at merchants under live processor stress",
        ],
        "buyer_roles": [
            "Founder or owner",
            "Finance lead or operator",
            "Payments or risk owner",
            "Operations lead",
            "Support or compliance lead when they directly own processor response",
        ],
        "problem_urgency_signals": [
            "Chargeback pressure",
            "Payout delays",
            "Held funds",
            "Reserve pressure",
            "Underwriting or review pressure",
            "Processor instability that could turn into account action",
        ],
        "high_conviction_prospect": {
            "definition": (
                "A merchant or operator with credible signs of live processor pressure, a reachable operator contact, "
                "and a situation serious enough that earlier warnings or guidance could matter now."
            ),
            "required_signals": [
                "At least one credible distress signal tied to processor pressure",
                "A real merchant domain or identity, not a vague forum persona",
                "A reachable human contact path",
            ],
            "strong_positive_signals": [
                "Multiple signals cluster around chargebacks, reserves, payout delays, or review pressure",
                "Language indicates the issue is active now, not historical or hypothetical",
                "The merchant appears to process meaningful payment volume or revenue",
                "The operator likely feels cash-flow or operational pain if the issue worsens",
                "The case fits the current PayFlux promise: clarity, monitoring, exports, and next steps",
            ],
        },
        "medium_conviction_prospect": {
            "definition": "A merchant with plausible distress but weaker evidence, weaker contact quality, or unclear urgency.",
            "use": "Worth investigation or softer outreach, but not the first case Meridian should prioritize.",
        },
        "disqualifiers": [
            "No real merchant identity or domain can be established",
            "The evidence is purely educational, hypothetical, or historical with no active pressure",
            "No operator contact path exists",
            "The case is really generic fraud prevention, generic analytics, or generic growth tooling rather than processor distress",
            "The merchant appears too early-stage, too low-intensity, or too vague for PayFlux Pro to matter",
            "The outreach would require bluffing beyond the evidence actually available",
        ],
        "commercial_fit_rules": [
            "Lead with active processor pressure, not generic observability or AI automation",
            "Treat Free as the entry path when the operator needs a first read",
            "Treat Pro as the path when the issue is live enough to need ongoing monitoring and earlier warnings",
            "Prefer cases where PayFlux can plausibly shorten time-to-clarity or improve the operator response",
        ],
        "language_rules": [
            "Prefer merchant, operator, processor pressure, payout delays, held funds, reserve pressure, and account pressure",
            "Avoid broad language like business growth, automation consulting, or AI transformation",
            "Do not overclaim that PayFlux can stop or override processor actions",
        ],
    }


def render_payflux_icp_context() -> str:
    ctx = get_payflux_icp_context()
    lines = ["PAYFLUX ICP CONTEXT"]
    lines.append(f"ICP summary: {ctx['icp_summary']}")

    for key, label in (
        ("primary_segments", "Primary segments"),
        ("buyer_roles", "Buyer roles"),
        ("problem_urgency_signals", "Urgency signals"),
    ):
        items = ctx.get(key) or []
        if items:
            lines.append(f"{label}:")
            lines.extend(f"- {item}" for item in items[:6])

    high = ctx.get("high_conviction_prospect") or {}
    if high:
        lines.append("High-conviction prospect:")
        if high.get("definition"):
            lines.append(f"- {high['definition']}")
        for item in high.get("required_signals") or []:
            lines.append(f"- Required: {item}")
        for item in high.get("strong_positive_signals") or []:
            lines.append(f"- Strong signal: {item}")

    medium = ctx.get("medium_conviction_prospect") or {}
    if medium:
        lines.append("Medium-conviction prospect:")
        if medium.get("definition"):
            lines.append(f"- {medium['definition']}")
        if medium.get("use"):
            lines.append(f"- {medium['use']}")

    disqualifiers = ctx.get("disqualifiers") or []
    if disqualifiers:
        lines.append("Disqualifiers:")
        lines.extend(f"- {item}" for item in disqualifiers[:8])

    fit_rules = ctx.get("commercial_fit_rules") or []
    if fit_rules:
        lines.append("Commercial fit rules:")
        lines.extend(f"- {item}" for item in fit_rules[:6])

    language_rules = ctx.get("language_rules") or []
    if language_rules:
        lines.append("Language rules:")
        lines.extend(f"- {item}" for item in language_rules[:5])

    return "\n".join(lines).strip()


def render_payflux_icp_brief() -> str:
    ctx = get_payflux_icp_context()
    high = ctx.get("high_conviction_prospect") or {}
    disqualifiers = ctx.get("disqualifiers") or []
    buyers = ", ".join((ctx.get("buyer_roles") or [])[:4])
    return (
        f"Who we sell to: {ctx.get('icp_summary')}\n\n"
        f"Main buyers: {buyers}.\n\n"
        f"What counts as high conviction: {high.get('definition')}\n"
        f"Required: {'; '.join((high.get('required_signals') or [])[:3])}.\n\n"
        f"What disqualifies a lead: {'; '.join(disqualifiers[:4])}."
    ).strip()
