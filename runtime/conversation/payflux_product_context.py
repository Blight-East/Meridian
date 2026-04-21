from __future__ import annotations

from typing import Any


def get_payflux_product_context() -> dict[str, Any]:
    return {
        "source_documents": [
            "/Users/ct/payment-node/docs/SYSTEM_TAXONOMY.md",
            "/Users/ct/payment-node/docs/SYSTEM_TAXONOMY.json",
            "/Users/ct/payflux-prod/payflux-site/src/pages/Home.jsx",
            "/Users/ct/payflux-prod/payflux-site/src/pages/Pricing.jsx",
        ],
        "product_name": "PayFlux",
        "product_type": "payment risk observability platform",
        "core_definition": (
            "PayFlux helps merchants detect processor pressure early, understand what changed, "
            "and decide what to do next before payout issues become a cash-flow problem."
        ),
        "plain_boundary": (
            "PayFlux is not the payment processor and does not issue merchant accounts. "
            "It is the visibility and decision-support layer around processor pressure."
        ),
        "system_definition": (
            "PayFlux is a payment risk observability platform that ingests processor-adjacent payment events, "
            "computes deterministic risk signals, emits warnings for elevated risk bands, and surfaces evidence, "
            "exports, and operator-readable records."
        ),
        "customer_problem": (
            "Merchants often do not see payout slowdowns, held funds, reserve pressure, or account review risk "
            "until cash flow is already getting squeezed."
        ),
        "core_promise": [
            "See processor risk before it turns into a cash-flow problem.",
            "Start with a free snapshot, then upgrade to live monitoring when the issue is active.",
            "Surface what changed, why it matters, and what to do first.",
        ],
        "who_it_helps": [
            "Merchants facing chargeback pressure",
            "Merchants seeing payout delays or held funds",
            "Operators worried processor review or account pressure is rising",
            "Teams that need exportable evidence and operator-ready records",
        ],
        "risk_categories": [
            "chargeback pressure",
            "payout delays",
            "held funds",
            "reserve pressure",
            "account pressure or review risk",
            "processor instability",
        ],
        "how_it_works": [
            "Run a free scan to surface visible warning signs.",
            "Connect Stripe read-only for live monitoring when available.",
            "Track changes over time and surface earlier warnings.",
            "Show forecast visibility, evidence exports, and operator guidance.",
        ],
        "plans": {
            "free": {
                "name": "Free",
                "price": "$0",
                "positioning": "one-time snapshot",
                "promise": "A one-time processor risk snapshot with visible warning signs and instability signals.",
            },
            "pro": {
                "name": "Pro",
                "price": "$499/month",
                "positioning": "live monitoring",
                "promise": (
                    "Live processor monitoring, earlier warnings for payout delays, held funds, and rising account pressure, "
                    "plus forecast visibility, exportable evidence, and guidance on what to fix first."
                ),
            },
            "enterprise": {
                "name": "Enterprise",
                "price": "custom",
                "positioning": "custom rollout",
                "promise": "Custom rollout for larger payment operations with multi-merchant reporting and more complex workflows.",
            },
        },
        "commercial_guardrails": [
            "Do not describe PayFlux as the processor, merchant account, gateway, or acquiring bank.",
            "Do not claim PayFlux can override processor or network controls.",
            "Do not imply PayFlux changes checkout flows or processor settings.",
            "Do not present internal architecture terms like 'evidence envelope' or 'tier gating' as frontline sales language unless asked.",
            "Lead with merchant outcomes: earlier warnings, clarity, exports, and guidance.",
        ],
        "language_preferences": [
            "Prefer processor pressure, payout delays, held funds, reserve pressure, account pressure, and cash-flow problem.",
            "Prefer free snapshot for entry and Pro for live monitoring.",
            "Avoid selling PayFlux as generic AI, generic automation, or broad observability jargon.",
            "If a merchant needs a new processor, position PayFlux as the tool that helps them understand and document the risk, not as the replacement processor itself.",
        ],
    }


def render_payflux_product_context() -> str:
    ctx = get_payflux_product_context()
    lines = ["PAYFLUX PRODUCT CONTEXT"]
    lines.append(f"Product: {ctx['product_name']} ({ctx['product_type']})")
    lines.append(f"Core definition: {ctx['core_definition']}")
    lines.append(f"Product boundary: {ctx['plain_boundary']}")
    lines.append(f"System definition: {ctx['system_definition']}")
    lines.append(f"Customer problem: {ctx['customer_problem']}")

    promises = ctx.get("core_promise") or []
    if promises:
        lines.append("Core promise:")
        lines.extend(f"- {item}" for item in promises[:4])

    who_it_helps = ctx.get("who_it_helps") or []
    if who_it_helps:
        lines.append("Who it helps:")
        lines.extend(f"- {item}" for item in who_it_helps[:5])

    risk_categories = ctx.get("risk_categories") or []
    if risk_categories:
        lines.append("Risk categories:")
        lines.extend(f"- {item}" for item in risk_categories[:6])

    how_it_works = ctx.get("how_it_works") or []
    if how_it_works:
        lines.append("How it works:")
        lines.extend(f"- {item}" for item in how_it_works[:5])

    plans = ctx.get("plans") or {}
    if plans:
        lines.append("Plans:")
        for key in ("free", "pro", "enterprise"):
            plan = plans.get(key) or {}
            if not plan:
                continue
            lines.append(f"- {plan.get('name')}: {plan.get('price')} — {plan.get('promise')}")

    guardrails = ctx.get("commercial_guardrails") or []
    if guardrails:
        lines.append("Commercial guardrails:")
        lines.extend(f"- {item}" for item in guardrails[:6])

    language = ctx.get("language_preferences") or []
    if language:
        lines.append("Language preferences:")
        lines.extend(f"- {item}" for item in language[:5])

    return "\n".join(lines).strip()


def render_payflux_product_brief() -> str:
    ctx = get_payflux_product_context()
    plans = ctx.get("plans") or {}
    free_plan = plans.get("free") or {}
    pro_plan = plans.get("pro") or {}
    return (
        f"{ctx.get('product_name')} is a {ctx.get('product_type')}.\n"
        f"{ctx.get('core_definition')}\n\n"
        f"{ctx.get('plain_boundary')}\n\n"
        f"Free: {free_plan.get('promise')}\n"
        f"Pro: {pro_plan.get('promise')}"
    ).strip()
