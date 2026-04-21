from __future__ import annotations

from typing import Any


def _join_sentences(items: list[str], limit: int) -> str:
    cleaned = []
    for item in items[:limit]:
        stripped = item.strip()
        while stripped.endswith((".", "?", "!")):
            stripped = stripped[:-1]
        cleaned.append(stripped)
    return "; ".join(cleaned)


def get_payflux_sales_context() -> dict[str, Any]:
    return {
        "source_documents": [
            "/Users/ct/payflux-prod/docs/FIRST_REVENUE_OUTBOUND_PLAYBOOK.md",
            "/Users/ct/payflux-prod/docs/SALES_CALL_OBJECTION_SCRIPT.md",
            "/Users/ct/payflux-prod/payflux-site/src/pages/Home.jsx",
            "/Users/ct/payflux-prod/payflux-site/src/pages/Pricing.jsx",
            "/Users/ct/payflux-prod/payflux-site/src/pages/ProofAsset.jsx",
        ],
        "sales_goal": "Convert a merchant under live processor pressure into a paid PayFlux workflow when the product clearly protects more downside than it costs.",
        "core_reframe": (
            "The merchant is not deciding whether to buy another dashboard. "
            "They are deciding whether a clearer read on what is changing, what it may mean, "
            "and what to check next is worth protecting more cash than the monthly cost."
        ),
        "main_objections": [
            "I am already under cash pressure, so I cannot add another fixed cost unless it clearly protects more downside than it costs.",
            "How do I know this is not just another dashboard?",
            "Will this work for my processor and my exact situation?",
            "What if I pay and still get held?",
            "Why should I believe this is urgent enough to pay for now?",
        ],
        "response_rules": [
            "Lead with avoided downside, not features.",
            "Lead with operator clarity: what changed, why it matters, and what to check next.",
            "State clearly that PayFlux is not the processor or merchant account.",
            "Do not promise PayFlux can override processor actions.",
            "Treat Free as the low-friction first step when urgency is uncertain.",
            "Treat Pro as the right path when the issue is active enough to need ongoing visibility.",
        ],
        "communication_principles": [
            "Sound like a sharp human operator: warm, clear, calm, and commercially credible.",
            "Explain PayFlux in normal language before using product language.",
            "Use short honest sentences and keep the average line easy to read.",
            "Earn urgency instead of forcing it.",
            "Keep the message focused on the merchant's situation, not on our internal sophistication.",
        ],
        "writing_guardrails": [
            "Do not sound like marketing automation or a consultant trying to impress.",
            "Do not use inflated words like transformative, powerful, robust, strategic, or pivotal.",
            "Do not pile on jargon, trailing -ing phrases, or rule-of-three filler.",
            "Do not hide uncertainty behind buzzwords or broad claims.",
            "Do not describe PayFlux as the processor, merchant account, or acquiring solution.",
            "Do not make PayFlux sound like it overrides processor decisions.",
        ],
        "plain_english_story": [
            "PayFlux helps teams see payment risk earlier.",
            "It gives a clearer read on what changed and what may be building.",
            "It helps teams keep a clear record they can share internally.",
            "It is not the processor itself.",
        ],
        "discovery_questions": [
            "Is the issue mainly payout delay, reserve/review pressure, or full processing disruption?",
            "Are you trying to stabilize the current processor, or evaluate an alternative quickly?",
            "How much does the uncertainty affect operations or cash flow if it stays unresolved?",
        ],
        "close_frames": [
            "Would a clearer read on what is changing and what to check next protect materially more value than the monthly cost while this is active?",
            "If the issue is active enough that ongoing visibility matters, the next move is Pro.",
        ],
    }


def render_payflux_sales_context() -> str:
    ctx = get_payflux_sales_context()
    lines = ["PAYFLUX SALES CONTEXT"]
    lines.append(f"Sales goal: {ctx['sales_goal']}")
    lines.append(f"Core reframe: {ctx['core_reframe']}")
    communication = ctx.get("communication_principles") or []
    if communication:
        lines.append("Communication principles:")
        lines.extend(f"- {item}" for item in communication[:6])
    guardrails = ctx.get("writing_guardrails") or []
    if guardrails:
        lines.append("Writing guardrails:")
        lines.extend(f"- {item}" for item in guardrails[:6])
    story = ctx.get("plain_english_story") or []
    if story:
        lines.append("Plain-English story:")
        lines.extend(f"- {item}" for item in story[:4])
    objections = ctx.get("main_objections") or []
    if objections:
        lines.append("Main objections:")
        lines.extend(f"- {item}" for item in objections[:6])
    response_rules = ctx.get("response_rules") or []
    if response_rules:
        lines.append("Response rules:")
        lines.extend(f"- {item}" for item in response_rules[:6])
    discovery = ctx.get("discovery_questions") or []
    if discovery:
        lines.append("Discovery questions:")
        lines.extend(f"- {item}" for item in discovery[:5])
    close_frames = ctx.get("close_frames") or []
    if close_frames:
        lines.append("Close frames:")
        lines.extend(f"- {item}" for item in close_frames[:4])
    return "\n".join(lines).strip()


def render_payflux_sales_brief() -> str:
    ctx = get_payflux_sales_context()
    story = ctx.get("plain_english_story") or []
    objections = ctx.get("main_objections") or []
    discovery = ctx.get("discovery_questions") or []
    return (
        f"Sales goal: {ctx.get('sales_goal')}\n"
        f"{ctx.get('core_reframe')}\n\n"
        f"Simple story: {_join_sentences(story, 3)}.\n\n"
        f"Main objections: {_join_sentences(objections, 3)}.\n\n"
        f"Best discovery questions: {_join_sentences(discovery, 3)}."
    ).strip()
