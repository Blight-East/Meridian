from __future__ import annotations

from typing import Any
import requests

from runtime.health.telemetry import get_component_state, is_stale


def _clean_clause(text: str) -> str:
    return str(text or "").strip().rstrip(".")


def _safe_health_check() -> dict[str, Any]:
    try:
        response = requests.get("http://localhost:8000/health", timeout=2)
        response.raise_for_status()
        data = response.json()
        return {
            "healthy": str(data.get("status") or "").strip().lower() == "healthy",
            "detail": data.get("status") or "unknown",
        }
    except Exception as exc:
        return {"healthy": False, "detail": str(exc)}


def _component_running(name: str, *, stale_after_seconds: int = 600) -> tuple[bool, str]:
    state = get_component_state(name) or {}
    status = str(state.get("status") or "").strip().lower()
    heartbeat_at = str(state.get("last_heartbeat_at") or "").strip()
    if status == "running" and not is_stale(heartbeat_at, stale_after_seconds):
        return True, "running"
    if heartbeat_at and not is_stale(heartbeat_at, stale_after_seconds):
        return True, "heartbeat fresh"
    if status:
        return False, status
    return False, "no recent heartbeat"


def _operator_command_path_status() -> dict[str, Any]:
    from runtime.ops import operator_commands

    checks = {
        "show_action_envelopes_command": hasattr(operator_commands, "show_action_envelopes_command"),
        "show_security_incidents_command": hasattr(operator_commands, "show_security_incidents_command"),
        "fix_pipeline": hasattr(operator_commands, "fix_pipeline"),
        "show_prospect_scout_command": hasattr(operator_commands, "show_prospect_scout_command"),
        "show_daily_operator_briefing_command": hasattr(operator_commands, "show_daily_operator_briefing_command"),
    }
    healthy = all(bool(value) for value in checks.values())
    missing = [name for name, ok in checks.items() if not ok]
    return {"healthy": healthy, "checks": checks, "missing": missing}


def _probe_live_operator_paths() -> dict[str, Any]:
    from runtime.ops.operator_commands import (
        list_sent_outreach_needing_follow_up_command,
        show_action_envelopes_command,
        show_daily_operator_briefing_command,
        show_local_outreach_drafts_command,
        show_prospect_scout_command,
        show_reply_review_queue_command,
    )

    probes: dict[str, dict[str, Any]] = {}

    def _run_probe(name: str, fn):
        try:
            result = fn()
            probes[name] = {"healthy": True, "detail": "ok", "sample": result}
        except Exception as exc:
            probes[name] = {"healthy": False, "detail": str(exc)}

    _run_probe("action_envelopes", lambda: show_action_envelopes_command(limit=1, review_status=""))
    _run_probe("reply_review_queue", lambda: show_reply_review_queue_command(limit=1))
    _run_probe("follow_up_queue", lambda: list_sent_outreach_needing_follow_up_command(limit=1))
    _run_probe("local_drafts", lambda: show_local_outreach_drafts_command(limit=1))
    _run_probe("prospect_scout", lambda: show_prospect_scout_command(refresh=False))
    _run_probe("daily_briefing", lambda: show_daily_operator_briefing_command())
    return probes


def _image_support_status() -> dict[str, Any]:
    try:
        from runtime.conversation.chat_engine import generate_chat_response
        from runtime.telegram_bot import chat_image

        return {
            "healthy": callable(generate_chat_response) and callable(chat_image),
            "detail": "telegram image handler and vision path are present",
        }
    except Exception as exc:
        return {"healthy": False, "detail": str(exc)}


def get_meridian_capability_audit() -> dict[str, Any]:
    api = _safe_health_check()
    telegram_ok, telegram_detail = _component_running("telegram", stale_after_seconds=900)
    scheduler_ok, scheduler_detail = _component_running("scheduler", stale_after_seconds=900)
    operator_paths = _operator_command_path_status()
    live_probes = _probe_live_operator_paths()
    image_support = _image_support_status()
    return {
        "api": api,
        "telegram_control": {"healthy": telegram_ok, "detail": telegram_detail},
        "scheduler": {"healthy": scheduler_ok, "detail": scheduler_detail},
        "operator_command_paths": operator_paths,
        "live_probes": live_probes,
        "image_support": image_support,
    }


def get_meridian_capability_context() -> dict[str, Any]:
    audit = get_meridian_capability_audit()
    return {
        "already_built": [
            "Private Telegram operator control with persistent mission, identity, and mandate memory.",
            "Direct Meridian conversation surfaces in Telegram, terminal chat, and Claude/Desktop via the private Meridian MCP server.",
            "Product, ICP, sales, and company-goal context in the default conversation stack.",
            "A private Meridian MCP tool surface for queue review, draft review, rewrite, approval, sending, follow-up inspection, workflow queue processing, full deal decision packages, batch outcome marking, and system health.",
            "Queue ranking with ICP fit, commercial readiness, and brain-state alignment.",
            "Outbound drafting, rewriting, approval, sending, and follow-up handling.",
            "Reply monitoring, reply review, and reply-draft follow-up generation with adaptive rewrite style selection: the system reads outcome learning signals and automatically selects shorter, softer, direct, or standard styles based on what has worked.",
            "Reply-intent and buyer-signal classification for merchant replies.",
            "Outcome learning memory with automatic reply-context feedback on won, lost, or ignored threads.",
            "Prompt-engineering and regression-test workflow guidance folded into Meridian's runtime context for sharper rewrites, prompt changes, and safer operational changes.",
        ],
        "partial_capabilities": [
            "Reply understanding is stronger than before, but still based on snippets and message text heuristics rather than a fully rich merchant-conversation parser.",
            "Prospect sourcing works, but upstream merchant identity and contact quality are still noisy enough to starve the best queue.",
            "Outcome learning exists, but live conversion volume is still too thin to create deep statistical confidence.",
            "Meridian can produce proof and objection-handling assets, but the trust stack still lacks real case-study depth.",
            "Operator memory is real, but still lighter than a full long-horizon teammate memory system with broad shorthand decoding across every project and person.",
        ],
        "true_gaps": [
            "Better upstream merchant identity and authority discovery for high-conviction prospects.",
            "Stronger automatic classification of merchant objections, deferrals, and routing replies from full thread context.",
            "More trustworthy proof from real merchant outcomes, not just synthetic or illustrative artifacts.",
        ],
        "tool_families": [
            "Conversation surfaces: Telegram for direct operator chat, terminal chat for local direct use, and Meridian MCP for structured desktop/tool clients.",
            "Pipeline inspection: top opportunities, opportunity workbench, follow-up queue, reply review queue, draft queue, and daily briefing.",
            "Draft actions: rewrite outreach, approve outreach, send approved outreach, and auto-send only the highest-confidence Gmail outreach that clears strict trust gates.",
            "Browser and research: live web search for public-web discovery and fact-checking, browser page fetch for reading specific URLs, and browser screenshots for visual analysis.",
            "System visibility: capability audit, subsystem health checks, and live path probes.",
            "Reasoning inspection: reasoning decision log, individual decision review, reasoning metrics, reasoning evaluation harness, and reasoning task rerun.",
            "Gmail operations: Gmail triage cycle, Gmail thread intelligence, Gmail merchant distress screening, Gmail reply drafting, and Gmail reply sending.",
            "Workflow playbooks: prompt-engineering discipline for prompt and message changes, plus test-automation discipline for regression checks before risky workflow edits or releases.",
        ],
        "workflow_playbooks": [
            "Prompt-engineering playbook: use it when rewriting Meridian prompts, outreach copy, capability answers, system messages, or operator-facing explanations. Clarify the goal, audience, constraints, tone, and desired output shape before changing the wording.",
            "Prompt-engineering playbook: use it to improve weak prompts by tightening instruction order, separating rules from examples, reducing ambiguity, and naming what good output looks like.",
            "Test-automation playbook: use it when changing operator briefings, diagnostics, routing, attribution, delivery, or any workflow that can silently regress. Define the expected behavior first, then run targeted checks against real commands or deterministic helpers.",
            "Test-automation playbook: use it before rollout when a change affects production messaging, queue ranking, autonomy, or system health so Meridian can say what was verified and what still carries risk.",
        ],
        "mcp_tool_reference": [
            "meridian_get_top_opportunities: show the highest-priority queue cases that need operator attention now.",
            "meridian_get_opportunity_workbench: inspect one opportunity in detail before deciding what to do.",
            "meridian_get_follow_up_queue: review sent outreach that is due for follow-up.",
            "meridian_get_reply_review_queue: review merchant replies waiting for judgment.",
            "meridian_get_reply_review: inspect one reply thread deeply before classifying or responding.",
            "meridian_get_daily_briefing: get the current pipeline summary in one pass.",
            "meridian_get_outreach_drafts: list drafts that are ready for operator review.",
            "meridian_get_outreach_draft: inspect one specific draft before rewriting, approving, or sending.",
            "meridian_search_web: search the live public web for merchant research, official pages, and current external signals.",
            "meridian_get_system_health: inspect the live health of Meridian's operating stack.",
            "meridian_rewrite_outreach: revise a draft to fit the operator's requested tone or constraint.",
            "meridian_approve_outreach: approve a draft after review; approval is not sending.",
            "meridian_send_outreach: send an already approved draft when it is truly ready.",
            "meridian_process_queue: show the top actionable queue items with operator-required vs auto-executable split and decision context.",
            "meridian_review_and_decide: get the full decision package for one deal including lifecycle stage, history, priority, and recommended action.",
            "meridian_batch_outcome: batch-mark outcomes (won, lost, ignored) for multiple dead threads at once.",
        ],
        "search_strategy_rules": [
            "When the operator asks a search-style question, first identify what kind of question it is: decision, status, factual, person, document, or exploratory.",
            "Break broad questions into smaller source-specific checks instead of treating search as one vague step.",
            "Prefer the most authoritative source for the question type: live pipeline for status, product context for factual product truth, sales context for messaging, and live queue tools for merchant-specific work.",
            "Use live web search when you need current public-web facts, official-site discovery, or external corroboration of merchant distress.",
            "For merchant discovery work, use web search to find the likely official domain or source page first, then fall back to browser fetch or same-domain inspection.",
            "If one source is thin or noisy, broaden carefully instead of giving up immediately.",
            "When ambiguity is real and materially changes the answer, ask one tight clarifying question instead of guessing.",
            "When you use multiple sources, synthesize them into one plain-English answer instead of dumping disconnected fragments.",
        ],
        "memory_strategy_rules": [
            "Treat stable operator preferences, recurring shorthand, and repeated PayFlux language as memory, not as disposable context.",
            "Decode familiar shorthand from existing context before asking the operator to restate it.",
            "When a nickname, acronym, or internal phrase is unclear, ask once, learn it, and use the clarified meaning consistently afterward.",
            "Keep hot memory focused on the operator, active opportunities, active objections, and current commercial truth.",
            "Use deeper context only when needed for execution; do not flood normal replies with memory artifacts.",
            "Memory is loaded from stored operator profile, mission memory, and brain state at prompt time. You do not have a live write-back API for preferences. If you learn something new about the operator, use it within the conversation but be honest that persistence requires a code or data update.",
        ],
        "auto_send_rules": [
            "Auto-send only fires when the Gmail channel is set to auto_send_high_confidence mode.",
            "The composite auto-send confidence must reach at least 0.90 to clear the threshold.",
            "Hard gate minimums: contact trust score at least 90, commercial readiness at least 80, queue quality at least 70, and ICP fit at least 45.",
            "Only initial_outreach and follow_up_nudge types are eligible for auto-send. All other outreach types require operator review.",
            "Distress must be classified and not unknown. If the best play is clarify_distress, auto-send is blocked.",
            "After every auto-send, the operator gets a Telegram notification with the opportunity, contact, confidence, and Gmail thread ID.",
        ],
        "channel_mode_rules": [
            "Gmail and Reddit each have a channel mode: dry_run (no outbound at all), approval_required (operator approves each send), or auto_send_high_confidence (strict trust gates, autonomous high-confidence sends).",
            "The kill switch immediately stops all outbound for a channel regardless of mode. Use set_channel_kill_switch to enable or disable it.",
            "Only explicit operator commands should change channel modes. Do not change modes autonomously.",
        ],
        "self_knowledge_rules": [
            "Do not say you lack scheduling. Reply and follow-up monitors already exist.",
            "Do not say you lack outcome learning. It exists; the honest limitation is depth and automation quality.",
            "When asked what is missing, separate built capabilities from partial ones and true gaps.",
            "When asked what would make you more valuable, point to the highest-leverage missing capability rather than restating what is already live.",
            "Do not pretend Codex skills are live runtime plugins inside Meridian. The honest truth is that Meridian uses the behaviors and rules folded into his runtime context.",
            "When asked about the new prompt-engineering and test-automation skills, explain them as operating playbooks Meridian now follows inside his runtime context, not as separate magic tools.",
            "When asked what tools you have, name the real tool families clearly instead of speaking in generalities.",
            "When asked how you should use your tools, explain the sequence plainly: inspect, judge, rewrite if needed, approve, then send.",
            "When asked when to use prompt engineering, say it is for rewrites, prompt changes, operator messaging, and any change where clearer instructions improve output quality or reduce ambiguity.",
            "When asked when to use test automation, say it is for validating risky changes to briefings, routing, diagnostics, attribution, delivery, or autonomy before trusting the new behavior in production.",
            "When auto-send is enabled, describe it honestly using the concrete auto-send rules: the composite confidence threshold is 0.90, hard minimums exist for contact trust, commercial readiness, queue quality, and ICP fit, and the operator always gets a notification after the send.",
            "When the operator asks what you can do right now, distinguish direct conversation ability from structured MCP tool actions.",
            "When asked about outreach modes, name them plainly: dry_run stops all outbound, approval_required means the operator approves each send, and auto_send_high_confidence means strict trust gates with autonomous high-confidence sends. The kill switch immediately stops all outbound for a channel.",
            "When asked about web research, be specific: web_search is for discovery queries and fact-checking, browser_fetch is for reading a known URL, and browser_screenshot is for visual analysis. Prefer web_search for open-ended questions and browser_fetch when you already have a URL.",
            "When asked about reply handling, mention that the reply draft monitor automatically adjusts follow-up tone based on outcome learning signals, choosing between shorter, softer, direct, or standard styles.",
        ],
        "capability_audit": audit,
    }


def render_meridian_capability_context() -> str:
    ctx = get_meridian_capability_context()
    audit = ctx.get("capability_audit") or {}
    lines = ["MERIDIAN CAPABILITY CONTEXT"]
    lines.append("Already built:")
    lines.extend(f"- {item}" for item in ctx.get("already_built", [])[:10])
    lines.append("Partial capabilities:")
    lines.extend(f"- {item}" for item in ctx.get("partial_capabilities", [])[:6])
    lines.append("True gaps:")
    lines.extend(f"- {item}" for item in ctx.get("true_gaps", [])[:6])
    tool_families = ctx.get("tool_families") or []
    if tool_families:
        lines.append("Tool families:")
        lines.extend(f"- {item}" for item in tool_families[:8])
    mcp_tool_reference = ctx.get("mcp_tool_reference") or []
    if mcp_tool_reference:
        lines.append("MCP tool reference:")
        lines.extend(f"- {item}" for item in mcp_tool_reference[:16])
    search_rules = ctx.get("search_strategy_rules") or []
    if search_rules:
        lines.append("Search strategy rules:")
        lines.extend(f"- {item}" for item in search_rules[:8])
    memory_rules = ctx.get("memory_strategy_rules") or []
    if memory_rules:
        lines.append("Memory strategy rules:")
        lines.extend(f"- {item}" for item in memory_rules[:6])
    workflow_playbooks = ctx.get("workflow_playbooks") or []
    if workflow_playbooks:
        lines.append("Workflow playbooks:")
        lines.extend(f"- {item}" for item in workflow_playbooks[:6])
    auto_send_rules = ctx.get("auto_send_rules") or []
    if auto_send_rules:
        lines.append("Auto-send rules:")
        lines.extend(f"- {item}" for item in auto_send_rules[:6])
    channel_mode_rules = ctx.get("channel_mode_rules") or []
    if channel_mode_rules:
        lines.append("Channel mode rules:")
        lines.extend(f"- {item}" for item in channel_mode_rules[:4])
    lines.append("Self-knowledge rules:")
    lines.extend(f"- {item}" for item in ctx.get("self_knowledge_rules", [])[:12])
    lines.append("Live capability audit:")
    lines.append(f"- API: {'healthy' if (audit.get('api') or {}).get('healthy') else 'degraded'} ({(audit.get('api') or {}).get('detail') or 'unknown'})")
    lines.append(f"- Telegram control: {'healthy' if (audit.get('telegram_control') or {}).get('healthy') else 'degraded'} ({(audit.get('telegram_control') or {}).get('detail') or 'unknown'})")
    lines.append(f"- Scheduler: {'healthy' if (audit.get('scheduler') or {}).get('healthy') else 'degraded'} ({(audit.get('scheduler') or {}).get('detail') or 'unknown'})")
    command_paths = audit.get("operator_command_paths") or {}
    if command_paths:
        if command_paths.get("healthy"):
            lines.append("- Operator command paths: healthy")
        else:
            lines.append(f"- Operator command paths: degraded (missing: {', '.join(command_paths.get('missing') or ['unknown'])})")
    image_support = audit.get("image_support") or {}
    lines.append(f"- Image support: {'healthy' if image_support.get('healthy') else 'degraded'} ({image_support.get('detail') or 'unknown'})")
    probes = audit.get("live_probes") or {}
    if probes:
        healthy = [name for name, row in probes.items() if row.get("healthy")]
        degraded = [name for name, row in probes.items() if not row.get("healthy")]
        lines.append(f"- Live path probes healthy: {', '.join(healthy) if healthy else 'none'}")
        if degraded:
            lines.append(f"- Live path probes degraded: {', '.join(degraded)}")
    return "\n".join(lines).strip()


def render_meridian_capability_answer(question_type: str = "capabilities") -> str:
    ctx = get_meridian_capability_context()
    audit = ctx.get("capability_audit") or {}
    q = str(question_type or "capabilities").strip().lower()
    if q == "can_do":
        audit_bits = []
        if (audit.get("api") or {}).get("healthy"):
            audit_bits.append("the live API path is healthy")
        if (audit.get("telegram_control") or {}).get("healthy"):
            audit_bits.append("Telegram is live")
        if (audit.get("scheduler") or {}).get("healthy"):
            audit_bits.append("the scheduler is running")
        return (
            "I can do real operator work now. I can review the queue, inspect full deal context, rewrite outreach, approve it, send it when it is ready, run Gmail triage and reply-follow-up workflows, use browser and web research tools, inspect reasoning or channel-control state when needed, and apply prompt-engineering or regression-test discipline when I am changing prompts, messaging, or risky workflow logic. "
            + ("Right now " + ", ".join(audit_bits) + "." if audit_bits else "")
        )
    if q == "tools":
        return (
            "I have real tool families for pipeline inspection, draft actions, browser and research, Gmail operations, reasoning inspection, system visibility, workflow playbooks, and structured Meridian MCP workflows. "
            "The highest-leverage MCP workflow tools are meridian_process_queue for ranked action review, meridian_review_and_decide for a full single-deal decision package, and meridian_batch_outcome for bulk dead-thread cleanup. "
            "For the atomic path, I can inspect top opportunities, the opportunity workbench, follow-up queue, reply review queue, drafts, rewrite, approve, and send. "
            "For web work, I use web_search for discovery and current public facts, browser_fetch for reading a known URL, and browser_screenshot for visual analysis. "
            "I can also inspect reasoning logs and metrics, run Gmail triage, explain or change channel modes, and use the kill switch. "
            "For workflow quality, I now explicitly use a prompt-engineering playbook when I improve wording or instructions, and a test-automation playbook when I validate risky workflow changes before rollout. "
            "Memory is prompt-loaded from stored operator and mission context plus recent conversation. I do not have a live write-back memory API."
        )
    if q == "gaps":
        partial = ctx.get("partial_capabilities") or []
        gaps = ctx.get("true_gaps") or []
        live_issue = ""
        if not (audit.get("api") or {}).get("healthy"):
            live_issue = " Separately, the live API path is degraded right now. That is a temporary system issue, not a permanent capability gap."
        main_gap = _clean_clause(gaps[0] if gaps else "better upstream merchant identity and authority discovery")
        partial_one = _clean_clause(partial[0] if partial else "reply understanding still needs deeper thread-level interpretation")
        partial_two = _clean_clause(partial[1] if len(partial) > 1 else "prospect sourcing is still too noisy upstream")
        return (
            f"The biggest thing I am missing is {main_gap}. "
            f"I already have a real operating stack, but two parts are still too partial: {partial_one}; and {partial_two}. "
            f"If you want to make me more valuable fastest, start with {main_gap}."
            + live_issue
        )
    if q == "audit":
        operator_paths = audit.get("operator_command_paths") or {}
        if operator_paths.get("healthy"):
            command_line = "my critical operator command paths are loading cleanly"
        else:
            missing = ", ".join(operator_paths.get("missing") or ["unknown"])
            command_line = f"my operator command paths are degraded because these exports are missing: {missing}"
        probes = audit.get("live_probes") or {}
        healthy_paths = [name for name, row in probes.items() if row.get("healthy")]
        degraded_paths = [name for name, row in probes.items() if not row.get("healthy")]
        probe_line = (
            f"Live path checks are healthy for: {', '.join(healthy_paths)}."
            if healthy_paths
            else "I do not have any healthy live path checks right now."
        )
        if degraded_paths:
            probe_line = f"{probe_line} Degraded: {', '.join(degraded_paths)}."
        return (
            "Here is the live audit: "
            f"API is {'healthy' if (audit.get('api') or {}).get('healthy') else 'degraded'}; "
            f"Telegram control is {'healthy' if (audit.get('telegram_control') or {}).get('healthy') else 'degraded'}; "
            f"scheduler is {'healthy' if (audit.get('scheduler') or {}).get('healthy') else 'degraded'}; "
            f"and {command_line}. "
            f"{probe_line} "
            f"Image support is {'healthy' if (audit.get('image_support') or {}).get('healthy') else 'degraded'}."
        )
    return (
        "I already have a real operating stack: direct operator conversation, structured MCP workflows, queue review, draft review, outreach execution, browser research, reply monitoring, and outcome learning. "
        "I also know the honest sequence for using my tools: inspect, judge, rewrite if needed, approve, then send. "
        "The real limits are upstream prospect quality, deeper buyer and authority discovery, stronger proof from real merchant outcomes, and the fact that memory persistence is still static context rather than a live write-back API."
    )
