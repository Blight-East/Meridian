"""
Deterministic intent-only router. Substring routing is a runtime error.

Routes tasks ONLY by an explicit 'intent' field. If no intent is provided
or the intent is unknown, the task is forwarded to the LLM reasoner.

Any attempt to route by stringified payload matching is forbidden.
"""
from __future__ import annotations
from runtime.reasoning.claude import reason


class SubstringRoutingAttempted(Exception):
    """Raised if any caller attempts legacy substring-based routing."""
    pass


ALLOWED_INTENTS = frozenset({
    "scan_signals", "system_status", "ping", "help",
    "web_fetch", "web_search", "db_query",
    "fix_pipeline", "run_sales_cycle",
})


def dispatch(task):
    """Route a task dict by explicit intent. Falls through to reasoner."""
    if not isinstance(task, dict):
        raise SubstringRoutingAttempted(
            f"router_v2 requires a dict task; got {type(task).__name__}"
        )
    intent = (task.get("intent") or "").strip()
    if not intent:
        return reason(task)
    if intent not in ALLOWED_INTENTS:
        return reason(task)
    return {"action": intent, "notes": f"intent:{intent}"}


def assert_no_substring_routing(payload_str: str):
    """Runtime tripwire — any caller still trying the legacy path."""
    raise SubstringRoutingAttempted(
        f"substring routing attempted on payload: {payload_str[:80]}"
    )
