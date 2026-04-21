from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass

from dotenv import load_dotenv

from config.logging_config import get_logger
from runtime.health.telemetry import record_component_state, utc_now_iso
from runtime.reasoning.providers import ProviderError, call_model, provider_is_configured
from runtime.reasoning.schemas import (
    CLASSIFICATION_TASKS,
    TASK_TYPES,
    merge_task_output,
    schema_for_task,
    validate_task_output,
)
from runtime.reasoning.store import (
    ensure_reasoning_tables,
    get_reasoning_decision,
    get_reasoning_metrics_snapshot,
    list_reasoning_decisions,
    log_reasoning_decision,
)


logger = get_logger("reasoning_control_plane")
ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(ENV_FILE_PATH, override=False)

TIER_COST = {
    "tier_0_rules_only": 0.0,
    "tier_1_cheap_model": 1.0,
    "tier_2_strong_model": 3.0,
}


@dataclass
class RoutingDecision:
    routing_decision: str
    provider: str | None = None
    model: str | None = None
    timeout_ms: int = 0
    max_tokens: int = 0
    temperature: float = 0.0
    reason_codes: list[str] | None = None


def reasoning_settings() -> dict:
    return {
        "enabled": _env_bool("REASONING_CONTROL_PLANE_ENABLED", True),
        "tier1_provider": os.getenv("REASONING_TIER1_PROVIDER", "nvidia_openai_compatible").strip() or "",
        "tier1_model": os.getenv("REASONING_TIER1_MODEL", "openai/gpt-oss-20b").strip() or "",
        "tier2_provider": os.getenv("REASONING_TIER2_PROVIDER", "github_models").strip() or "",
        "tier2_model": os.getenv("REASONING_TIER2_MODEL", "openai/gpt-4.1-mini").strip() or "",
        "tier1_timeout_ms": _env_int("REASONING_TIER1_TIMEOUT_MS", 15000),
        "tier2_timeout_ms": _env_int("REASONING_TIER2_TIMEOUT_MS", 12000),
        "tier1_max_tokens": _env_int("REASONING_TIER1_MAX_TOKENS", 700),
        "tier2_max_tokens": _env_int("REASONING_TIER2_MAX_TOKENS", 900),
        "max_budget_per_item": _env_float("REASONING_MAX_BUDGET_PER_ITEM", 4.0),
        "reply_refinement_enabled": _env_bool("REASONING_REPLY_REFINEMENT_ENABLED", True),
        "opportunity_refinement_enabled": _env_bool("REASONING_OPPORTUNITY_REFINEMENT_ENABLED", True),
    }


def route_reasoning_task(task_type: str, item: dict, deterministic_result: dict, context: dict | None = None) -> dict:
    if task_type not in TASK_TYPES:
        raise ValueError(f"Unsupported reasoning task type: {task_type}")

    settings = reasoning_settings()
    context = context or {}
    deterministic_confidence = float(
        context.get("deterministic_confidence", deterministic_result.get("confidence", 0.0)) or 0.0
    )
    category = deterministic_result.get("thread_category") or context.get("thread_category") or ""
    processor = deterministic_result.get("processor") or ""
    distress_type = deterministic_result.get("distress_type") or ""
    merchant_identity_confidence = deterministic_result.get("merchant_identity_confidence")
    merchant_known = bool(context.get("merchant_known") or context.get("merchant_id"))
    opportunity_possible = bool(
        context.get("opportunity_impact_possible") or deterministic_result.get("opportunity_eligible")
    )
    high_value = bool(
        context.get("reply_priority") == "high"
        or opportunity_possible
        or context.get("urgency_level") == "high"
    )
    test_thread = bool(context.get("test_thread") or deterministic_result.get("test_thread"))
    hard_noise = bool(
        context.get("hard_suppression")
        or category == "noise_system"
        or context.get("sender_is_system")
    )
    reason_codes = []

    if not settings["enabled"]:
        reason_codes.append("control_plane_disabled")
        return _routing_dict(
            RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
            deterministic_confidence,
        )

    if test_thread or hard_noise:
        reason_codes.append("suppressed_noise_or_test")
        return _routing_dict(
            RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
            deterministic_confidence,
        )

    if task_type == "reply_draft_refinement" and not settings["reply_refinement_enabled"]:
        reason_codes.append("reply_refinement_disabled")
        return _routing_dict(
            RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
            deterministic_confidence,
        )

    if task_type == "opportunity_scoring_refinement" and not settings["opportunity_refinement_enabled"]:
        reason_codes.append("opportunity_refinement_disabled")
        return _routing_dict(
            RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
            deterministic_confidence,
        )

    if task_type in CLASSIFICATION_TASKS:
        reason_codes.append("classification_rules_default")
        return _routing_dict(
            RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
            deterministic_confidence,
        )

    if task_type == "merchant_identity_refinement":
        if merchant_known or (merchant_identity_confidence is not None and float(merchant_identity_confidence) >= 0.75):
            reason_codes.append("merchant_identity_already_sufficient")
            return _routing_dict(
                RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
                deterministic_confidence,
            )
        if not provider_is_configured(settings["tier1_provider"]):
            reason_codes.append("tier1_provider_unavailable")
            return _routing_dict(
                RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
                deterministic_confidence,
            )
        if not high_value and deterministic_confidence >= 0.65:
            reason_codes.append("merchant_identity_rules_sufficient_for_now")
            return _routing_dict(
                RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
                deterministic_confidence,
            )
        reason_codes.append("merchant_identity_priority_task")
        return _routing_dict(
            RoutingDecision(
                "tier_1_cheap_model",
                provider=settings["tier1_provider"],
                model=settings["tier1_model"],
                timeout_ms=settings["tier1_timeout_ms"],
                max_tokens=settings["tier1_max_tokens"],
                temperature=0.0,
                reason_codes=reason_codes,
            ),
            deterministic_confidence,
        )

    if task_type == "opportunity_scoring_refinement":
        if not opportunity_possible:
            reason_codes.append("no_opportunity_impact")
            return _routing_dict(
                RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
                deterministic_confidence,
            )
        if deterministic_confidence >= 0.85 and merchant_known:
            reason_codes.append("rules_decisive_for_opportunity")
            return _routing_dict(
                RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
                deterministic_confidence,
            )
        if not provider_is_configured(settings["tier1_provider"]):
            reason_codes.append("tier1_provider_unavailable")
            return _routing_dict(
                RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
                deterministic_confidence,
            )
        if not high_value:
            reason_codes.append("opportunity_rules_default")
            return _routing_dict(
                RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
                deterministic_confidence,
            )
        reason_codes.append("opportunity_secondary_priority_task")
        return _routing_dict(
            RoutingDecision(
                "tier_1_cheap_model",
                provider=settings["tier1_provider"],
                model=settings["tier1_model"],
                timeout_ms=settings["tier1_timeout_ms"],
                max_tokens=settings["tier1_max_tokens"],
                temperature=0.0,
                reason_codes=reason_codes,
            ),
            deterministic_confidence,
        )

    if task_type == "reply_draft_refinement":
        reason_codes.append("reply_rules_default")
        return _routing_dict(
            RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
            deterministic_confidence,
        )

    reason_codes.append("default_rules_only")
    return _routing_dict(
        RoutingDecision("tier_0_rules_only", reason_codes=reason_codes),
        deterministic_confidence,
    )


def run_reasoning_task(
    task_type: str,
    item: dict,
    deterministic_result: dict,
    context: dict | None = None,
) -> dict:
    ensure_reasoning_tables()
    context = context or {}
    item_type = context.get("item_type", "gmail_thread")
    item_id = str(context.get("item_id") or item.get("thread_id") or item.get("signal_id") or "unknown")
    channel = context.get("channel", "gmail")
    route = route_reasoning_task(task_type, item, deterministic_result, context)
    final_decision = copy.deepcopy(deterministic_result or {})
    deterministic_confidence = route["deterministic_confidence"]

    if route["routing_decision"] == "tier_0_rules_only":
        decision_id = log_reasoning_decision(
            item_type=item_type,
            item_id=item_id,
            channel=channel,
            task_type=task_type,
            routing_decision=route["routing_decision"],
            deterministic_confidence=deterministic_confidence,
            provider=None,
            model=None,
            raw_model_output={},
            validated_output={},
            final_decision=final_decision,
            success=True,
            error=None,
            fallback_used=False,
            latency_ms=0.0,
        )
        _record_reasoning_state("healthy", task_type=task_type, route=route["routing_decision"])
        return {"decision_id": decision_id, "routing": route, "final_decision": final_decision}

    if TIER_COST[route["routing_decision"]] > reasoning_settings()["max_budget_per_item"]:
        decision_id = log_reasoning_decision(
            item_type=item_type,
            item_id=item_id,
            channel=channel,
            task_type=task_type,
            routing_decision=route["routing_decision"],
            deterministic_confidence=deterministic_confidence,
            provider=route["provider"],
            model=route["model"],
            raw_model_output={},
            validated_output={},
            final_decision=final_decision,
            success=False,
            error="budget_exceeded",
            fallback_used=False,
            latency_ms=0.0,
        )
        _record_reasoning_state("degraded", task_type=task_type, route=route["routing_decision"], error="budget_exceeded")
        return {"decision_id": decision_id, "routing": route, "final_decision": final_decision}

    result = _invoke_reasoning_model(
        route=route,
        task_type=task_type,
        item=item,
        deterministic_result=deterministic_result,
        context=context,
        item_type=item_type,
        item_id=item_id,
        channel=channel,
        fallback_used=False,
    )
    if result.get("error"):
        fallback = _fallback_after_provider_error(
            route=route,
            result=result,
            task_type=task_type,
            item=item,
            deterministic_result=deterministic_result,
            context=context,
            item_type=item_type,
            item_id=item_id,
            channel=channel,
            deterministic_confidence=deterministic_confidence,
        )
        if fallback is None:
            return result
        route = fallback["route"]
        result = fallback["result"]
        if result.get("error"):
            return result

    validated_output = result["validated_output"]
    final_decision = merge_task_output(task_type, deterministic_result, validated_output)
    decision_id = log_reasoning_decision(
        item_type=item_type,
        item_id=item_id,
        channel=channel,
        task_type=task_type,
        routing_decision=route["routing_decision"],
        deterministic_confidence=deterministic_confidence,
        provider=route["provider"],
        model=route["model"],
        raw_model_output=result["raw_output"],
        validated_output=validated_output,
        final_decision=final_decision,
        success=True,
        error=None,
        fallback_used=bool(result.get("fallback_used")),
        latency_ms=result["latency_ms"],
    )

    if route["routing_decision"] == "tier_1_cheap_model" and _should_escalate_to_tier2(
        task_type,
        validated_output,
        deterministic_result,
        context,
    ):
        tier2_route = _routing_dict(
            RoutingDecision(
                "tier_2_strong_model",
                provider=reasoning_settings()["tier2_provider"],
                model=reasoning_settings()["tier2_model"],
                timeout_ms=reasoning_settings()["tier2_timeout_ms"],
                max_tokens=reasoning_settings()["tier2_max_tokens"],
                temperature=0.1,
                reason_codes=["tier1_needs_escalation"],
            ),
            deterministic_confidence,
        )
        if TIER_COST[tier2_route["routing_decision"]] <= reasoning_settings()["max_budget_per_item"]:
            tier2_result = _invoke_reasoning_model(
                route=tier2_route,
                task_type=task_type,
                item=item,
                deterministic_result=final_decision,
                context=context,
                item_type=item_type,
                item_id=item_id,
                channel=channel,
                fallback_used=False,
            )
            if not tier2_result.get("error"):
                tier2_validated = tier2_result["validated_output"]
                final_decision = merge_task_output(task_type, final_decision, tier2_validated)
                decision_id = log_reasoning_decision(
                    item_type=item_type,
                    item_id=item_id,
                    channel=channel,
                    task_type=task_type,
                    routing_decision=tier2_route["routing_decision"],
                    deterministic_confidence=deterministic_confidence,
                    provider=tier2_route["provider"],
                    model=tier2_route["model"],
                    raw_model_output=tier2_result["raw_output"],
                    validated_output=tier2_validated,
                    final_decision=final_decision,
                    success=True,
                    error=None,
                    fallback_used=False,
                    latency_ms=tier2_result["latency_ms"],
                )
                route = tier2_route

    _record_reasoning_state("healthy", task_type=task_type, route=route["routing_decision"])
    return {"decision_id": decision_id, "routing": route, "final_decision": final_decision}


def show_reasoning_decision_log(*, limit: int = 25, task_type: str = "", item_id: str = "") -> dict:
    entries = list_reasoning_decisions(limit=limit, task_type=task_type or None, item_id=item_id or None)
    return {"count": len(entries), "entries": entries}


def show_reasoning_decision(decision_id: int) -> dict:
    row = get_reasoning_decision(decision_id)
    if not row:
        return {"status": "error", "error": "reasoning_decision_not_found"}
    return {"decision": row}


def show_reasoning_metrics() -> dict:
    return get_reasoning_metrics_snapshot()


def rerun_reasoning_task(task_type: str, item_id: str) -> dict:
    if task_type not in TASK_TYPES:
        return {"status": "error", "error": "unsupported_reasoning_task"}

    if task_type in CLASSIFICATION_TASKS or task_type in {"merchant_identity_refinement", "reply_draft_refinement"}:
        from runtime.channels.gmail_adapter import GmailAdapter
        from runtime.channels.gmail_triage import sync_gmail_thread_intelligence
        from runtime.channels.templates import build_gmail_reply_deterministic

        adapter = GmailAdapter()
        target = adapter.read_context(item_id)
        intelligence = sync_gmail_thread_intelligence(target, create_signal=False, force=True)
        if task_type == "reply_draft_refinement":
            target.metadata["gmail_thread_intelligence"] = intelligence
            draft = build_gmail_reply_deterministic(target)
            deterministic_result = {
                "approved_for_draft": intelligence.get("thread_category") == "merchant_distress",
                "draft_style": "merchant_distress_concise",
                "recommended_subject": draft.subject,
                "recommended_body": draft.text,
                "questions_count": draft.text.count("?"),
                "reason_codes": list((draft.metadata or {}).get("reason_codes", [])),
                "confidence": draft.confidence,
            }
            item = {
                "thread_id": target.thread_id,
                "subject": target.title,
                "body": target.body[:2500],
                "draft_subject": draft.subject,
                "draft_body": draft.text,
            }
        elif task_type == "merchant_identity_refinement":
            deterministic_result = {
                "merchant_name_candidate": intelligence.get("merchant_name_candidate"),
                "merchant_domain_candidate": intelligence.get("merchant_domain_candidate"),
                "merchant_identity_confidence": intelligence.get("merchant_identity_confidence") or 0.0,
                "metadata": intelligence.get("metadata", {}),
            }
            item = {
                "thread_id": target.thread_id,
                "subject": target.title,
                "body": target.body[:2500],
                "sender_email": target.metadata.get("sender_email"),
                "sender_name": target.metadata.get("sender_name"),
            }
        else:
            deterministic_result = intelligence
            item = {
                "thread_id": target.thread_id,
                "subject": target.title,
                "body": target.body[:2500],
                "sender_email": target.metadata.get("sender_email"),
                "sender_name": target.metadata.get("sender_name"),
            }
        return run_reasoning_task(
            task_type,
            item,
            deterministic_result,
            context=_gmail_reasoning_context(target, intelligence),
        )

    if task_type == "opportunity_scoring_refinement":
        from sqlalchemy import text
        from memory.structured.db import engine

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT gti.thread_id, gti.thread_category, gti.confidence, gti.reply_priority,
                           gti.signal_id, gti.opportunity_id, gti.processor, gti.distress_type,
                           gti.merchant_name_candidate, gti.merchant_domain_candidate,
                           gti.merchant_identity_confidence, gti.opportunity_eligible, s.merchant_id
                    FROM gmail_thread_intelligence gti
                    LEFT JOIN signals s ON s.id = gti.signal_id
                    WHERE gti.signal_id = :signal_id
                    LIMIT 1
                    """
                ),
                {"signal_id": int(item_id)},
            ).fetchone()
        if not row:
            return {"status": "error", "error": "opportunity_source_not_found"}
        intelligence = dict(row._mapping)
        deterministic_result = {
            "should_create_or_keep_opportunity": bool(intelligence.get("opportunity_eligible")),
            "opportunity_score_adjustment": 0.0,
            "urgency_level": "high" if intelligence.get("reply_priority") == "high" else "medium",
            "reason_codes": ["deterministic_opportunity"],
            "confidence": float(intelligence.get("confidence") or 0.0),
        }
        return run_reasoning_task(
            task_type,
            {
                "signal_id": int(item_id),
                "thread_id": intelligence.get("thread_id"),
                "processor": intelligence.get("processor"),
                "distress_type": intelligence.get("distress_type"),
            },
            deterministic_result,
            context={
                "item_type": "signal",
                "item_id": str(item_id),
                "channel": "gmail",
                "thread_category": intelligence.get("thread_category"),
                "deterministic_confidence": float(intelligence.get("confidence") or 0.0),
                "reply_priority": intelligence.get("reply_priority"),
                "opportunity_impact_possible": bool(intelligence.get("opportunity_eligible")),
                "merchant_known": bool(intelligence.get("merchant_id")),
                "merchant_identity_confidence": intelligence.get("merchant_identity_confidence"),
            },
        )

    return {"status": "error", "error": "unsupported_reasoning_rerun"}


def _invoke_reasoning_model(
    *,
    route: dict,
    task_type: str,
    item: dict,
    deterministic_result: dict,
    context: dict,
    item_type: str,
    item_id: str,
    channel: str,
    fallback_used: bool,
) -> dict:
    provider = route.get("provider") or ""
    model = route.get("model") or ""
    final_decision = copy.deepcopy(deterministic_result or {})
    schema_definition = schema_for_task(task_type)
    schema = {
        **schema_definition,
        "validate": lambda payload: validate_task_output(task_type, payload),
    }
    if not provider or not model or not provider_is_configured(provider):
        decision_id = log_reasoning_decision(
            item_type=item_type,
            item_id=item_id,
            channel=channel,
            task_type=task_type,
            routing_decision=route["routing_decision"],
            deterministic_confidence=route["deterministic_confidence"],
            provider=provider or None,
            model=model or None,
            raw_model_output={},
            validated_output={},
            final_decision=final_decision,
            success=False,
            error="provider_not_configured",
            fallback_used=fallback_used,
            latency_ms=0.0,
        )
        _record_reasoning_state("degraded", task_type=task_type, route=route["routing_decision"], error="provider_not_configured")
        return {"decision_id": decision_id, "routing": route, "final_decision": final_decision, "error": "provider_not_configured"}

    try:
        response = call_model(
            provider_name=provider,
            model_name=model,
            messages=_build_messages(task_type, item, deterministic_result, context, schema_definition["template"]),
            temperature=route["temperature"],
            max_tokens=route["max_tokens"],
            schema=schema,
            timeout_ms=route["timeout_ms"],
        )
        response["fallback_used"] = fallback_used
        return response
    except ProviderError as exc:
        decision_id = log_reasoning_decision(
            item_type=item_type,
            item_id=item_id,
            channel=channel,
            task_type=task_type,
            routing_decision=route["routing_decision"],
            deterministic_confidence=route["deterministic_confidence"],
            provider=provider,
            model=model,
            raw_model_output=getattr(exc, "raw_output", {}),
            validated_output={},
            final_decision=final_decision,
            success=False,
            error=str(exc),
            fallback_used=fallback_used,
            latency_ms=0.0,
        )
        _record_reasoning_state("degraded", task_type=task_type, route=route["routing_decision"], error=str(exc))
        return {"decision_id": decision_id, "routing": route, "final_decision": final_decision, "error": str(exc)}
    except Exception as exc:
        decision_id = log_reasoning_decision(
            item_type=item_type,
            item_id=item_id,
            channel=channel,
            task_type=task_type,
            routing_decision=route["routing_decision"],
            deterministic_confidence=route["deterministic_confidence"],
            provider=provider,
            model=model,
            raw_model_output={},
            validated_output={},
            final_decision=final_decision,
            success=False,
            error=str(exc),
            fallback_used=fallback_used,
            latency_ms=0.0,
        )
        _record_reasoning_state("degraded", task_type=task_type, route=route["routing_decision"], error=str(exc))
        return {"decision_id": decision_id, "routing": route, "final_decision": final_decision, "error": str(exc)}


def _fallback_after_provider_error(
    *,
    route: dict,
    result: dict,
    task_type: str,
    item: dict,
    deterministic_result: dict,
    context: dict,
    item_type: str,
    item_id: str,
    channel: str,
    deterministic_confidence: float,
):
    if route.get("routing_decision") != "tier_2_strong_model":
        return None
    settings = reasoning_settings()
    tier1_provider = settings["tier1_provider"]
    tier1_model = settings["tier1_model"]
    if not tier1_provider or not tier1_model or not provider_is_configured(tier1_provider):
        return None
    if TIER_COST["tier_1_cheap_model"] > settings["max_budget_per_item"]:
        return None
    fallback_route = _routing_dict(
        RoutingDecision(
            "tier_1_cheap_model",
            provider=tier1_provider,
            model=tier1_model,
            timeout_ms=settings["tier1_timeout_ms"],
            max_tokens=settings["tier1_max_tokens"],
            temperature=0.0,
            reason_codes=["tier2_failed_fallback_to_tier1"],
        ),
        deterministic_confidence,
    )
    fallback_result = _invoke_reasoning_model(
        route=fallback_route,
        task_type=task_type,
        item=item,
        deterministic_result=deterministic_result,
        context=context,
        item_type=item_type,
        item_id=item_id,
        channel=channel,
        fallback_used=True,
    )
    fallback_result["fallback_used"] = True
    return {"route": fallback_route, "result": fallback_result}


def _build_messages(task_type: str, item: dict, deterministic_result: dict, context: dict, schema_template: dict) -> list[dict]:
    system = (
        "You are the PayFlux reasoning control plane. "
        "Return valid JSON only. Do not include markdown, explanations, or extra text. "
        "Use only the allowed enum values shown in the schema template."
    )
    user_payload = {
        "task_type": task_type,
        "item": _trim_payload(item),
        "deterministic_result": _trim_payload(deterministic_result),
        "context": _trim_payload(context),
        "schema_template": schema_template,
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=True)},
    ]


def _trim_payload(payload):
    if isinstance(payload, dict):
        trimmed = {}
        for key, value in payload.items():
            trimmed[key] = _trim_payload(value)
        return trimmed
    if isinstance(payload, list):
        return [_trim_payload(value) for value in payload[:12]]
    if isinstance(payload, str):
        return payload[:3000]
    return payload


def _should_escalate_to_tier2(task_type: str, validated_output: dict, deterministic_result: dict, context: dict) -> bool:
    high_value = bool(
        context.get("reply_priority") == "high"
        or context.get("opportunity_impact_possible")
        or deterministic_result.get("opportunity_eligible")
    )
    if not high_value or not provider_is_configured(reasoning_settings()["tier2_provider"]):
        return False
    if task_type == "merchant_identity_refinement":
        return (
            float(validated_output.get("merchant_identity_confidence", 0.0)) < 0.6
            or not validated_output.get("merchant_domain_candidate")
            or not validated_output.get("merchant_name_candidate")
        )
    if task_type == "opportunity_scoring_refinement":
        return float(validated_output.get("confidence", 0.0)) < 0.6
    return False


def _record_reasoning_state(status: str, *, task_type: str, route: str, error: str = ""):
    record_component_state(
        "reasoning",
        ttl=900,
        reasoning_status=status,
        last_reasoning_task=task_type,
        last_reasoning_route=route,
        last_reasoning_error=error,
        last_reasoning_at=utc_now_iso(),
    )


def _routing_dict(decision: RoutingDecision, deterministic_confidence: float) -> dict:
    return {
        "routing_decision": decision.routing_decision,
        "provider": decision.provider,
        "model": decision.model,
        "timeout_ms": decision.timeout_ms,
        "max_tokens": decision.max_tokens,
        "temperature": decision.temperature,
        "reason_codes": decision.reason_codes or [],
        "deterministic_confidence": deterministic_confidence,
    }


def _gmail_reasoning_context(target, intelligence: dict) -> dict:
    metadata = intelligence.get("metadata") or {}
    return {
        "item_type": "gmail_thread",
        "item_id": target.thread_id,
        "channel": "gmail",
        "thread_category": intelligence.get("thread_category"),
        "deterministic_confidence": float(intelligence.get("confidence", 0.0) or 0.0),
        "reply_priority": intelligence.get("reply_priority"),
        "opportunity_impact_possible": bool(intelligence.get("opportunity_eligible")),
        "merchant_known": bool(intelligence.get("merchant_id")),
        "merchant_identity_confidence": intelligence.get("merchant_identity_confidence"),
        "test_thread": bool(intelligence.get("test_thread")),
        "hard_suppression": bool(intelligence.get("thread_category") == "noise_system"),
        "sender_is_system": bool(metadata.get("sender_is_automated")),
    }


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default
