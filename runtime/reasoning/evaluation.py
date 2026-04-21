from __future__ import annotations

import copy
from statistics import mean

from runtime.reasoning.control_plane import (
    _build_messages,
    _should_escalate_to_tier2,
    reasoning_settings,
    route_reasoning_task,
)
from runtime.reasoning.eval_dataset import CURATED_GMAIL_REASONING_SET
from runtime.reasoning.providers import call_model, provider_is_configured
from runtime.reasoning.schemas import CLASSIFICATION_TASKS, merge_task_output, schema_for_task, validate_task_output


EVALUATION_STRATEGIES = ("deterministic_only", "tier1_only", "cascade")


def run_reasoning_evaluation() -> dict:
    dataset = copy.deepcopy(CURATED_GMAIL_REASONING_SET)
    per_sample = []
    strategy_results = {strategy: [] for strategy in EVALUATION_STRATEGIES}

    for sample in dataset:
        baseline = _evaluate_strategy(sample, "deterministic_only")
        tier1 = _evaluate_strategy(sample, "tier1_only")
        cascade = _evaluate_strategy(sample, "cascade")
        for strategy, result in (
            ("deterministic_only", baseline),
            ("tier1_only", tier1),
            ("cascade", cascade),
        ):
            strategy_results[strategy].append(result)
        per_sample.append(
            {
                "sample_id": sample["sample_id"],
                "task_type": sample["task_type"],
                "description": sample["description"],
                "deterministic_only": _sample_view(baseline),
                "tier1_only": _sample_view(tier1),
                "cascade": _sample_view(cascade),
            }
        )

    summaries = {
        strategy: _summarize_strategy(strategy_results[strategy], strategy_results["deterministic_only"])
        for strategy in EVALUATION_STRATEGIES
    }

    return {
        "sample_count": len(dataset),
        "task_distribution": _task_distribution(dataset),
        "curated_dataset": [{"sample_id": row["sample_id"], "task_type": row["task_type"], "description": row["description"]} for row in dataset],
        "strategy_summaries": summaries,
        "sample_results": per_sample,
    }


def _evaluate_strategy(sample: dict, strategy: str) -> dict:
    deterministic_result = copy.deepcopy(sample["deterministic_result"])
    context = copy.deepcopy(sample["context"])
    task_type = sample["task_type"]

    if strategy == "deterministic_only":
        final_decision = copy.deepcopy(deterministic_result)
        return _evaluation_result(
            sample=sample,
            strategy=strategy,
            routing_decision="tier_0_rules_only",
            provider=None,
            model=None,
            final_decision=final_decision,
            latency_ms=0.0,
            success=True,
            error=None,
            model_called=False,
            fallback_used=False,
        )

    if strategy == "tier1_only":
        route = _tier1_route()
        result = _run_route(sample, route, deterministic_result, context)
        return _evaluation_result(
            sample=sample,
            strategy=strategy,
            routing_decision=route["routing_decision"],
            provider=route["provider"],
            model=route["model"],
            final_decision=result["final_decision"],
            latency_ms=result["latency_ms"],
            success=result["success"],
            error=result["error"],
            model_called=result["model_called"],
            fallback_used=False,
        )

    route = route_reasoning_task(task_type, sample["item"], deterministic_result, context)
    if route["routing_decision"] == "tier_0_rules_only":
        return _evaluation_result(
            sample=sample,
            strategy=strategy,
            routing_decision="tier_0_rules_only",
            provider=None,
            model=None,
            final_decision=copy.deepcopy(deterministic_result),
            latency_ms=0.0,
            success=True,
            error=None,
            model_called=False,
            fallback_used=False,
        )

    if route["routing_decision"] == "tier_1_cheap_model":
        tier1 = _run_route(sample, route, deterministic_result, context)
        final_decision = copy.deepcopy(tier1["final_decision"])
        latency_ms = tier1["latency_ms"]
        success = tier1["success"]
        error = tier1["error"]
        model_called = tier1["model_called"]
        provider = route["provider"]
        model = route["model"]
        routing_decision = route["routing_decision"]
        if tier1["success"] and _should_escalate_to_tier2(task_type, final_decision, deterministic_result, context):
            tier2_route = _tier2_route(float(route["deterministic_confidence"]))
            tier2 = _run_route(sample, tier2_route, final_decision, context)
            if tier2["success"]:
                final_decision = tier2["final_decision"]
                latency_ms += tier2["latency_ms"]
                success = True
                error = None
                model_called = True
                provider = tier2_route["provider"]
                model = tier2_route["model"]
                routing_decision = tier2_route["routing_decision"]
        return _evaluation_result(
            sample=sample,
            strategy=strategy,
            routing_decision=routing_decision,
            provider=provider,
            model=model,
            final_decision=final_decision,
            latency_ms=latency_ms,
            success=success,
            error=error,
            model_called=model_called,
            fallback_used=False,
        )

    tier2 = _run_route(sample, route, deterministic_result, context)
    if tier2["success"]:
        return _evaluation_result(
            sample=sample,
            strategy=strategy,
            routing_decision=route["routing_decision"],
            provider=route["provider"],
            model=route["model"],
            final_decision=tier2["final_decision"],
            latency_ms=tier2["latency_ms"],
            success=True,
            error=None,
            model_called=True,
            fallback_used=False,
        )

    fallback_route = _tier1_route()
    fallback = _run_route(sample, fallback_route, deterministic_result, context)
    return _evaluation_result(
        sample=sample,
        strategy=strategy,
        routing_decision=fallback_route["routing_decision"],
        provider=fallback_route["provider"],
        model=fallback_route["model"],
        final_decision=fallback["final_decision"],
        latency_ms=tier2["latency_ms"] + fallback["latency_ms"],
        success=fallback["success"],
        error=fallback["error"] or tier2["error"],
        model_called=tier2["model_called"] or fallback["model_called"],
        fallback_used=True,
    )


def _run_route(sample: dict, route: dict, deterministic_result: dict, context: dict) -> dict:
    provider = route.get("provider") or ""
    model = route.get("model") or ""
    final_decision = copy.deepcopy(deterministic_result)
    if not provider or not model or not provider_is_configured(provider):
        return {
            "final_decision": final_decision,
            "latency_ms": 0.0,
            "success": False,
            "error": "provider_not_configured",
            "model_called": False,
        }

    task_type = sample["task_type"]
    schema_definition = schema_for_task(task_type)
    schema = {**schema_definition, "validate": lambda payload: validate_task_output(task_type, payload)}
    try:
        response = call_model(
            provider_name=provider,
            model_name=model,
            messages=_build_messages(task_type, sample["item"], deterministic_result, context, schema_definition["template"]),
            temperature=route["temperature"],
            max_tokens=route["max_tokens"],
            schema=schema,
            timeout_ms=route["timeout_ms"],
        )
        merged = merge_task_output(task_type, deterministic_result, response["validated_output"])
        return {
            "final_decision": merged,
            "latency_ms": float(response["latency_ms"] or 0.0),
            "success": True,
            "error": None,
            "model_called": True,
        }
    except Exception as exc:
        return {
            "final_decision": final_decision,
            "latency_ms": 0.0,
            "success": False,
            "error": str(exc),
            "model_called": True,
        }


def _evaluation_result(
    *,
    sample: dict,
    strategy: str,
    routing_decision: str,
    provider: str | None,
    model: str | None,
    final_decision: dict,
    latency_ms: float,
    success: bool,
    error: str | None,
    model_called: bool,
    fallback_used: bool,
) -> dict:
    score = _score_result(sample, final_decision)
    return {
        "sample_id": sample["sample_id"],
        "task_type": sample["task_type"],
        "strategy": strategy,
        "routing_decision": routing_decision,
        "provider": provider,
        "model": model,
        "final_decision": final_decision,
        "latency_ms": round(float(latency_ms or 0.0), 2),
        "success": bool(success),
        "error": error or "",
        "model_called": bool(model_called),
        "fallback_used": bool(fallback_used),
        "score": score,
    }


def _score_result(sample: dict, final_decision: dict) -> dict:
    expectations = sample["expectations"]
    task_type = sample["task_type"]
    checks = {}

    if task_type in CLASSIFICATION_TASKS:
        checks["category_match"] = final_decision.get("thread_category") == expectations["thread_category"]
        checks["processor_match"] = final_decision.get("processor") == expectations["processor"]
        checks["distress_type_match"] = final_decision.get("distress_type") == expectations["distress_type"]
        checks["reply_priority_match"] = final_decision.get("reply_priority") == expectations["reply_priority"]
        passed = all(checks.values())
        return {
            "passed": passed,
            "checks": checks,
            "merchant_distress_false_positive": expectations["thread_category"] != "merchant_distress"
            and final_decision.get("thread_category") == "merchant_distress",
            "merchant_distress_false_negative": expectations["thread_category"] == "merchant_distress"
            and final_decision.get("thread_category") != "merchant_distress",
        }

    if task_type == "merchant_identity_refinement":
        checks["name_match"] = _normalize(final_decision.get("merchant_name_candidate")) == _normalize(
            expectations["merchant_name_candidate"]
        )
        checks["domain_match"] = _normalize(final_decision.get("merchant_domain_candidate")) == _normalize(
            expectations["merchant_domain_candidate"]
        )
        checks["confidence_meets_min"] = float(final_decision.get("merchant_identity_confidence") or 0.0) >= float(
            expectations["min_identity_confidence"]
        )
        return {"passed": all(checks.values()), "checks": checks}

    if task_type == "opportunity_scoring_refinement":
        checks["should_create_match"] = bool(final_decision.get("should_create_or_keep_opportunity")) == bool(
            expectations["should_create_or_keep_opportunity"]
        )
        checks["urgency_match"] = final_decision.get("urgency_level") == expectations["urgency_level"]
        checks["confidence_meets_min"] = float(final_decision.get("confidence") or 0.0) >= float(
            expectations["min_confidence"]
        )
        return {"passed": all(checks.values()), "checks": checks}

    if task_type == "reply_draft_refinement":
        checks["approved_for_draft_match"] = bool(final_decision.get("approved_for_draft")) == bool(
            expectations["approved_for_draft"]
        )
        checks["style_match"] = final_decision.get("draft_style") == expectations["draft_style"]
        checks["questions_within_limit"] = int(final_decision.get("questions_count") or 0) <= int(
            expectations["max_questions_count"]
        )
        checks["body_present"] = bool((final_decision.get("recommended_body") or "").strip()) if expectations.get(
            "body_required"
        ) else True
        return {"passed": all(checks.values()), "checks": checks}

    return {"passed": False, "checks": {}}


def _summarize_strategy(results: list[dict], deterministic_results: list[dict]) -> dict:
    deterministic_by_sample = {row["sample_id"]: row for row in deterministic_results}
    classification_rows = [row for row in results if row["task_type"] in CLASSIFICATION_TASKS]
    identity_rows = [row for row in results if row["task_type"] == "merchant_identity_refinement"]
    opportunity_rows = [row for row in results if row["task_type"] == "opportunity_scoring_refinement"]
    draft_rows = [row for row in results if row["task_type"] == "reply_draft_refinement"]
    useful_refinements = 0
    for row in results:
        baseline = deterministic_by_sample[row["sample_id"]]
        if row["score"]["passed"] and not baseline["score"]["passed"]:
            useful_refinements += 1

    return {
        "sample_count": len(results),
        "model_call_rate": round(sum(1 for row in results if row["model_called"]) / len(results), 4) if results else 0.0,
        "avg_latency_ms": round(mean([row["latency_ms"] for row in results]) if results else 0.0, 2),
        "success_rate": round(sum(1 for row in results if row["success"]) / len(results), 4) if results else 0.0,
        "fallback_rate": round(sum(1 for row in results if row["fallback_used"]) / len(results), 4) if results else 0.0,
        "classification_accuracy": _rate(classification_rows, "category_match"),
        "classification_processor_accuracy": _rate(classification_rows, "processor_match"),
        "classification_distress_type_accuracy": _rate(classification_rows, "distress_type_match"),
        "classification_false_positives": sum(1 for row in classification_rows if row["score"]["merchant_distress_false_positive"]),
        "classification_false_negatives": sum(1 for row in classification_rows if row["score"]["merchant_distress_false_negative"]),
        "merchant_identity_domain_match_rate": _rate(identity_rows, "domain_match"),
        "merchant_identity_name_match_rate": _rate(identity_rows, "name_match"),
        "opportunity_precision": _rate(opportunity_rows, "should_create_match"),
        "draft_approval_accuracy": _rate(draft_rows, "approved_for_draft_match"),
        "useful_refinement_count": useful_refinements,
        "useful_refinement_rate": round(useful_refinements / len(results), 4) if results else 0.0,
        "estimated_cost_units": sum(_cost_units(row["routing_decision"]) for row in results),
        "estimated_cost_per_useful_refinement": round(
            sum(_cost_units(row["routing_decision"]) for row in results) / useful_refinements,
            4,
        )
        if useful_refinements
        else 0.0,
        "task_type_breakdown": {
            task_type: _summarize_task_group([row for row in results if row["task_type"] == task_type], deterministic_by_sample)
            for task_type in sorted({row["task_type"] for row in results})
        },
    }


def _task_distribution(dataset: list[dict]) -> dict:
    distribution = {}
    for row in dataset:
        distribution[row["task_type"]] = distribution.get(row["task_type"], 0) + 1
    return distribution


def _summarize_task_group(rows: list[dict], deterministic_by_sample: dict[str, dict]) -> dict:
    if not rows:
        return {"count": 0}
    useful_refinements = 0
    for row in rows:
        baseline = deterministic_by_sample[row["sample_id"]]
        if row["score"]["passed"] and not baseline["score"]["passed"]:
            useful_refinements += 1
    summary = {
        "count": len(rows),
        "model_call_rate": round(sum(1 for row in rows if row["model_called"]) / len(rows), 4),
        "success_rate": round(sum(1 for row in rows if row["success"]) / len(rows), 4),
        "useful_refinement_count": useful_refinements,
        "avg_latency_ms": round(mean([row["latency_ms"] for row in rows]), 2),
    }
    task_type = rows[0]["task_type"]
    if task_type in CLASSIFICATION_TASKS:
        summary.update(
            {
                "classification_accuracy": _rate(rows, "category_match"),
                "classification_processor_accuracy": _rate(rows, "processor_match"),
                "classification_distress_type_accuracy": _rate(rows, "distress_type_match"),
            }
        )
    elif task_type == "merchant_identity_refinement":
        summary.update(
            {
                "merchant_identity_domain_match_rate": _rate(rows, "domain_match"),
                "merchant_identity_name_match_rate": _rate(rows, "name_match"),
            }
        )
    elif task_type == "opportunity_scoring_refinement":
        summary.update({"opportunity_precision": _rate(rows, "should_create_match")})
    elif task_type == "reply_draft_refinement":
        summary.update({"draft_approval_accuracy": _rate(rows, "approved_for_draft_match")})
    return summary


def _rate(rows: list[dict], check_name: str) -> float:
    if not rows:
        return 0.0
    return round(sum(1 for row in rows if row["score"]["checks"].get(check_name)) / len(rows), 4)


def _sample_view(result: dict) -> dict:
    final_decision = result["final_decision"]
    return {
        "routing_decision": result["routing_decision"],
        "provider": result["provider"],
        "model": result["model"],
        "success": result["success"],
        "error": result["error"],
        "latency_ms": result["latency_ms"],
        "fallback_used": result["fallback_used"],
        "score": result["score"],
        "final_decision_preview": _preview_decision(final_decision),
    }


def _preview_decision(final_decision: dict) -> dict:
    preview_keys = (
        "thread_category",
        "processor",
        "distress_type",
        "reply_priority",
        "merchant_name_candidate",
        "merchant_domain_candidate",
        "merchant_identity_confidence",
        "should_create_or_keep_opportunity",
        "urgency_level",
        "approved_for_draft",
        "draft_style",
        "questions_count",
    )
    return {key: final_decision.get(key) for key in preview_keys if key in final_decision}


def _tier1_route() -> dict:
    settings = reasoning_settings()
    return {
        "routing_decision": "tier_1_cheap_model",
        "provider": settings["tier1_provider"],
        "model": settings["tier1_model"],
        "timeout_ms": settings["tier1_timeout_ms"],
        "max_tokens": settings["tier1_max_tokens"],
        "temperature": 0.0,
        "deterministic_confidence": 0.0,
    }


def _tier2_route(deterministic_confidence: float) -> dict:
    settings = reasoning_settings()
    return {
        "routing_decision": "tier_2_strong_model",
        "provider": settings["tier2_provider"],
        "model": settings["tier2_model"],
        "timeout_ms": settings["tier2_timeout_ms"],
        "max_tokens": settings["tier2_max_tokens"],
        "temperature": 0.1,
        "deterministic_confidence": deterministic_confidence,
    }


def _normalize(value) -> str:
    return str(value or "").strip().lower()


def _cost_units(routing_decision: str) -> int:
    if routing_decision == "tier_1_cheap_model":
        return 1
    if routing_decision == "tier_2_strong_model":
        return 3
    return 0
