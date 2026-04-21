from __future__ import annotations

import copy


THREAD_CATEGORIES = {"merchant_distress", "customer_support", "noise_system"}
PROCESSORS = {
    "stripe",
    "paypal",
    "square",
    "adyen",
    "shopify_payments",
    "braintree",
    "authorize_net",
    "worldpay",
    "checkout_com",
    "unknown",
}
DISTRESS_TYPES = {
    "account_frozen",
    "payouts_delayed",
    "reserve_hold",
    "verification_review",
    "processor_switch_intent",
    "chargeback_issue",
    "account_terminated",
    "onboarding_rejected",
    "unknown",
}
DISTRESS_SIGNALS = {
    "frozen_funds",
    "payout_paused",
    "reserve_increase",
    "compliance_review",
    "website_verification",
    "kyc_kyb_review",
    "rolling_reserve",
    "processing_disabled",
    "looking_for_alternative",
    "negative_balance",
    "dispute_spike",
    "unknown",
}
INDUSTRIES = {
    "ecommerce",
    "saas",
    "agency",
    "creator",
    "marketplace",
    "health_wellness",
    "high_risk",
    "unknown",
}
REPLY_PRIORITIES = {"high", "medium", "low", "none"}
URGENCY_LEVELS = {"high", "medium", "low"}

TASK_TYPES = {
    "gmail_thread_classification_refinement",
    "gmail_processor_extraction_refinement",
    "gmail_distress_type_refinement",
    "merchant_identity_refinement",
    "opportunity_scoring_refinement",
    "reply_draft_refinement",
}

CLASSIFICATION_TASKS = {
    "gmail_thread_classification_refinement",
    "gmail_processor_extraction_refinement",
    "gmail_distress_type_refinement",
}


def schema_for_task(task_type: str) -> dict:
    if task_type in CLASSIFICATION_TASKS:
        return {
            "name": "classification_refinement",
            "fields": [
                "thread_category",
                "confidence",
                "processor",
                "distress_type",
                "distress_signals",
                "industry",
                "reply_priority",
                "reason_codes",
                "needs_escalation",
            ],
            "template": {
                "thread_category": "merchant_distress|customer_support|noise_system",
                "confidence": 0.0,
                "processor": "stripe|paypal|square|adyen|shopify_payments|braintree|authorize_net|worldpay|checkout_com|unknown",
                "distress_type": "account_frozen|payouts_delayed|reserve_hold|verification_review|processor_switch_intent|chargeback_issue|account_terminated|onboarding_rejected|unknown",
                "distress_signals": [],
                "industry": "ecommerce|saas|agency|creator|marketplace|health_wellness|high_risk|unknown",
                "reply_priority": "high|medium|low|none",
                "reason_codes": [],
                "needs_escalation": False,
            },
        }
    if task_type == "merchant_identity_refinement":
        return {
            "name": "merchant_identity_refinement",
            "fields": [
                "merchant_name_candidate",
                "merchant_domain_candidate",
                "merchant_identity_confidence",
                "evidence_spans",
                "reason_codes",
            ],
            "template": {
                "merchant_name_candidate": None,
                "merchant_domain_candidate": None,
                "merchant_identity_confidence": 0.0,
                "evidence_spans": [],
                "reason_codes": [],
            },
        }
    if task_type == "opportunity_scoring_refinement":
        return {
            "name": "opportunity_scoring_refinement",
            "fields": [
                "should_create_or_keep_opportunity",
                "opportunity_score_adjustment",
                "urgency_level",
                "reason_codes",
                "confidence",
            ],
            "template": {
                "should_create_or_keep_opportunity": True,
                "opportunity_score_adjustment": 0.0,
                "urgency_level": "medium",
                "reason_codes": [],
                "confidence": 0.0,
            },
        }
    if task_type == "reply_draft_refinement":
        return {
            "name": "reply_draft_refinement",
            "fields": [
                "approved_for_draft",
                "draft_style",
                "recommended_subject",
                "recommended_body",
                "questions_count",
                "reason_codes",
                "confidence",
            ],
            "template": {
                "approved_for_draft": True,
                "draft_style": "merchant_distress_concise",
                "recommended_subject": "",
                "recommended_body": "",
                "questions_count": 0,
                "reason_codes": [],
                "confidence": 0.0,
            },
        }
    raise ValueError(f"Unsupported reasoning task type: {task_type}")


def validate_task_output(task_type: str, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("model output must be a JSON object")

    if task_type in CLASSIFICATION_TASKS:
        category = _enum(payload.get("thread_category"), THREAD_CATEGORIES, "thread_category")
        processor = _enum(payload.get("processor"), PROCESSORS, "processor")
        distress_type = _enum(payload.get("distress_type"), DISTRESS_TYPES, "distress_type")
        industry = _enum(payload.get("industry"), INDUSTRIES, "industry")
        reply_priority = _enum(payload.get("reply_priority"), REPLY_PRIORITIES, "reply_priority")
        return {
            "thread_category": category,
            "confidence": _bounded_float(payload.get("confidence"), "confidence"),
            "processor": processor,
            "distress_type": distress_type,
            "distress_signals": _enum_list(payload.get("distress_signals"), DISTRESS_SIGNALS, "distress_signals"),
            "industry": industry,
            "reply_priority": reply_priority,
            "reason_codes": _string_list(payload.get("reason_codes"), "reason_codes"),
            "needs_escalation": bool(payload.get("needs_escalation", False)),
        }
    if task_type == "merchant_identity_refinement":
        merchant_domain = _nullable_string(payload.get("merchant_domain_candidate"))
        return {
            "merchant_name_candidate": _nullable_string(payload.get("merchant_name_candidate")),
            "merchant_domain_candidate": merchant_domain,
            "merchant_identity_confidence": _bounded_float(payload.get("merchant_identity_confidence"), "merchant_identity_confidence"),
            "evidence_spans": _string_list(payload.get("evidence_spans"), "evidence_spans"),
            "reason_codes": _string_list(payload.get("reason_codes"), "reason_codes"),
        }
    if task_type == "opportunity_scoring_refinement":
        return {
            "should_create_or_keep_opportunity": bool(payload.get("should_create_or_keep_opportunity")),
            "opportunity_score_adjustment": _bounded_float(
                payload.get("opportunity_score_adjustment"),
                "opportunity_score_adjustment",
                minimum=-100.0,
                maximum=100.0,
            ),
            "urgency_level": _enum(payload.get("urgency_level"), URGENCY_LEVELS, "urgency_level"),
            "reason_codes": _string_list(payload.get("reason_codes"), "reason_codes"),
            "confidence": _bounded_float(payload.get("confidence"), "confidence"),
        }
    if task_type == "reply_draft_refinement":
        return {
            "approved_for_draft": bool(payload.get("approved_for_draft")),
            "draft_style": _required_string(payload.get("draft_style"), "draft_style"),
            "recommended_subject": _required_string(payload.get("recommended_subject"), "recommended_subject"),
            "recommended_body": _required_string(payload.get("recommended_body"), "recommended_body"),
            "questions_count": _bounded_int(payload.get("questions_count"), "questions_count", minimum=0, maximum=5),
            "reason_codes": _string_list(payload.get("reason_codes"), "reason_codes"),
            "confidence": _bounded_float(payload.get("confidence"), "confidence"),
        }
    raise ValueError(f"Unsupported reasoning task type: {task_type}")


def merge_task_output(task_type: str, deterministic_result: dict, validated_output: dict) -> dict:
    merged = copy.deepcopy(deterministic_result or {})

    if task_type in CLASSIFICATION_TASKS:
        merged.update(
            {
                "thread_category": validated_output["thread_category"],
                "confidence": validated_output["confidence"],
                "processor": validated_output["processor"],
                "distress_type": validated_output["distress_type"],
                "distress_signals": validated_output["distress_signals"],
                "industry": validated_output["industry"],
                "reply_priority": validated_output["reply_priority"],
            }
        )
        metadata = dict(merged.get("metadata") or {})
        metadata["reasoning_reason_codes"] = validated_output["reason_codes"]
        metadata["reasoning_needs_escalation"] = validated_output["needs_escalation"]
        merged["metadata"] = metadata
        return merged

    if task_type == "merchant_identity_refinement":
        merged.update(
            {
                "merchant_name_candidate": validated_output["merchant_name_candidate"],
                "merchant_domain_candidate": validated_output["merchant_domain_candidate"],
                "merchant_identity_confidence": validated_output["merchant_identity_confidence"],
            }
        )
        metadata = dict(merged.get("metadata") or {})
        metadata["merchant_identity_reason_codes"] = validated_output["reason_codes"]
        metadata["merchant_identity_evidence_spans"] = validated_output["evidence_spans"]
        merged["metadata"] = metadata
        return merged

    if task_type == "opportunity_scoring_refinement":
        merged.update(validated_output)
        return merged

    if task_type == "reply_draft_refinement":
        merged.update(validated_output)
        return merged

    raise ValueError(f"Unsupported reasoning task type: {task_type}")


def _enum(value, allowed: set[str], field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if normalized not in allowed:
        raise ValueError(f"{field_name} must be one of: {sorted(allowed)}")
    return normalized


def _enum_list(value, allowed: set[str], field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    output = []
    for item in value:
        output.append(_enum(item, allowed, field_name))
    return output


def _string_list(value, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    output = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"{field_name} must contain strings")
        normalized = item.strip()
        if normalized:
            output.append(normalized)
    return output


def _nullable_string(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("value must be a string or null")
    normalized = value.strip()
    return normalized or None


def _required_string(value, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _bounded_float(value, field_name: str, minimum: float = 0.0, maximum: float = 1.0) -> float:
    try:
        converted = float(value)
    except Exception as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if converted < minimum or converted > maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return round(converted, 4)


def _bounded_int(value, field_name: str, minimum: int = 0, maximum: int = 100) -> int:
    try:
        converted = int(value)
    except Exception as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if converted < minimum or converted > maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}")
    return converted
