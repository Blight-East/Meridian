from __future__ import annotations

import re


CANONICAL_DISTRESS_TYPES = {
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

CANONICAL_OPERATOR_ACTIONS = {
    "urgent_processor_migration",
    "payout_acceleration",
    "reserve_negotiation",
    "compliance_remediation",
    "chargeback_mitigation",
    "onboarding_assistance",
    "clarify_distress",
}

DISTRESS_STRATEGY_MAP = {
    "account_frozen": "urgent processor migration",
    "payouts_delayed": "payout acceleration strategy",
    "reserve_hold": "reserve negotiation strategy",
    "verification_review": "compliance remediation support",
    "processor_switch_intent": "merchant onboarding assistance",
    "chargeback_issue": "chargeback mitigation plus processor advisory",
    "account_terminated": "replacement processor recovery plan",
    "onboarding_rejected": "onboarding rescue and alternative processor placement",
    "unknown": "clarify the distress before escalating",
}

DISTRESS_OPERATOR_ACTION_MAP = {
    "account_frozen": "urgent_processor_migration",
    "account_terminated": "urgent_processor_migration",
    "payouts_delayed": "payout_acceleration",
    "reserve_hold": "reserve_negotiation",
    "verification_review": "compliance_remediation",
    "chargeback_issue": "chargeback_mitigation",
    "processor_switch_intent": "onboarding_assistance",
    "onboarding_rejected": "onboarding_assistance",
    "unknown": "clarify_distress",
}

_NORMALIZATION_PATTERNS = {
    "account_frozen": (
        r"\baccount frozen\b",
        r"\baccounts frozen\b",
        r"\bfrozen account\b",
        r"\bfroze\b",
        r"\bfrozen funds\b",
        r"\bfunds frozen\b",
        r"\bfunds held\b",
        r"\bfunds on hold\b",
        r"\bpayments disabled\b",
        r"\bprocessing disabled\b",
    ),
    "payouts_delayed": (
        r"\bpayout delay(?:ed)?\b",
        r"\bpayouts delay(?:ed)?\b",
        r"\bpayout paused\b",
        r"\bpayouts paused\b",
        r"\bdelayed payout\b",
    ),
    "reserve_hold": (
        r"\breserve\b",
        r"\breserve hold\b",
        r"\breserve increase\b",
        r"\brolling reserve\b",
    ),
    "verification_review": (
        r"\bcompliance review\b",
        r"\bverification review\b",
        r"\bkyc review\b",
        r"\bkyb review\b",
        r"\bkyc\b",
        r"\bkyb\b",
        r"\bwebsite verification\b",
        r"\bunder review\b",
    ),
    "processor_switch_intent": (
        r"\blooking for a new processor\b",
        r"\bneed a new payment processor\b",
        r"\bneed a processor\b",
        r"\bprocessor alternative\b",
        r"\balternative to stripe\b",
        r"\balternative to paypal\b",
    ),
    "chargeback_issue": (
        r"\bchargeback issue\b",
        r"\bchargeback spike\b",
        r"\bdispute spike\b",
        r"\bnegative balance\b",
    ),
    "account_terminated": (
        r"\baccount terminated\b",
        r"\baccount closed\b",
        r"\bterminated account\b",
        r"\btermination\b",
        r"\b(?:payment gateway|gateway|processor)\b.*\bstopped accepting\b",
        r"\bstopped accepting\b.*\b(?:payments?|cards?|orders?|sales)\b",
        r"\b(?:payment gateway|gateway)\b.*\b(?:stopped|cut off|shut off)\b",
        r"\bprocessor\b.*\b(?:stopped|closed|cut off|shut off)\b",
    ),
    "onboarding_rejected": (
        r"\bonboarding rejected\b",
        r"\bapplication denied\b",
        r"\bapplication rejected\b",
        r"\bmerchant account denied\b",
    ),
}


def normalize_distress_topic(raw_topic: str | None) -> str:
    if not raw_topic:
        return "unknown"

    lowered = str(raw_topic).strip().lower()
    if not lowered:
        return "unknown"

    normalized = lowered.replace("-", "_").replace("/", " ").replace("&", " and ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    canonical_candidate = normalized.replace(" ", "_")
    if canonical_candidate in CANONICAL_DISTRESS_TYPES:
        return canonical_candidate

    generic_unknowns = {
        "unknown",
        "merchant distress",
        "unclassified merchant distress",
        "consumer complaints",
        "consumer complaint",
        "testing distress gate",
    }
    if normalized in generic_unknowns:
        return "unknown"

    for canonical, patterns in _NORMALIZATION_PATTERNS.items():
        if any(re.search(pattern, normalized) for pattern in patterns):
            return canonical

    return "unknown"


def strategy_for_distress(distress_type: str | None) -> str:
    normalized = normalize_distress_topic(distress_type)
    return DISTRESS_STRATEGY_MAP.get(normalized, DISTRESS_STRATEGY_MAP["unknown"])


def operator_action_for_distress(distress_type: str | None) -> str:
    normalized = normalize_distress_topic(distress_type)
    return DISTRESS_OPERATOR_ACTION_MAP.get(normalized, DISTRESS_OPERATOR_ACTION_MAP["unknown"])


def normalize_operator_action(raw_action: str | None) -> str:
    if not raw_action:
        return "clarify_distress"
    normalized = str(raw_action).strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "urgent_processor_migration": "urgent_processor_migration",
        "processor_migration": "urgent_processor_migration",
        "payout_acceleration": "payout_acceleration",
        "reserve_negotiation": "reserve_negotiation",
        "compliance_remediation": "compliance_remediation",
        "chargeback_mitigation": "chargeback_mitigation",
        "onboarding_assistance": "onboarding_assistance",
        "clarify_distress": "clarify_distress",
    }
    return aliases.get(normalized, "clarify_distress")


def operator_action_label(action: str | None) -> str:
    normalized = normalize_operator_action(action)
    labels = {
        "urgent_processor_migration": "urgent processor migration",
        "payout_acceleration": "payout acceleration",
        "reserve_negotiation": "reserve negotiation",
        "compliance_remediation": "compliance remediation",
        "chargeback_mitigation": "chargeback mitigation",
        "onboarding_assistance": "onboarding assistance",
        "clarify_distress": "distress clarification",
    }
    return labels.get(normalized, labels["clarify_distress"])


def doctrine_priority_reason(
    *,
    processor: str | None = None,
    distress_type: str | None = None,
    industry: str | None = None,
) -> str:
    normalized_distress = normalize_distress_topic(distress_type)
    normalized_processor = (processor or "unknown").strip().lower()
    normalized_industry = (industry or "unknown").strip().lower()

    if normalized_processor != "unknown" and normalized_distress in {"account_frozen", "payouts_delayed", "reserve_hold"}:
        return "Under PayFlux doctrine, processor-linked liquidity threats come first."
    if normalized_distress == "processor_switch_intent":
        return "Under PayFlux doctrine, merchants already looking for alternatives are high-value signals."
    if normalized_industry == "ecommerce" and normalized_distress in {
        "account_frozen",
        "account_terminated",
        "verification_review",
        "reserve_hold",
        "payouts_delayed",
    }:
        return "Under PayFlux doctrine, ecommerce merchants with frozen or restricted accounts are top targets."
    if normalized_processor != "unknown" and normalized_distress != "unknown":
        return "Under PayFlux doctrine, processor-linked merchant distress outranks general support noise."
    return "Under PayFlux doctrine, only clear merchant payment distress should pull operator attention."
