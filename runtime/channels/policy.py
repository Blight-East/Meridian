from __future__ import annotations

import os
from dataclasses import dataclass

from runtime.channels.base import DraftAction, ChannelTarget
from runtime.channels.store import (
    compute_idempotency_key,
    count_recent_channel_target_results,
    count_recent_results,
    count_recent_subreddit_sends,
    get_channel_settings,
    has_journal_entry,
    has_recent_success,
)


@dataclass
class PolicyDecision:
    status: str
    reason: str
    approval_state: str
    mode: str
    idempotency_key: str = ""


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _list_env(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def evaluate_send_policy(
    *,
    channel: str,
    target: ChannelTarget,
    draft: DraftAction,
    human_approved: bool = False,
) -> PolicyDecision:
    settings = get_channel_settings(channel)
    mode = settings.get("mode", "approval_required")
    if not _bool_env("CHANNEL_ACTION_LAYER_ENABLED", True):
        return PolicyDecision("blocked", "global_kill_switch", "blocked", mode)
    if not _bool_env(f"{channel.upper()}_CHANNEL_ENABLED", True):
        return PolicyDecision("blocked", "channel_env_disabled", "blocked", mode)
    if settings.get("kill_switch"):
        return PolicyDecision("blocked", "channel_kill_switch", "blocked", mode)

    body_text = draft.text or ""
    idempotency_key = compute_idempotency_key(channel, draft.action_type, target.target_id, body_text)
    cooldown_hours = _int_env(f"{channel.upper()}_CHANNEL_COOLDOWN_HOURS", 72)
    if has_recent_success(channel, idempotency_key, cooldown_hours=cooldown_hours):
        return PolicyDecision("skipped_duplicate", "duplicate_attempt", "skipped_duplicate", mode, idempotency_key)

    min_confidence = _float_env(f"{channel.upper()}_CHANNEL_MIN_CONFIDENCE", 0.82 if channel == "reddit" else 0.72)
    if draft.confidence < min_confidence and not human_approved:
        return PolicyDecision("blocked", "confidence_below_threshold", "blocked", mode, idempotency_key)

    security_markers = target.metadata.get("security_markers") or draft.metadata.get("security_markers") or []
    if security_markers and not human_approved:
        return PolicyDecision("blocked", "untrusted_instruction_detected", "blocked", mode, idempotency_key)

    if channel == "reddit":
        subreddit = str(target.metadata.get("subreddit", "")).lower()
        allowlist = [item.lower() for item in settings.get("allowlist", [])] or _list_env("REDDIT_ALLOWLIST")
        denylist = [item.lower() for item in settings.get("denylist", [])] or _list_env("REDDIT_DENYLIST")
        if allowlist and subreddit not in allowlist:
            return PolicyDecision("blocked", "subreddit_not_allowlisted", "blocked", mode, idempotency_key)
        if subreddit and subreddit in denylist:
            return PolicyDecision("blocked", "subreddit_denied", "blocked", mode, idempotency_key)
        if target.metadata.get("hostile") or draft.metadata.get("hostile"):
            return PolicyDecision("blocked", "hostile_or_sensitive_thread", "blocked", mode, idempotency_key)
        max_hour = _int_env("REDDIT_MAX_REPLIES_PER_HOUR", 3)
        if count_recent_results("reddit", "sent", hours=1) >= max_hour:
            return PolicyDecision("blocked", "reddit_hourly_rate_limit", "blocked", mode, idempotency_key)
        max_subreddit_day = _int_env("REDDIT_MAX_REPLIES_PER_SUBREDDIT_PER_DAY", 5)
        if subreddit and count_recent_subreddit_sends(subreddit, hours=24) >= max_subreddit_day:
            return PolicyDecision("blocked", "reddit_subreddit_daily_limit", "blocked", mode, idempotency_key)
        if has_journal_entry("reddit", target.thread_id, "proactive_reply") and not human_approved:
            return PolicyDecision("blocked", "thread_already_contacted", "blocked", mode, idempotency_key)

    if channel == "gmail":
        recipient = str(draft.metadata.get("recipient", "")).lower()
        if draft.metadata.get("thread_category") == "noise_system" or draft.metadata.get("test_thread"):
            return PolicyDecision("blocked", "gmail_noise_or_test_thread", "blocked", mode, idempotency_key)
        automated_tokens = ("no-reply", "noreply", "do-not-reply", "mailer-daemon", "postmaster")
        if any(token in recipient for token in automated_tokens):
            return PolicyDecision("blocked", "automated_recipient", "blocked", mode, idempotency_key)
        if draft.metadata.get("do_not_contact"):
            return PolicyDecision("blocked", "do_not_contact", "blocked", mode, idempotency_key)
        gmail_denylist = [item.lower() for item in settings.get("denylist", [])] or _list_env("GMAIL_DENYLIST")
        if recipient and recipient in gmail_denylist:
            return PolicyDecision("blocked", "recipient_denied", "blocked", mode, idempotency_key)
        max_day = _int_env("GMAIL_MAX_SENDS_PER_DAY", 20)
        if count_recent_results("gmail", "sent", hours=24) >= max_day:
            return PolicyDecision("blocked", "gmail_daily_send_limit", "blocked", mode, idempotency_key)
        if count_recent_channel_target_results("gmail", "sent", "thread_id", target.thread_id, hours=cooldown_hours) > 0:
            return PolicyDecision("blocked", "gmail_thread_cooldown", "blocked", mode, idempotency_key)

    if mode == "dry_run":
        return PolicyDecision("blocked", "channel_in_dry_run", "dry_run", mode, idempotency_key)

    if mode == "approval_required" and not human_approved:
        return PolicyDecision("requires_approval", "human_approval_required", "approval_required", mode, idempotency_key)

    if mode == "auto_send_high_confidence":
        auto_threshold = _float_env(f"{channel.upper()}_CHANNEL_AUTO_SEND_CONFIDENCE", min_confidence + 0.1)
        if draft.confidence < auto_threshold and not human_approved:
            return PolicyDecision("requires_approval", "auto_send_threshold_not_met", "approval_required", mode, idempotency_key)

    return PolicyDecision("allowed", "policy_pass", "approved", mode, idempotency_key)
