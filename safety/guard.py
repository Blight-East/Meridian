import os
import redis
from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.safety.control_plane import record_security_incident

logger = get_logger("safety")

ALLOWED_OPERATORS = set(
    os.environ.get("ALLOWED_OPERATOR_IDS", "").split(",")
)
ALLOWED_OPERATOR_CHATS = set(
    chat_id.strip() for chat_id in os.environ.get("ALLOWED_OPERATOR_CHAT_IDS", "").split(",") if chat_id.strip()
)
REQUIRE_PRIVATE_OPERATOR_CHAT = os.environ.get("OPERATOR_REQUIRE_PRIVATE_CHAT", "true").strip().lower() not in {"0", "false", "no"}
_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)

BLOCKED_ACTIONS = {"rm", "drop_table", "delete_all", "shutdown"}


def validate_operator(user_id):
    uid = str(user_id)
    if ALLOWED_OPERATORS and "" not in ALLOWED_OPERATORS and uid not in ALLOWED_OPERATORS:
        raise PermissionError(f"Operator {uid} not authorized")
    return True


def _trusted_chat_key(user_id):
    return f"agent_flux:trusted_chat_id:{str(user_id)}"


def _trusted_chat_owner_key(chat_id):
    return f"agent_flux:trusted_chat_owner:{str(chat_id)}"


def get_trusted_operator_chat(user_id):
    chat_id = _redis.get(_trusted_chat_key(user_id))
    return str(chat_id) if chat_id else None


def resolve_operator_delivery_chat(base_target):
    target = str(base_target or "").strip()
    if not target:
        return None

    trusted_for_user = _redis.get(_trusted_chat_key(target))
    if trusted_for_user:
        return str(trusted_for_user)

    trusted_owner = _redis.get(_trusted_chat_owner_key(target))
    if trusted_owner:
        return target

    session_chat = _redis.get(f"agent_flux:chat_id:{target}")
    if session_chat:
        return str(session_chat)

    if target in ALLOWED_OPERATOR_CHATS:
        return target

    return target


def _record_operator_channel_incident(*, user_id, chat_id, chat_type, username="", reason, severity="high", title="Unauthorized operator channel"):
    metadata = {
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "chat_type": chat_type or "",
        "username": username or "",
        "reason": reason,
    }
    try:
        save_event("operator_channel_rejected", metadata)
    except Exception:
        logger.exception("Failed to save operator channel rejection event")
    try:
        record_security_incident(
            incident_type="unauthorized_operator_channel",
            source_channel="telegram",
            source_id=str(chat_id),
            thread_id=str(user_id),
            severity=severity,
            title=title,
            summary=f"Rejected Telegram operator access attempt: {reason}.",
            markers=["operator_channel_rejected"],
            metadata=metadata,
        )
    except Exception:
        logger.exception("Failed to record operator channel security incident")


def validate_operator_context(user_id, *, chat_id=None, chat_type="", username="", bind_on_first_use=True):
    uid = str(user_id)
    cid = str(chat_id or "").strip()
    ctype = str(chat_type or "").strip().lower()

    validate_operator(uid)

    if not cid:
        raise PermissionError("Missing operator chat id")

    if REQUIRE_PRIVATE_OPERATOR_CHAT and ctype and ctype != "private":
        _record_operator_channel_incident(
            user_id=uid,
            chat_id=cid,
            chat_type=ctype,
            username=username,
            reason="non_private_chat",
            severity="critical",
            title="Rejected non-private operator chat",
        )
        raise PermissionError("Operator commands require a private Telegram chat")

    if ALLOWED_OPERATOR_CHATS and cid not in ALLOWED_OPERATOR_CHATS:
        _record_operator_channel_incident(
            user_id=uid,
            chat_id=cid,
            chat_type=ctype,
            username=username,
            reason="chat_not_in_allowlist",
        )
        raise PermissionError("Operator chat is not allowlisted")

    trusted_chat_id = get_trusted_operator_chat(uid)
    if trusted_chat_id:
        if trusted_chat_id != cid:
            _record_operator_channel_incident(
                user_id=uid,
                chat_id=cid,
                chat_type=ctype,
                username=username,
                reason=f"trusted_chat_mismatch:{trusted_chat_id}",
                severity="critical",
                title="Rejected untrusted operator chat",
            )
            raise PermissionError("This chat is not the bound operator control channel")
        _redis.set(_trusted_chat_owner_key(cid), uid)
        return {"authorized": True, "newly_bound": False, "trusted_chat_id": trusted_chat_id}

    if not bind_on_first_use:
        raise PermissionError("Operator chat is not yet bound")

    _redis.set(_trusted_chat_key(uid), cid)
    _redis.set(_trusted_chat_owner_key(cid), uid)
    try:
        save_event(
            "operator_chat_bound",
            {
                "user_id": uid,
                "chat_id": cid,
                "chat_type": ctype,
                "username": username or "",
            },
        )
    except Exception:
        logger.exception("Failed to save operator chat binding event")

    return {"authorized": True, "newly_bound": True, "trusted_chat_id": cid}


def safe_execute(action, input_data):
    if action in BLOCKED_ACTIONS:
        raise ValueError(f"Action '{action}' is blocked by safety policy")
    if isinstance(input_data, str) and any(cmd in input_data.lower() for cmd in ["rm -rf", "drop table", "delete from"]):
        raise ValueError(f"Dangerous pattern detected in input for action '{action}'")
    return True
