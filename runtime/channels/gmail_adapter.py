from __future__ import annotations

import base64
import json
import os
from email.mime.text import MIMEText
from pathlib import Path

import requests
from dotenv import dotenv_values

from runtime.channels.base import ActionResult, ChannelAdapter, ChannelTarget
from runtime.channels.store import count_recent_results
from runtime.channels.templates import build_gmail_reply, confidence_score, detect_hostility, normalized_text


GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"
DRIVE_API_BASE = "https://www.googleapis.com/upload/drive/v3/files"
ENV_FILE_PATH = Path(__file__).resolve().parents[2] / ".env"
ENV_FILE_VALUES = dotenv_values(ENV_FILE_PATH)


def _env_value(*keys: str, default: str = "") -> str:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    for key in keys:
        value = ENV_FILE_VALUES.get(key)
        if value:
            return value
    return default


def _decode_body(data: str) -> str:
    if not data:
        return ""
    padded = data + "=" * ((4 - len(data) % 4) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8", errors="ignore")


def _payload_body(payload: dict) -> str:
    if not payload:
        return ""
    body = payload.get("body", {}) or {}
    if body.get("data"):
        return _decode_body(body["data"])
    for part in payload.get("parts", []) or []:
        text = _payload_body(part)
        if text:
            return text
    return ""


def _headers_map(payload: dict) -> dict:
    headers = {}
    for item in payload.get("headers", []) or []:
        name = item.get("name")
        if name:
            headers[name.lower()] = item.get("value", "")
    return headers


class GmailAdapter(ChannelAdapter):
    channel_name = "gmail"

    def __init__(self):
        self.client_id = _env_value("GOOGLE_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_ID")
        self.client_secret = _env_value("GOOGLE_CLIENT_SECRET", "GOOGLE_OAUTH_CLIENT_SECRET")
        self.refresh_token = _env_value("GOOGLE_REFRESH_TOKEN", "GOOGLE_OAUTH_REFRESH_TOKEN")
        self.sender_email = _env_value("GMAIL_SENDER_EMAIL")
        self.sender_name = _env_value("GMAIL_SENDER_NAME", default="Agent Flux")
        self.drive_folder_id = _env_value("GOOGLE_DRIVE_AUDIT_FOLDER_ID")

    def _token(self) -> str:
        if not self.client_id or not self.client_secret or not self.refresh_token:
            raise RuntimeError("Google OAuth env is incomplete")
        response = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        return payload["access_token"]

    def _request(self, method: str, path: str, **kwargs):
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._token()}"
        response = requests.request(method, f"{GMAIL_API_BASE}{path}", headers=headers, timeout=25, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    def _label_lookup(self) -> dict[str, str]:
        labels = self._request("GET", "/labels").get("labels", [])
        return {label["id"]: label["name"] for label in labels}

    def ensure_default_labels(self) -> dict:
        desired = {
            "bot-draft": {"labelListVisibility": "labelShow", "messageListVisibility": "show"},
            "bot-sent": {"labelListVisibility": "labelShow", "messageListVisibility": "show"},
            "bot-needs-review": {"labelListVisibility": "labelShow", "messageListVisibility": "show"},
            "bot-replied": {"labelListVisibility": "labelShow", "messageListVisibility": "show"},
            "bot-do-not-contact": {"labelListVisibility": "labelHide", "messageListVisibility": "hide"},
        }
        existing = self._label_lookup()
        created = []
        for name, visibility in desired.items():
            if name in existing.values():
                continue
            payload = {
                "name": name,
                "labelListVisibility": visibility["labelListVisibility"],
                "messageListVisibility": visibility["messageListVisibility"],
            }
            self._request("POST", "/labels", json=payload)
            created.append(name)
        return {"created_labels": created}

    def list_targets(self, query: str = "", label_ids: list[str] | None = None, limit: int = 10, **kwargs) -> list[ChannelTarget]:
        params = {"maxResults": limit}
        if query:
            params["q"] = query
        if label_ids:
            params["labelIds"] = label_ids
        threads = self._request("GET", "/threads", params=params).get("threads", [])
        targets = []
        label_lookup = self._label_lookup()
        for thread in threads:
            target = self.read_context(thread["id"], label_lookup=label_lookup)
            targets.append(target)
        return targets

    def read_context(self, target_id: str, label_lookup: dict[str, str] | None = None, **kwargs) -> ChannelTarget:
        thread = self._request("GET", f"/threads/{target_id}", params={"format": "full"})
        label_lookup = label_lookup or self._label_lookup()
        messages = thread.get("messages", [])
        last_message = messages[-1] if messages else {}
        payload = last_message.get("payload", {}) or {}
        headers = _headers_map(payload)
        text_body = _payload_body(payload)
        from_value = headers.get("from", "")
        sender_email = from_value.split("<")[-1].rstrip(">") if "@" in from_value else from_value
        sender_domain = sender_email.split("@", 1)[1].lower() if "@" in sender_email else ""
        labels = [label_lookup.get(label_id, label_id) for label_id in last_message.get("labelIds", [])]
        snippet = thread.get("snippet", "")
        title = headers.get("subject", snippet[:120])
        body = text_body or snippet
        body_normalized = normalized_text(title, body)
        metadata = {
            "labels": labels,
            "sender_email": sender_email,
            "sender_name": from_value.split("<")[0].strip().strip('"') if "<" in from_value else sender_email,
            "reply_to": headers.get("reply-to", sender_email),
            "reply_subject": headers.get("subject", ""),
            "message_id": headers.get("message-id", ""),
            "references": headers.get("references", ""),
            "in_reply_to": headers.get("in-reply-to", ""),
            "snippet": snippet,
            "sender_domain": sender_domain,
            "last_message_id": last_message.get("id", ""),
            "last_message_internal_date": last_message.get("internalDate", ""),
            "hostile": detect_hostility(body_normalized),
            "do_not_contact": "bot-do-not-contact" in labels,
            "confidence_hint": confidence_score(body_normalized, merchant_fit=0.22),
        }
        return ChannelTarget(
            channel="gmail",
            target_id=target_id,
            thread_id=thread["id"],
            title=title,
            body=body,
            author=sender_email,
            account_identity=self.sender_email,
            metadata=metadata,
        )

    def draft_action(self, target: ChannelTarget, **kwargs):
        return build_gmail_reply(target)

    def _apply_labels(self, thread_id: str, add_labels: list[str]):
        label_lookup = self._label_lookup()
        reverse_lookup = {name: label_id for label_id, name in label_lookup.items()}
        ids = []
        for label in add_labels:
            label_id = reverse_lookup.get(label)
            if label_id:
                ids.append(label_id)
        if ids:
            self._request("POST", f"/threads/{thread_id}/modify", json={"addLabelIds": ids})

    def send_action(self, draft, **kwargs) -> ActionResult:
        recipient = draft.metadata.get("recipient") or kwargs.get("recipient")
        if not recipient:
            return ActionResult(False, "error", draft.target_id, draft.thread_id, error="Missing recipient")
        # Optional flag-gated pre-send transforms (funnel tracking + dry-run).
        try:
            from runtime.ops import conversion_upgrade as _upgrade  # lazy import to avoid cycles
        except Exception:
            _upgrade = None
        text_body = draft.text
        if _upgrade is not None:
            text_body = _upgrade.augment_body_with_tracking(text_body)
            if _upgrade.is_dry_run_enabled():
                _upgrade.log_upgrade(
                    "gmail_adapter_dry_run",
                    recipient=recipient,
                    subject=(draft.subject or "")[:120],
                    thread_id=draft.thread_id,
                )
                # ok=False is intentional.  The channel service treats
                # ok-true results as `envelope_status="sent"` and writes
                # journal entries — neither should happen for a dry run.
                return ActionResult(
                    False,
                    "dry_run",
                    draft.target_id,
                    draft.thread_id,
                    "",
                    {"dry_run": True},
                    error="MERIDIAN_UPGRADE_DRY_RUN enabled; send skipped",
                )
        message = MIMEText(text_body)
        message["to"] = recipient
        message["from"] = f"{self.sender_name} <{self.sender_email}>" if self.sender_email else self.sender_name
        message["subject"] = draft.subject
        if draft.metadata.get("message_id"):
            message["In-Reply-To"] = draft.metadata.get("message_id")
            references = draft.metadata.get("references") or draft.metadata.get("message_id")
            message["References"] = references
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        payload = {"raw": raw, "threadId": draft.thread_id}
        result = self._request("POST", "/messages/send", json=payload)
        self._apply_labels(draft.thread_id, ["bot-sent", "bot-replied"])
        if _upgrade is not None:
            try:
                _upgrade.record_funnel_event(
                    "sent",
                    channel="gmail",
                    thread_id=draft.thread_id,
                    message_id=result.get("id", ""),
                    recipient=recipient,
                    metadata={"subject": (draft.subject or "")[:160], "via": "adapter"},
                )
                if _upgrade.detect_bounce_from_gmail_result(result):
                    _upgrade.record_funnel_event(
                        "bounce",
                        channel="gmail",
                        thread_id=draft.thread_id,
                        message_id=result.get("id", ""),
                        recipient=recipient,
                        metadata={"source": "gmail_send_response"},
                    )
            except Exception:
                pass
        return ActionResult(True, "sent", draft.target_id, draft.thread_id, result.get("id", ""), result)

    def modify_own_action(self, target_id: str, content: str, **kwargs) -> ActionResult:
        draft_id = kwargs.get("draft_id")
        recipient = kwargs.get("recipient", self.sender_email)
        subject = kwargs.get("subject", "Updated draft")
        if not draft_id:
            return ActionResult(False, "error", target_id, error="draft_id is required")
        message = MIMEText(content)
        message["to"] = recipient
        message["subject"] = subject
        message["from"] = f"{self.sender_name} <{self.sender_email}>" if self.sender_email else self.sender_name
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
        payload = {"id": draft_id, "message": {"raw": raw}}
        result = self._request("PUT", f"/drafts/{draft_id}", json=payload)
        return ActionResult(True, "modified", target_id, payload=result, remote_id=result.get("id", ""))

    def upload_artifact(self, path: str, mime_type: str = "application/octet-stream", name: str = "") -> dict:
        headers = {"Authorization": f"Bearer {self._token()}"}
        metadata = {"name": name or os.path.basename(path)}
        if self.drive_folder_id:
            metadata["parents"] = [self.drive_folder_id]
        with open(path, "rb") as handle:
            files = {
                "metadata": ("metadata", json.dumps(metadata), "application/json"),
                "file": (metadata["name"], handle, mime_type),
            }
            response = requests.post(
                f"{DRIVE_API_BASE}?uploadType=multipart",
                headers=headers,
                files=files,
                timeout=30,
            )
        response.raise_for_status()
        return response.json()

    def get_rate_limit_state(self) -> dict:
        return {
            "daily_sends_last_24h": count_recent_results("gmail", "sent", hours=24),
            "mode": _env_value("GMAIL_CHANNEL_MODE", default="approval_required"),
        }
