from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ChannelTarget:
    channel: str
    target_id: str
    thread_id: str
    title: str = ""
    body: str = ""
    author: str = ""
    account_identity: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DraftAction:
    channel: str
    action_type: str
    target_id: str
    thread_id: str
    subject: str = ""
    text: str = ""
    rationale: str = ""
    confidence: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ActionResult:
    ok: bool
    status: str
    target_id: str
    thread_id: str = ""
    remote_id: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    error: str = ""


class ChannelAdapter(ABC):
    channel_name: str

    @abstractmethod
    def list_targets(self, **kwargs) -> list[ChannelTarget]:
        raise NotImplementedError

    @abstractmethod
    def read_context(self, target_id: str, **kwargs) -> ChannelTarget:
        raise NotImplementedError

    @abstractmethod
    def draft_action(self, target: ChannelTarget, **kwargs) -> DraftAction:
        raise NotImplementedError

    @abstractmethod
    def send_action(self, draft: DraftAction, **kwargs) -> ActionResult:
        raise NotImplementedError

    @abstractmethod
    def modify_own_action(self, target_id: str, content: str, **kwargs) -> ActionResult:
        raise NotImplementedError

    @abstractmethod
    def get_rate_limit_state(self) -> dict[str, Any]:
        raise NotImplementedError

