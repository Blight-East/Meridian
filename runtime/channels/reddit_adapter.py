from __future__ import annotations

import os
from urllib.parse import quote

import requests

from runtime.channels.base import ActionResult, ChannelAdapter, ChannelTarget
from runtime.channels.store import count_recent_results
from runtime.channels.templates import build_reddit_draft, confidence_score, detect_hostility, normalized_text


REDDIT_OAUTH_BASE = "https://oauth.reddit.com"


class RedditAdapter(ChannelAdapter):
    channel_name = "reddit"

    def __init__(self):
        self.client_id = os.getenv("REDDIT_CLIENT_ID", "")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET", "")
        self.refresh_token = os.getenv("REDDIT_REFRESH_TOKEN", "")
        self.user_agent = os.getenv("REDDIT_USER_AGENT", "agent-flux/phase1")
        self.username = os.getenv("REDDIT_ACCOUNT_USERNAME", "")
        self.default_queries = [
            "stripe froze my account",
            "looking for a payment processor",
            "alternative to stripe",
            "alternative to paypal",
        ]

    def _token(self) -> str:
        if not self.client_id or not self.client_secret or not self.refresh_token:
            raise RuntimeError("Reddit OAuth env is incomplete")
        response = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(self.client_id, self.client_secret),
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token},
            headers={"User-Agent": self.user_agent},
            timeout=20,
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def _request(self, method: str, path: str, **kwargs):
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._token()}"
        headers["User-Agent"] = self.user_agent
        response = requests.request(method, f"{REDDIT_OAUTH_BASE}{path}", headers=headers, timeout=25, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else {}

    def list_targets(self, query: str = "", subreddits: list[str] | None = None, limit: int = 10, **kwargs) -> list[ChannelTarget]:
        targets = []
        queries = [query] if query else self.default_queries
        subreddits = subreddits or [item.strip() for item in os.getenv("REDDIT_ALLOWLIST", "").split(",") if item.strip()]
        if not subreddits:
            subreddits = ["stripe", "shopify", "smallbusiness", "entrepreneur"]
        seen = set()
        for subreddit in subreddits:
            for search_query in queries:
                params = {
                    "q": search_query,
                    "restrict_sr": "on",
                    "sort": "new",
                    "limit": limit,
                    "raw_json": 1,
                }
                result = self._request("GET", f"/r/{quote(subreddit)}/search", params=params)
                children = result.get("data", {}).get("children", [])
                for child in children:
                    data = child.get("data", {})
                    fullname = data.get("name")
                    if not fullname or fullname in seen:
                        continue
                    seen.add(fullname)
                    title = data.get("title", "")
                    body = data.get("selftext", "")
                    combined = normalized_text(title, body)
                    targets.append(
                        ChannelTarget(
                            channel="reddit",
                            target_id=fullname,
                            thread_id=fullname,
                            title=title,
                            body=body,
                            author=data.get("author", ""),
                            account_identity=self.username,
                            metadata={
                                "subreddit": data.get("subreddit", ""),
                                "permalink": data.get("permalink", ""),
                                "comments": data.get("num_comments", 0),
                                "score": data.get("score", 0),
                                "hostile": detect_hostility(combined),
                                "confidence_hint": confidence_score(combined, merchant_fit=0.16),
                            },
                        )
                    )
        return targets

    def read_context(self, target_id: str, permalink: str = "", **kwargs) -> ChannelTarget:
        if permalink:
            path = permalink if permalink.startswith("/") else f"/{permalink.lstrip('/')}"
        else:
            info = self._request("GET", "/api/info", params={"id": target_id, "raw_json": 1})
            child = (info.get("data", {}).get("children") or [{}])[0].get("data", {})
            path = child.get("permalink", "")
        if not path:
            raise RuntimeError(f"Could not resolve Reddit target {target_id}")
        listing = self._request("GET", path, params={"raw_json": 1, "depth": 2, "limit": 8})
        post = (listing[0].get("data", {}).get("children") or [{}])[0].get("data", {})
        comments = (listing[1].get("data", {}).get("children") or []) if len(listing) > 1 else []
        comment_bodies = []
        for item in comments[:5]:
            data = item.get("data", {})
            if data.get("body"):
                comment_bodies.append(data["body"])
        combined = normalized_text(post.get("title", ""), post.get("selftext", ""), " ".join(comment_bodies))
        return ChannelTarget(
            channel="reddit",
            target_id=target_id or post.get("name", ""),
            thread_id=post.get("name", target_id),
            title=post.get("title", ""),
            body=post.get("selftext", ""),
            author=post.get("author", ""),
            account_identity=self.username,
            metadata={
                "subreddit": post.get("subreddit", ""),
                "permalink": post.get("permalink", path),
                "comments_preview": comment_bodies,
                "hostile": detect_hostility(combined),
                "confidence_hint": confidence_score(combined, merchant_fit=0.2),
            },
        )

    def draft_action(self, target: ChannelTarget, **kwargs):
        return build_reddit_draft(target)

    def send_action(self, draft, **kwargs) -> ActionResult:
        result = self._request(
            "POST",
            "/api/comment",
            data={"api_type": "json", "thing_id": draft.target_id, "text": draft.text},
        )
        errors = ((result.get("json") or {}).get("errors") or [])
        if errors:
            return ActionResult(False, "error", draft.target_id, draft.thread_id, error=str(errors), payload=result)
        things = ((result.get("json") or {}).get("data") or {}).get("things") or []
        remote_id = ""
        if things:
            remote_id = things[0].get("data", {}).get("name", "")
        return ActionResult(True, "sent", draft.target_id, draft.thread_id, remote_id, result)

    def modify_own_action(self, target_id: str, content: str, **kwargs) -> ActionResult:
        result = self._request(
            "POST",
            "/api/editusertext",
            data={"api_type": "json", "thing_id": target_id, "text": content},
        )
        errors = ((result.get("json") or {}).get("errors") or [])
        if errors:
            return ActionResult(False, "error", target_id, error=str(errors), payload=result)
        return ActionResult(True, "modified", target_id, payload=result)

    def get_rate_limit_state(self) -> dict:
        return {
            "replies_last_hour": count_recent_results("reddit", "sent", hours=1),
            "mode": os.getenv("REDDIT_CHANNEL_MODE", "approval_required"),
        }
