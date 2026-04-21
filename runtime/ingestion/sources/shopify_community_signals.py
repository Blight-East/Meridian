"""
Shopify Community public-web scanner.
Fetches category HTML plus thread detail pages without API calls.
"""
import json
import os
import random
import re
import sys
import time
from collections import Counter

import redis
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from config.logging_config import get_logger
from memory.structured.db import save_event
from runtime.intelligence.public_web_source_utils import (
    absolute_url,
    build_session,
    classify_source_candidate,
    extract_hrefs,
    fetch_html,
    format_signal_content,
    normalize_whitespace,
    parse_created_at,
    select_best_merchant_domain,
    should_emit_signal,
    soup_text,
    summarize_top_distress,
)

logger = get_logger("shopify_community_signals")

SOURCE_NAME = "shopify_community"
SOURCE_LABEL = "Shopify Community"
BASE_URL = "https://community.shopify.com"
BOARD_URLS = {
    "Payments + Shipping & Fulfilment": f"{BASE_URL}/c/payments-shipping-fulfilment/217",
    "Shopify Discussion": f"{BASE_URL}/c/shopify-discussion/95",
    "Technical Q&A": f"{BASE_URL}/c/technical-qa/211",
}
TITLE_DISTRESS_PATTERNS = (
    re.compile(r"\bpayouts?\s+(?:on hold|hold|paused|stopped|delayed|delay|late)\b", re.IGNORECASE),
    re.compile(r"\bfunds?\s+frozen\b", re.IGNORECASE),
    re.compile(r"\bpayout\s+review\b", re.IGNORECASE),
    re.compile(r"\b(?:rolling\s+reserve|reserve\s+(?:hold|increase|requirement)|funds?\s+held)\b", re.IGNORECASE),
    re.compile(r"\baccount\s+review\b", re.IGNORECASE),
    re.compile(r"\bunder review\b", re.IGNORECASE),
    re.compile(r"\bstandard review\b", re.IGNORECASE),
    re.compile(r"\bverification required\b", re.IGNORECASE),
    re.compile(r"\bcompliance\b", re.IGNORECASE),
    re.compile(r"\bpayments?\s+disabled\b", re.IGNORECASE),
    re.compile(r"\bshopify payments disabled\b", re.IGNORECASE),
    re.compile(r"\bpayment gateway error\b", re.IGNORECASE),
    re.compile(r"\bpayment provider not enabled\b", re.IGNORECASE),
    re.compile(r"\bpayment failed\b", re.IGNORECASE),
)
MAX_THREADS_PER_BOARD = 8
MAX_THREAD_FETCHES = 10
MAX_REPLY_SIGNALS = 1
BOARD_FETCH_COOLDOWN_SECONDS = 600
BOARD_RATE_LIMIT_BACKOFF_SECONDS = 1800
THREAD_FETCH_COOLDOWN_SECONDS = 900
THREAD_REFRESH_WINDOW_SECONDS = 6 * 3600
REQUEST_JITTER_RANGE_SECONDS = (0.25, 0.9)

try:
    _redis = redis.Redis(host="localhost", port=6379, decode_responses=True)
except Exception:
    _redis = None


def _matches_distress_title(title: str) -> bool:
    return any(pattern.search(title or "") for pattern in TITLE_DISTRESS_PATTERNS)


def _parse_board_threads(board_name: str, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    threads: list[dict] = []
    seen: set[str] = set()
    for row in soup.select("tr.topic-list-item"):
        link = row.select_one("a.title")
        title = soup_text(link)
        thread_url = absolute_url(BASE_URL, link.get("href") if link else "")
        if not title or not thread_url or thread_url in seen:
            continue
        seen.add(thread_url)
        if title.lower().startswith("about the "):
            continue
        if not _matches_distress_title(title):
            continue
        replies_text = soup_text(row.select_one("td.replies"))
        reply_count = 0
        if replies_text.isdigit():
            reply_count = int(replies_text)
        last_activity_label = soup_text(row.select_one("td:last-child"))
        threads.append(
            {
                "source_category": board_name,
                "thread_or_question_url": thread_url,
                "title": title,
                "reply_count": reply_count,
                "last_activity_label": last_activity_label,
                "thread_fingerprint": _thread_fingerprint(title, reply_count, last_activity_label),
            }
        )
        if len(threads) >= MAX_THREADS_PER_BOARD:
            break
    return threads


def _parse_thread_detail(candidate: dict, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    posts = soup.select("div.topic-body.crawler-post")
    if not posts:
        return []

    first_post = posts[0]
    first_text = soup_text(first_post.select_one("div.post"))
    first_hrefs = extract_hrefs(first_post, base_url=BASE_URL)
    merchant_domain, ranked_domains = select_best_merchant_domain(first_text, first_hrefs)
    created_at = parse_created_at(first_post)
    author_handle = soup_text(first_post.select_one(".creator [itemprop='name']")) or "unknown"
    reply_count = max(0, len(posts) - 1)
    classified = classify_source_candidate(
        title=candidate["title"],
        body_text=first_text,
        merchant_domain=merchant_domain,
    )
    parent_candidate = {
        **candidate,
        **classified,
        "source_name": SOURCE_NAME,
        "source_label": SOURCE_LABEL,
        "body_text": first_text,
        "author_handle": author_handle,
        "created_at": created_at,
        "reply_count": reply_count,
        "merchant_domain": merchant_domain,
        "merchant_attribution_state": "domain_resolved" if merchant_domain else "domainless",
        "domain_candidates": ranked_domains,
        "evidence_text": _reply_evidence(posts[1:]),
    }
    emitted: list[dict] = [parent_candidate]
    separate_reply_count = 0
    for reply in posts[1:]:
        if separate_reply_count >= MAX_REPLY_SIGNALS:
            break
        reply_text = soup_text(reply.select_one("div.post"))
        if not reply_text:
            continue
        reply_author = soup_text(reply.select_one(".creator [itemprop='name']")) or "unknown"
        reply_hrefs = extract_hrefs(reply, base_url=BASE_URL)
        reply_domain, reply_ranked_domains = select_best_merchant_domain(reply_text, reply_hrefs)
        if not reply_domain or reply_domain == merchant_domain:
            continue
        reply_classified = classify_source_candidate(
            title=candidate["title"],
            body_text=reply_text,
            merchant_domain=reply_domain,
        )
        if reply_classified.get("merchant_signal_classification") != "merchant_operator":
            continue
        ok, _ = should_emit_signal(
            {
                **reply_classified,
                "merchant_domain": reply_domain,
                "title": candidate["title"],
                "body_text": reply_text,
            },
            allow_without_domain=False,
        )
        if not ok:
            continue
        emitted.append(
            {
                **candidate,
                **reply_classified,
                "source_name": SOURCE_NAME,
                "source_label": SOURCE_LABEL,
                "body_text": reply_text,
                "author_handle": reply_author,
                "created_at": parse_created_at(reply),
                "reply_count": 0,
                "merchant_domain": reply_domain,
                "merchant_attribution_state": "domain_resolved",
                "domain_candidates": reply_ranked_domains,
                "evidence_text": "Separate merchant reply on the same thread.",
            }
        )
        separate_reply_count += 1
    return emitted


def _reply_evidence(posts) -> str:
    snippets: list[str] = []
    for reply in posts[:2]:
        text = soup_text(reply.select_one("div.post"))
        if not text:
            continue
        if "same issue" in text.lower() or "same here" in text.lower():
            snippets.append(f"Merchant reply evidence: {text[:220]}")
        else:
            snippets.append(text[:220])
    return " | ".join(snippets)


def _thread_fingerprint(title: str, reply_count: int, last_activity_label: str) -> str:
    return f"{normalize_whitespace(title)}|{int(reply_count or 0)}|{normalize_whitespace(last_activity_label)}"


def _redis_key(prefix: str, value: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9:_-]+", "_", value or "")
    return f"shopify_community:{prefix}:{safe}"


def _sleep_with_jitter():
    time.sleep(random.uniform(*REQUEST_JITTER_RANGE_SECONDS))


def _get_cached_json(key: str) -> dict:
    if not _redis:
        return {}
    raw = _redis.get(key)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _set_cached_json(key: str, payload: dict, ttl_seconds: int) -> None:
    if not _redis:
        return
    try:
        _redis.setex(key, ttl_seconds, json.dumps(payload))
    except Exception:
        return


def _ttl_seconds(key: str) -> int:
    if not _redis:
        return 0
    try:
        ttl = int(_redis.ttl(key))
    except Exception:
        return 0
    return ttl if ttl > 0 else 0


def _record_board_event(board_name: str, *, status: str, **kwargs) -> None:
    save_event(
        "shopify_community_board_cycle",
        {
            "board": board_name,
            "status": status,
            **kwargs,
        },
    )


def _record_duplicate_skip(board_name: str, thread: dict, *, reason: str, ttl_seconds: int = 0) -> None:
    save_event(
        "shopify_community_duplicate_skipped",
        {
            "board": board_name,
            "url": thread.get("thread_or_question_url"),
            "title": thread.get("title"),
            "reply_count": int(thread.get("reply_count") or 0),
            "last_activity_label": thread.get("last_activity_label") or "",
            "reason": reason,
            "cooldown_remaining_seconds": int(ttl_seconds or 0),
        },
    )


def _record_rate_limit(board_name: str, url: str, *, scope: str) -> None:
    save_event(
        "shopify_community_rate_limited",
        {
            "board": board_name,
            "url": url,
            "scope": scope,
            "backoff_seconds": BOARD_RATE_LIMIT_BACKOFF_SECONDS,
        },
    )


def _board_cooldown_key(board_name: str) -> str:
    return _redis_key("board_cooldown", board_name)


def _board_backoff_key(board_name: str) -> str:
    return _redis_key("board_backoff", board_name)


def _thread_state_key(thread_url: str) -> str:
    return _redis_key("thread_state", thread_url)


def _board_backoff_active(board_name: str) -> int:
    return _ttl_seconds(_board_backoff_key(board_name))


def _board_cooldown_active(board_name: str) -> int:
    return _ttl_seconds(_board_cooldown_key(board_name))


def _thread_should_refresh(thread: dict) -> tuple[bool, str, int]:
    state = _get_cached_json(_thread_state_key(thread["thread_or_question_url"]))
    if not state:
        return True, "first_seen", 0
    now = int(time.time())
    fingerprint = thread.get("thread_fingerprint") or ""
    last_fetched_at = int(state.get("last_fetched_at") or 0)
    previous_fingerprint = str(state.get("fingerprint") or "")
    if not previous_fingerprint or previous_fingerprint != fingerprint:
        return True, "fingerprint_changed", 0
    if (now - last_fetched_at) >= THREAD_REFRESH_WINDOW_SECONDS:
        return True, "refresh_window_elapsed", 0
    ttl_remaining = max(0, THREAD_FETCH_COOLDOWN_SECONDS - (now - last_fetched_at))
    return False, "detail_cooldown_active", ttl_remaining


def _remember_thread_fetch(thread: dict) -> None:
    _set_cached_json(
        _thread_state_key(thread["thread_or_question_url"]),
        {
            "fingerprint": thread.get("thread_fingerprint") or "",
            "reply_count": int(thread.get("reply_count") or 0),
            "last_activity_label": thread.get("last_activity_label") or "",
            "last_fetched_at": int(time.time()),
        },
        ttl_seconds=7 * 24 * 3600,
    )


def _fetch_shopify_html(session, board_name: str, url: str, *, scope: str) -> tuple[str | None, bool]:
    cooldown_remaining = _board_backoff_active(board_name)
    if cooldown_remaining:
        _record_board_event(
            board_name,
            status="cooldown_skipped",
            scope=scope,
            cooldown_remaining_seconds=cooldown_remaining,
            url=url,
        )
        return None, False
    if scope == "board":
        board_cooldown = _board_cooldown_active(board_name)
        if board_cooldown:
            _record_board_event(
                board_name,
                status="cooldown_skipped",
                scope=scope,
                cooldown_remaining_seconds=board_cooldown,
                url=url,
            )
            return None, False
    _sleep_with_jitter()
    try:
        html = fetch_html(session, url)
    except requests.HTTPError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if int(status_code or 0) == 429:
            if _redis:
                _redis.setex(_board_backoff_key(board_name), BOARD_RATE_LIMIT_BACKOFF_SECONDS, str(int(time.time())))
            _record_rate_limit(board_name, url, scope=scope)
            _record_board_event(
                board_name,
                status="rate_limited",
                scope=scope,
                cooldown_remaining_seconds=BOARD_RATE_LIMIT_BACKOFF_SECONDS,
                url=url,
            )
            logger.warning(f"Shopify Community {scope} rate limited for {board_name}: {url}")
            return None, True
        raise
    except Exception:
        raise
    if _redis and scope == "board":
        _redis.setex(_board_cooldown_key(board_name), BOARD_FETCH_COOLDOWN_SECONDS, str(int(time.time())))
    return html, False


def fetch_candidates() -> dict:
    session = build_session()
    seen_urls: set[str] = set()
    board_threads_seen = 0
    candidates_seen = 0
    domains_extracted = 0
    filtered = 0
    stored_ready: list[dict] = []
    errors = 0
    duplicate_skips = 0
    rate_limited = 0
    domainless_signals = 0
    board_stats: list[dict] = []

    for board_name, board_url in BOARD_URLS.items():
        board_seen = 0
        board_candidates = 0
        board_filtered = 0
        board_duplicates = 0
        board_rate_limited = 0
        board_domainless = 0
        try:
            board_html, board_limited = _fetch_shopify_html(session, board_name, board_url, scope="board")
        except Exception as exc:
            errors += 1
            logger.warning(f"Shopify Community board fetch failed for {board_name}: {exc}")
            save_event("public_source_ingestion_error", {"source": SOURCE_NAME, "url": board_url, "error": str(exc)})
            continue
        if board_limited:
            rate_limited += 1
            board_rate_limited += 1
            board_stats.append(
                {
                    "board": board_name,
                    "threads_seen": 0,
                    "candidates_ready": 0,
                    "filtered": 0,
                    "duplicate_skips": 0,
                    "domainless_signals": 0,
                    "rate_limited": 1,
                    "status": "rate_limited",
                }
            )
            continue
        if not board_html:
            board_stats.append(
                {
                    "board": board_name,
                    "threads_seen": 0,
                    "candidates_ready": 0,
                    "filtered": 0,
                    "duplicate_skips": 0,
                    "domainless_signals": 0,
                    "rate_limited": 0,
                    "status": "cooldown_skipped",
                }
            )
            continue
        thread_rows = _parse_board_threads(board_name, board_html)
        board_threads_seen += len(thread_rows)
        board_seen += len(thread_rows)
        for thread in thread_rows:
            if len(seen_urls) >= MAX_THREAD_FETCHES:
                break
            thread_url = thread["thread_or_question_url"]
            if thread_url in seen_urls:
                continue
            seen_urls.add(thread_url)
            should_refresh, skip_reason, ttl_remaining = _thread_should_refresh(thread)
            if not should_refresh:
                duplicate_skips += 1
                board_duplicates += 1
                _record_duplicate_skip(board_name, thread, reason=skip_reason, ttl_seconds=ttl_remaining)
                continue
            try:
                html, thread_limited = _fetch_shopify_html(session, board_name, thread_url, scope="thread")
            except Exception as exc:
                errors += 1
                logger.warning(f"Shopify Community thread fetch failed for {thread_url}: {exc}")
                save_event("public_source_ingestion_error", {"source": SOURCE_NAME, "url": thread_url, "error": str(exc)})
                continue
            if thread_limited:
                rate_limited += 1
                board_rate_limited += 1
                continue
            if not html:
                duplicate_skips += 1
                board_duplicates += 1
                _record_duplicate_skip(board_name, thread, reason="board_cooldown_active", ttl_seconds=_board_backoff_active(board_name))
                continue
            _remember_thread_fetch(thread)
            parsed_candidates = _parse_thread_detail(thread, html)
            for candidate in parsed_candidates:
                candidates_seen += 1
                board_candidates += 1
                if candidate.get("merchant_domain"):
                    domains_extracted += 1
                else:
                    domainless_signals += 1
                    board_domainless += 1
                ok, reason = should_emit_signal(candidate, allow_without_domain=True)
                candidate["filter_reason"] = reason
                if ok:
                    stored_ready.append(candidate)
                else:
                    filtered += 1
                    board_filtered += 1
                    save_event(
                        "shopify_community_candidate_filtered",
                        {
                            "board": board_name,
                            "url": candidate.get("thread_or_question_url"),
                            "title": candidate.get("title"),
                            "reason": reason,
                            "processor": candidate.get("processor"),
                            "distress_type": candidate.get("distress_type"),
                        },
                    )
        status = "signals_ready" if board_candidates and (board_candidates - board_filtered) > 0 else "filtered_only" if board_candidates else "idle"
        _record_board_event(
            board_name,
            status=status,
            threads_seen=board_seen,
            candidates_ready=max(0, board_candidates - board_filtered),
            filtered=board_filtered,
            duplicate_skips=board_duplicates,
            domainless_signals=board_domainless,
            rate_limited=board_rate_limited,
        )
        board_stats.append(
            {
                "board": board_name,
                "threads_seen": board_seen,
                "candidates_ready": max(0, board_candidates - board_filtered),
                "filtered": board_filtered,
                "duplicate_skips": board_duplicates,
                "domainless_signals": board_domainless,
                "rate_limited": board_rate_limited,
                "status": status,
            }
        )

    return {
        "threads_seen": board_threads_seen,
        "candidates_seen": candidates_seen,
        "domains_extracted": domains_extracted,
        "filtered": filtered,
        "errors": errors,
        "duplicate_skips": duplicate_skips,
        "rate_limited": rate_limited,
        "domainless_signals": domainless_signals,
        "board_stats": board_stats,
        "candidates": stored_ready,
    }


def store_signal(signal: dict):
    from runtime.intelligence.ingestion import process_and_store_signal

    return process_and_store_signal(signal["source"], signal["content"])


def run_shopify_community_ingestion():
    result = fetch_candidates()
    stored = 0
    emitted_candidates = result["candidates"]
    top_distress = summarize_top_distress(emitted_candidates)
    board_signal_counts: Counter[str] = Counter()
    for candidate in emitted_candidates:
        signal = {
            "source": SOURCE_NAME,
            "content": format_signal_content(candidate),
        }
        try:
            signal_id = store_signal(signal)
            if signal_id is None:
                continue
            stored += 1
            save_event(
                "shopify_community_signal_created",
                {
                    "signal_id": signal_id,
                    "url": candidate["thread_or_question_url"],
                    "board": candidate["source_category"],
                    "distress_type": candidate.get("distress_type"),
                    "processor": candidate.get("processor"),
                    "merchant_domain": candidate.get("merchant_domain"),
                    "merchant_attribution_state": "domain_resolved" if candidate.get("merchant_domain") else "domainless",
                    "reply_count": int(candidate.get("reply_count") or 0),
                    "last_activity_label": candidate.get("last_activity_label") or "",
                },
            )
            board_signal_counts[str(candidate["source_category"])] += 1
        except Exception as exc:
            result["errors"] += 1
            logger.warning(f"Failed to store Shopify Community signal {candidate['thread_or_question_url']}: {exc}")
            save_event("public_source_ingestion_error", {"source": SOURCE_NAME, "url": candidate["thread_or_question_url"], "error": str(exc)})

    for board_stat in result.get("board_stats") or []:
        board_stat["signals_created"] = int(board_signal_counts.get(board_stat.get("board") or "", 0))
    payload = {
        "threads_seen": result["threads_seen"],
        "candidates_seen": result["candidates_seen"],
        "fetched": result["candidates_seen"],
        "stored": stored,
        "signals": stored,
        "domains_extracted": result["domains_extracted"],
        "filtered": result["filtered"],
        "errors": result["errors"],
        "duplicate_skips": result.get("duplicate_skips", 0),
        "rate_limited": result.get("rate_limited", 0),
        "domainless_signals": result.get("domainless_signals", 0),
        "top_distress_pattern": top_distress,
        "board_stats": result.get("board_stats") or [],
    }
    save_event("shopify_community_ingestion_cycle", payload)
    logger.info(f"Shopify Community ingestion: stored {stored} from {result['candidates_seen']} candidates")
    return payload


def run_shopify_forum_ingestion():
    """Compatibility alias for legacy scheduler/task references."""
    return run_shopify_community_ingestion()
