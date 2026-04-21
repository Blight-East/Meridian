"""
Stack Overflow public-web scanner for payment-flow merchant distress.
"""
import os
import re
import sys

from bs4 import BeautifulSoup

try:
    from tools.browser_tool import browser_fetch
except ImportError:
    browser_fetch = None

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
    parse_created_at,
    select_best_merchant_domain,
    should_emit_signal,
    soup_text,
    summarize_top_distress,
)

logger = get_logger("stack_overflow_signals")

SOURCE_NAME = "stack_overflow"
SOURCE_LABEL = "Stack Overflow"
BASE_URL = "https://stackoverflow.com"
TAG_URLS = {
    "stripe-payments": f"{BASE_URL}/questions/tagged/stripe-payments?tab=Newest",
    "shopify": f"{BASE_URL}/questions/tagged/shopify?tab=Newest",
    "payment-gateway": f"{BASE_URL}/questions/tagged/payment-gateway?tab=Newest",
    "paypal": f"{BASE_URL}/questions/tagged/paypal?tab=Newest",
    "braintree": f"{BASE_URL}/questions/tagged/braintree?tab=Newest",
    "checkout": f"{BASE_URL}/questions/tagged/checkout?tab=Newest",
    "webhooks": f"{BASE_URL}/questions/tagged/webhooks?tab=Newest",
    "api-error": f"{BASE_URL}/questions/tagged/api-error?tab=Newest",
}
TITLE_OR_SNIPPET_PATTERNS = (
    re.compile(r"\bpayment provider (?:is )?not enabled\b", re.IGNORECASE),
    re.compile(r"\baccount under review\b", re.IGNORECASE),
    re.compile(r"\bpayouts?\s+(?:stopped|paused|on hold)\b", re.IGNORECASE),
    re.compile(r"\bgateway timeout\b", re.IGNORECASE),
    re.compile(r"\btransaction declined\b", re.IGNORECASE),
    re.compile(r"\baccount restricted\b", re.IGNORECASE),
    re.compile(r"\bpayments?\s+disabled\b", re.IGNORECASE),
    re.compile(r"\bwebhooks?\s+failing\b", re.IGNORECASE),
    re.compile(r"\bcheckout\b.*\bpayment\b.*\bfail", re.IGNORECASE),
)
PAYMENT_CONTEXT_PATTERN = re.compile(
    r"\b(?:stripe|shopify|paypal|braintree|payment|payments|checkout|gateway|webhook|merchant)\b",
    re.IGNORECASE,
)
MAX_QUESTIONS_PER_TAG = 5
MAX_QUESTION_FETCHES = 12
MAX_ANSWER_SNIPPETS = 2
MAX_COMMENT_SNIPPETS = 2


def _matches_candidate(title: str, excerpt: str) -> bool:
    haystack = f"{title} {excerpt}"
    return any(pattern.search(haystack) for pattern in TITLE_OR_SNIPPET_PATTERNS)


def _parse_tag_listing(tag_name: str, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    questions: list[dict] = []
    seen: set[str] = set()
    for row in soup.select("div.s-post-summary"):
        link = row.select_one("a.s-link")
        title = soup_text(link)
        question_url = absolute_url(BASE_URL, link.get("href") if link else "")
        excerpt = soup_text(row.select_one(".s-post-summary--content-excerpt"))
        if not title or not question_url or question_url in seen:
            continue
        seen.add(question_url)
        if not PAYMENT_CONTEXT_PATTERN.search(f"{title} {excerpt}"):
            continue
        answer_count = 0
        answer_node = row.select_one(".s-post-summary--stats-item[title*='answer'], .s-post-summary--stats-item")
        if answer_node:
            match = re.search(r"(\d+)\s+(?:answer|answers|reply|replies)", soup_text(answer_node))
            if match:
                answer_count = int(match.group(1))
        questions.append(
            {
                "source_category": tag_name,
                "thread_or_question_url": question_url,
                "title": title,
                "excerpt": excerpt,
                "answer_count": answer_count,
            }
        )
        if len(questions) >= MAX_QUESTIONS_PER_TAG:
            break
    return questions


def _parse_question_detail(candidate: dict, html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    question = soup.select_one("div.question")
    if question is None:
        return None
    title = soup_text(soup.select_one("h1 a.question-hyperlink")) or candidate["title"]
    question_body = soup_text(question.select_one("div.js-post-body"))
    if not question_body:
        return None
    question_hrefs = extract_hrefs(question, base_url=BASE_URL)
    merchant_domain, ranked_domains = select_best_merchant_domain(question_body, question_hrefs)
    author_handle = question.get("data-author-username") or "unknown"
    created_at = parse_created_at(question, selector="time[itemprop='dateCreated'], time")
    evidence_parts: list[str] = []
    for answer in soup.select("div.answer")[:MAX_ANSWER_SNIPPETS]:
        answer_body = soup_text(answer.select_one("div.js-post-body"))
        if answer_body:
            evidence_parts.append(answer_body[:260])
    for comment in soup.select("ul.comments-list li.comment")[:MAX_COMMENT_SNIPPETS]:
        comment_text = soup_text(comment)
        if comment_text:
            evidence_parts.append(comment_text[:180])
    evidence_text = " | ".join(evidence_parts)
    classified = classify_source_candidate(
        title=title,
        body_text=f"{question_body} {evidence_text}".strip(),
        merchant_domain=merchant_domain,
    )
    return {
        **candidate,
        **classified,
        "source_name": SOURCE_NAME,
        "source_label": SOURCE_LABEL,
        "title": title,
        "body_text": question_body,
        "author_handle": author_handle,
        "created_at": created_at,
        "merchant_domain": merchant_domain,
        "domain_candidates": ranked_domains,
        "evidence_text": evidence_text,
    }


def fetch_candidates() -> dict:
    session = build_session()
    questions_seen = 0
    candidates_seen = 0
    domains_extracted = 0
    filtered = 0
    errors = 0
    seen_urls: set[str] = set()
    candidates: list[dict] = []

    for tag_name, tag_url in TAG_URLS.items():
        try:
            if browser_fetch:
                res = browser_fetch(tag_url)
                html = res.get("html", "") if res.get("status") == "ok" else ""
                if not html:
                    raise Exception(f"browser_fetch failed or returned empty for {tag_url}")
            else:
                html = fetch_html(session, tag_url)
        except Exception as exc:
            errors += 1
            logger.warning(f"Stack Overflow tag fetch failed for {tag_name}: {exc}")
            save_event("public_source_ingestion_error", {"source": SOURCE_NAME, "url": tag_url, "error": str(exc)})
            continue
        rows = _parse_tag_listing(tag_name, html)
        questions_seen += len(rows)
        for row in rows:
            if len(seen_urls) >= MAX_QUESTION_FETCHES:
                break
            question_url = row["thread_or_question_url"]
            if question_url in seen_urls:
                continue
            seen_urls.add(question_url)
            try:
                if browser_fetch:
                    res = browser_fetch(question_url)
                    detail_html = res.get("html", "") if res.get("status") == "ok" else ""
                    if not detail_html:
                        raise Exception(f"browser_fetch failed or returned empty for {question_url}")
                else:
                    detail_html = fetch_html(session, question_url)
            except Exception as exc:
                errors += 1
                logger.warning(f"Stack Overflow question fetch failed for {question_url}: {exc}")
                save_event("public_source_ingestion_error", {"source": SOURCE_NAME, "url": question_url, "error": str(exc)})
                continue
            parsed = _parse_question_detail(row, detail_html)
            if not parsed:
                continue
            candidates_seen += 1
            if parsed.get("merchant_domain"):
                domains_extracted += 1
            ok, reason = should_emit_signal(parsed, allow_without_domain=False)
            parsed["filter_reason"] = reason
            if ok:
                candidates.append(parsed)
            else:
                filtered += 1

    return {
        "questions_seen": questions_seen,
        "candidates_seen": candidates_seen,
        "domains_extracted": domains_extracted,
        "filtered": filtered,
        "errors": errors,
        "candidates": candidates,
    }


def store_signal(signal: dict):
    from runtime.intelligence.ingestion import process_and_store_signal

    return process_and_store_signal(signal["source"], signal["content"])


def run_stack_overflow_ingestion():
    result = fetch_candidates()
    stored = 0
    top_distress = summarize_top_distress(result["candidates"])
    for candidate in result["candidates"]:
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
                "stack_overflow_signal_created",
                {
                    "signal_id": signal_id,
                    "url": candidate["thread_or_question_url"],
                    "tag": candidate["source_category"],
                    "distress_type": candidate.get("distress_type"),
                    "processor": candidate.get("processor"),
                    "merchant_domain": candidate.get("merchant_domain"),
                },
            )
        except Exception as exc:
            result["errors"] += 1
            logger.warning(f"Failed to store Stack Overflow signal {candidate['thread_or_question_url']}: {exc}")
            save_event("public_source_ingestion_error", {"source": SOURCE_NAME, "url": candidate["thread_or_question_url"], "error": str(exc)})

    payload = {
        "questions_seen": result["questions_seen"],
        "candidates_seen": result["candidates_seen"],
        "fetched": result["candidates_seen"],
        "stored": stored,
        "signals": stored,
        "domains_extracted": result["domains_extracted"],
        "filtered": result["filtered"],
        "errors": result["errors"],
        "top_distress_pattern": top_distress,
    }
    save_event("stack_overflow_ingestion_cycle", payload)
    logger.info(f"Stack Overflow ingestion: stored {stored} from {result['candidates_seen']} candidates")
    return payload
