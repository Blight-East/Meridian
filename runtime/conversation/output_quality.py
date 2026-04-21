"""
Output Quality Gate — Post-Generation Filter for Meridian
==========================================================
Scores and auto-cleans Meridian's outgoing messages against the
communication rules before they reach the operator.

This is runtime enforcement of the communication skill, not a
prompt instruction. The model can ignore prompt rules at will;
it cannot bypass a code-level filter.

Usage:
    from runtime.conversation.output_quality import score_output, clean_output
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("meridian.output_quality")

# Score threshold: below this, auto-clean is applied.
# Set to 0 via env var for score-only mode (telemetry without cleaning).
_CLEAN_THRESHOLD = int(os.getenv("MERIDIAN_OUTPUT_QUALITY_CLEAN_THRESHOLD", "0"))

# ── Pattern Definitions ──────────────────────────────────────────────────────

# Rule-of-three filler: adjective chains like "strategic, transformative, and robust"
_RULE_OF_THREE_RE = re.compile(
    r"\b(\w+),\s+(\w+),\s+and\s+(\w+)\b",
    re.IGNORECASE,
)
_FILLER_ADJECTIVES = {
    "strategic", "transformative", "robust", "comprehensive", "holistic",
    "innovative", "dynamic", "cutting-edge", "groundbreaking", "scalable",
    "seamless", "world-class", "best-in-class", "mission-critical",
    "next-generation", "state-of-the-art", "enterprise-grade", "end-to-end",
    "actionable", "impactful",
}

# Em-dash overuse
_EM_DASH_RE = re.compile(r"—")

# Marketing automation phrases
_MARKETING_PHRASES = [
    r"\bi(?:'d| would) love to\b",
    r"\bi(?:'m| am) excited to\b",
    r"\blet(?:'s| us) explore\b",
    r"\blet(?:'s| us) dive (?:deep )?into\b",
    r"\blet(?:'s| us) unpack\b",
    r"\bgreat question\b",
    r"\bthat(?:'s| is) a great (?:point|question|observation)\b",
    r"\babsolutely[!.]\b",
    r"\bi hear you\b",
    r"\bhere(?:'s| is) the(?:thing| deal)\b",
]
_MARKETING_RE = re.compile("|".join(_MARKETING_PHRASES), re.IGNORECASE)

# Consultant jargon
_JARGON_WORDS = {
    "leverage", "synergy", "synergies", "synergistic", "paradigm",
    "disruptive", "disruption", "game-changing", "game-changer",
    "move the needle", "value-add", "ideate", "ideation",
    "net-net", "north star",
}
_JARGON_RE = re.compile(
    r"\b(" + "|".join(re.escape(w) for w in sorted(_JARGON_WORDS, key=len, reverse=True)) + r")(?:s|ing|ed)?\b",
    re.IGNORECASE,
)

# Inflated significance markers
_INFLATION_PHRASES = [
    r"\bthis is (?:a )?(?:huge|massive|enormous|incredible|game-changing)\b",
    r"\bthis changes everything\b",
    r"\bthis is going to be (?:huge|massive|incredible)\b",
    r"\bI cannot overstate\b",
    r"\bthe implications (?:are|here are) (?:huge|enormous|staggering)\b",
]
_INFLATION_RE = re.compile("|".join(_INFLATION_PHRASES), re.IGNORECASE)

# Sycophantic openers
_SYCOPHANTIC_PHRASES = [
    r"^(?:Absolutely|Great (?:question|point|observation))(?:[!,.]|\s)",
    r"^(?:That(?:'s| is) (?:a |an )?(?:great|excellent|fantastic|wonderful|brilliant) )",
    r"^(?:I (?:really )?appreciate (?:you|your|that|this))",
    r"^(?:What a thoughtful|What an insightful)",
    r"^Of course!",
]
_SYCOPHANTIC_RE = re.compile("|".join(_SYCOPHANTIC_PHRASES), re.IGNORECASE | re.MULTILINE)


# ── Violation Types ──────────────────────────────────────────────────────────

@dataclass
class Violation:
    """A specific quality violation found in the output."""
    category: str
    matched_text: str
    line_number: int = 0
    severity: int = 5  # 1-10 scale


@dataclass
class QualityScore:
    """Result of scoring an output."""
    score: int  # 0-100
    violations: list[Violation] = field(default_factory=list)
    total_deductions: int = 0

    @property
    def needs_cleaning(self) -> bool:
        return self.score < _CLEAN_THRESHOLD


# ── Scoring ──────────────────────────────────────────────────────────────────

def score_output(text: str) -> QualityScore:
    """
    Score a Meridian output for communication quality.

    Returns a QualityScore with a 0-100 score and a list of violations.
    100 = clean output. Lower = more violations.
    """
    if not text or not text.strip():
        return QualityScore(score=100)

    violations: list[Violation] = []
    lines = text.split("\n")

    # Check rule-of-three filler
    for i, line in enumerate(lines, 1):
        for match in _RULE_OF_THREE_RE.finditer(line):
            words = {match.group(1).lower(), match.group(2).lower(), match.group(3).lower()}
            if len(words & _FILLER_ADJECTIVES) >= 2:
                violations.append(Violation(
                    category="rule_of_three",
                    matched_text=match.group(0),
                    line_number=i,
                    severity=6,
                ))

    # Check em-dash overuse
    em_dash_count = len(_EM_DASH_RE.findall(text))
    if em_dash_count >= 3:
        violations.append(Violation(
            category="em_dash_overuse",
            matched_text=f"{em_dash_count} em-dashes in one message",
            severity=4,
        ))

    # Check marketing phrases
    for match in _MARKETING_RE.finditer(text):
        violations.append(Violation(
            category="marketing_phrase",
            matched_text=match.group(0),
            severity=5,
        ))

    # Check consultant jargon
    for match in _JARGON_RE.finditer(text):
        violations.append(Violation(
            category="consultant_jargon",
            matched_text=match.group(0),
            severity=6,
        ))

    # Check inflated significance
    for match in _INFLATION_RE.finditer(text):
        violations.append(Violation(
            category="inflated_significance",
            matched_text=match.group(0),
            severity=7,
        ))

    # Check sycophantic openers
    for match in _SYCOPHANTIC_RE.finditer(text):
        violations.append(Violation(
            category="sycophantic_opener",
            matched_text=match.group(0).strip(),
            severity=6,
        ))

    # Compute score
    total_deductions = sum(v.severity for v in violations)
    score = max(0, 100 - total_deductions)

    return QualityScore(
        score=score,
        violations=violations,
        total_deductions=total_deductions,
    )


# ── Cleaning ─────────────────────────────────────────────────────────────────

def clean_output(text: str) -> tuple[str, list[str]]:
    """
    Auto-fix the most common quality violations.

    Returns:
        Tuple of (cleaned_text, list_of_changes_made).
    """
    if not text or not text.strip():
        return text, []

    cleaned = text
    changes: list[str] = []

    # Fix em-dash overuse: replace excessive em-dashes with commas or periods
    em_dashes = list(_EM_DASH_RE.finditer(cleaned))
    if len(em_dashes) >= 3:
        # Keep the first one, replace the rest
        replaced = 0
        result_parts = []
        last_end = 0
        for i, match in enumerate(em_dashes):
            result_parts.append(cleaned[last_end:match.start()])
            if i == 0:
                result_parts.append("—")  # keep first
            else:
                # Replace with comma if mid-sentence, period if at a natural break
                after = cleaned[match.end():match.end() + 1] if match.end() < len(cleaned) else ""
                if after and after[0].isupper():
                    result_parts.append(". ")
                else:
                    result_parts.append(", ")
                replaced += 1
            last_end = match.end()
        result_parts.append(cleaned[last_end:])
        cleaned = "".join(result_parts)
        if replaced > 0:
            changes.append(f"Replaced {replaced} excess em-dash(es)")

    # Fix sycophantic openers: remove them
    sycophantic_match = _SYCOPHANTIC_RE.match(cleaned)
    if sycophantic_match:
        opener = sycophantic_match.group(0).strip()
        # Remove the opener and any trailing whitespace/punctuation
        after_opener = cleaned[sycophantic_match.end():].lstrip(" ,.")
        if after_opener:
            # Capitalize the next word
            cleaned = after_opener[0].upper() + after_opener[1:] if len(after_opener) > 1 else after_opener.upper()
            changes.append(f"Removed sycophantic opener: '{opener}'")

    # Fix marketing phrases: remove or replace
    def _replace_marketing(match: re.Match) -> str:
        phrase = match.group(0).lower().strip()
        if "love to" in phrase or "excited to" in phrase:
            return "I can"
        if "explore" in phrase or "dive" in phrase or "unpack" in phrase:
            return "look at"
        if "great question" in phrase or "great point" in phrase:
            return ""
        if "absolutely" in phrase:
            return "Yes."
        return ""

    before = cleaned
    cleaned = _MARKETING_RE.sub(_replace_marketing, cleaned)
    if cleaned != before:
        changes.append("Replaced marketing automation phrases")

    # Fix jargon: replace with plain language
    _JARGON_REPLACEMENTS = {
        "leverage": "use",
        "synergy": "overlap",
        "synergies": "overlaps",
        "paradigm": "model",
        "game-changing": "significant",
        "game-changer": "significant change",
        "move the needle": "make a difference",
        "value-add": "value",
        "ideate": "plan",
        "net-net": "bottom line",
        "north star": "goal",
    }

    def _replace_jargon(match: re.Match) -> str:
        word = match.group(0).lower()
        for jargon, replacement in _JARGON_REPLACEMENTS.items():
            if word.startswith(jargon) or word == jargon:
                return replacement
        return match.group(0)

    before = cleaned
    cleaned = _JARGON_RE.sub(_replace_jargon, cleaned)
    if cleaned != before:
        changes.append("Replaced consultant jargon with plain language")

    # Clean up double spaces
    cleaned = re.sub(r"  +", " ", cleaned)
    # Clean up space before punctuation
    cleaned = re.sub(r" ([.,;!?])", r"\1", cleaned)
    # Clean up empty parentheses/quotes
    cleaned = re.sub(r"\(\s*\)", "", cleaned)

    return cleaned.strip(), changes


def filter_output(text: str) -> tuple[str, QualityScore]:
    """
    Score and optionally clean an output in one pass.

    Returns:
        Tuple of (final_text, quality_score).
        If the score is below threshold, final_text is the cleaned version.
    """
    quality = score_output(text)
    if quality.needs_cleaning:
        cleaned, changes = clean_output(text)
        if changes:
            logger.info(
                "output_quality: score=%d, applied %d fix(es): %s",
                quality.score, len(changes), "; ".join(changes),
            )
        return cleaned, quality
    return text, quality
