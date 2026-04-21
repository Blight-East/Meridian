from __future__ import annotations

from collections import Counter
from typing import Any

from sqlalchemy import text

from memory.structured.db import engine, save_event

_TABLE_READY = False


def ensure_outreach_learning_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    with engine.connect() as conn:
        try:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS outreach_learning_memory (
                        id BIGSERIAL PRIMARY KEY,
                        opportunity_id BIGINT,
                        merchant_domain TEXT,
                        processor TEXT,
                        distress_type TEXT,
                        selected_play TEXT,
                        outreach_type TEXT NOT NULL DEFAULT 'initial_outreach',
                        rewrite_style TEXT NOT NULL DEFAULT 'standard',
                        outcome_status TEXT NOT NULL DEFAULT 'pending',
                        lesson_summary TEXT NOT NULL DEFAULT '',
                        operator_note TEXT NOT NULL DEFAULT '',
                        source TEXT NOT NULL DEFAULT 'system',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS outreach_learning_memory_created_idx
                    ON outreach_learning_memory (created_at DESC)
                    """
                )
            )
            conn.execute(text("ALTER TABLE outreach_learning_memory ADD COLUMN IF NOT EXISTS outreach_type TEXT NOT NULL DEFAULT 'initial_outreach'"))
            conn.execute(text("ALTER TABLE outreach_learning_memory ADD COLUMN IF NOT EXISTS rewrite_style TEXT NOT NULL DEFAULT 'standard'"))
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS outreach_learning_memory_play_idx
                    ON outreach_learning_memory (selected_play, outreach_type, outcome_status, created_at DESC)
                    """
                )
            )
        except Exception as exc:
            message = str(exc).lower()
            if "outreach_learning_memory" not in message and "pg_type_typname_nsp_index" not in message:
                raise
        conn.commit()
    _TABLE_READY = True


def record_outreach_learning(
    *,
    opportunity_id: int | None = None,
    merchant_domain: str = "",
    processor: str = "",
    distress_type: str = "",
    selected_play: str = "",
    outreach_type: str = "",
    rewrite_style: str = "",
    outcome_status: str = "",
    lesson_summary: str = "",
    operator_note: str = "",
    source: str = "system",
) -> dict[str, Any]:
    ensure_outreach_learning_table()
    payload = {
        "opportunity_id": int(opportunity_id) if opportunity_id else None,
        "merchant_domain": (merchant_domain or "").strip().lower(),
        "processor": (processor or "").strip().lower(),
        "distress_type": (distress_type or "").strip().lower(),
        "selected_play": (selected_play or "").strip().lower(),
        "outreach_type": (outreach_type or "initial_outreach").strip().lower(),
        "rewrite_style": (rewrite_style or "standard").strip().lower(),
        "outcome_status": (outcome_status or "pending").strip().lower(),
        "lesson_summary": (lesson_summary or "").strip(),
        "operator_note": (operator_note or "").strip(),
        "source": (source or "system").strip().lower(),
    }
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO outreach_learning_memory (
                    opportunity_id,
                    merchant_domain,
                    processor,
                    distress_type,
                    selected_play,
                    outreach_type,
                    rewrite_style,
                    outcome_status,
                    lesson_summary,
                    operator_note,
                    source
                )
                VALUES (
                    :opportunity_id,
                    :merchant_domain,
                    :processor,
                    :distress_type,
                    :selected_play,
                    :outreach_type,
                    :rewrite_style,
                    :outcome_status,
                    :lesson_summary,
                    :operator_note,
                    :source
                )
                RETURNING *
                """
            ),
            payload,
        ).mappings().first()
        conn.commit()
    if row:
        save_event(
            "outreach_learning_recorded",
            {
                "opportunity_id": payload["opportunity_id"],
                "merchant_domain": payload["merchant_domain"],
                "selected_play": payload["selected_play"],
                "outcome_status": payload["outcome_status"],
                "source": payload["source"],
            },
        )
    return _serialize_learning_row(dict(row or payload))


def list_outreach_learning(limit: int = 10) -> dict[str, Any]:
    ensure_outreach_learning_table()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM outreach_learning_memory
                ORDER BY created_at DESC
                LIMIT :limit
                """
            ),
            {"limit": max(1, int(limit))},
        ).mappings().fetchall()
    items = [_serialize_learning_row(dict(row)) for row in rows]
    return {"count": len(items), "items": items}


def summarize_outreach_learning(limit: int = 50) -> dict[str, Any]:
    records = list_outreach_learning(limit=max(1, int(limit))).get("items", [])
    outcome_counter: Counter[str] = Counter()
    play_outcomes: dict[str, Counter[str]] = {}
    recent_lessons: list[str] = []
    for record in records:
        outcome = str(record.get("outcome_status") or "pending")
        play = str(record.get("selected_play") or "unknown")
        outcome_counter[outcome] += 1
        play_outcomes.setdefault(play, Counter())[outcome] += 1
        lesson = str(record.get("lesson_summary") or record.get("operator_note") or "").strip()
        if lesson:
            recent_lessons.append(lesson)

    top_patterns: list[dict[str, Any]] = []
    for play, counts in sorted(
        play_outcomes.items(),
        key=lambda item: (item[1].get("won", 0), sum(item[1].values())),
        reverse=True,
    )[:5]:
        total = sum(counts.values())
        wins = counts.get("won", 0)
        top_patterns.append(
            {
                "selected_play": play,
                "total": total,
                "wins": wins,
                "losses": counts.get("lost", 0),
                "ignored": counts.get("ignored", 0),
                "win_rate": round(wins / total, 4) if total else 0.0,
            }
        )

    return {
        "records_considered": len(records),
        "outcome_counts": dict(outcome_counter),
        "top_patterns": top_patterns,
        "recent_lessons": recent_lessons[:5],
    }


def get_outreach_learning_signal(
    *,
    selected_play: str = "",
    processor: str = "",
    distress_type: str = "",
    outreach_type: str = "",
) -> dict[str, Any]:
    ensure_outreach_learning_table()
    selected_play = str(selected_play or "").strip().lower()
    processor = str(processor or "").strip().lower()
    distress_type = str(distress_type or "").strip().lower()
    outreach_type = str(outreach_type or "").strip().lower()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT selected_play, processor, distress_type, outreach_type, rewrite_style, outcome_status, lesson_summary, operator_note
                FROM outreach_learning_memory
                WHERE (:selected_play = '' OR selected_play = :selected_play)
                  AND (:processor = '' OR processor = :processor)
                  AND (:distress_type = '' OR distress_type = :distress_type)
                  AND (:outreach_type = '' OR outreach_type = :outreach_type)
                ORDER BY created_at DESC
                LIMIT 50
                """
            ),
            {
                "selected_play": selected_play,
                "processor": processor,
                "distress_type": distress_type,
                "outreach_type": outreach_type,
            },
        ).mappings().fetchall()

    rows = [dict(row) for row in rows]
    total = len(rows)
    wins = sum(1 for row in rows if row.get("outcome_status") == "won")
    losses = sum(1 for row in rows if row.get("outcome_status") == "lost")
    ignored = sum(1 for row in rows if row.get("outcome_status") == "ignored")
    pending = sum(1 for row in rows if row.get("outcome_status") == "pending")
    rewrite_style_counts: Counter[str] = Counter()
    winning_rewrite_styles: Counter[str] = Counter()
    confidence_bonus = 0
    if total:
        confidence_bonus = (wins * 14) - (losses * 9) - (ignored * 4) + min(pending, 3)
        confidence_bonus = max(-20, min(25, confidence_bonus))
    lessons = []
    for row in rows[:3]:
        lesson = str(row.get("lesson_summary") or row.get("operator_note") or "").strip()
        if lesson:
            lessons.append(lesson)
    for row in rows:
        style = str(row.get("rewrite_style") or "standard").strip().lower() or "standard"
        rewrite_style_counts[style] += 1
        if row.get("outcome_status") == "won":
            winning_rewrite_styles[style] += 1
    preferred_rewrite_style = ""
    if winning_rewrite_styles:
        preferred_rewrite_style = winning_rewrite_styles.most_common(1)[0][0]
    elif rewrite_style_counts:
        preferred_rewrite_style = rewrite_style_counts.most_common(1)[0][0]
    return {
        "selected_play": selected_play,
        "processor": processor,
        "distress_type": distress_type,
        "outreach_type": outreach_type,
        "records": total,
        "wins": wins,
        "losses": losses,
        "ignored": ignored,
        "pending": pending,
        "win_rate": round(wins / total, 4) if total else 0.0,
        "confidence_bonus": confidence_bonus,
        "preferred_rewrite_style": preferred_rewrite_style,
        "rewrite_style_counts": dict(rewrite_style_counts),
        "recent_lessons": lessons,
    }


def _serialize_learning_row(row: dict[str, Any]) -> dict[str, Any]:
    created_at = row.get("created_at")
    if created_at is not None and hasattr(created_at, "isoformat"):
        row["created_at"] = created_at.isoformat()
    return row
