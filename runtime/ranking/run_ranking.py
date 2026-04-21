import sys, os, re, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from runtime.ranking.signal_ranker import score_signal
from memory.structured.db import save_event
from config.logging_config import get_logger

logger = get_logger("ranking")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

def extract_metadata(content):
    score = 0
    comments = 0
    m_score = re.search(r'\[score:(\d+)', content)
    m_comments = re.search(r'comments:(\d+)', content)
    m_pts = re.search(r'pts:(\d+)', content)
    if m_score:
        score = int(m_score.group(1))
    elif m_pts:
        score = int(m_pts.group(1))
    if m_comments:
        comments = int(m_comments.group(1))
    return score, comments

def rank_signals(batch_size=50):
    start = time.time()
    ranked_count = 0
    revenue_detected = 0
    merchant_relevant = 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, content, detected_at
            FROM signals
            WHERE priority_score = 0 OR priority_score IS NULL
            ORDER BY id DESC LIMIT :limit
        """), {"limit": batch_size})
        signals = [dict(row._mapping) for row in rows]

    for sig in signals:
        content = sig.get("content", "")
        created_at = sig.get("detected_at")
        score, comments = extract_metadata(content)

        priority, metadata = score_signal(content, score=score, comments=comments, created_at=created_at)

        if metadata.get("revenue_detected"):
            revenue_detected += 1
        if metadata.get("merchant_relevant"):
            merchant_relevant += 1

        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE signals SET priority_score = :priority, ranked_at = CURRENT_TIMESTAMP, age_weight = :aw,
                    merchant_relevant = :mr, revenue_detected = :rd
                WHERE id = :id
            """), {
                "priority": priority,
                "aw": metadata.get("age_weight", 1.0),
                "mr": metadata.get("merchant_relevant", False),
                "rd": metadata.get("revenue_detected", False),
                "id": sig["id"],
            })
            conn.commit()

        save_event("signal_ranked", {"signal_id": sig["id"], "priority_score": priority, **metadata})
        ranked_count += 1

    elapsed = round(time.time() - start, 2)
    if ranked_count > 0:
        save_event("ranking_batch_complete", {
            "ranked_count": ranked_count,
            "revenue": revenue_detected,
            "merchant": merchant_relevant,
            "seconds": elapsed,
        })
        logger.info(f"Ranked {ranked_count} signals ({revenue_detected} revenue, {merchant_relevant} merchant) in {elapsed}s")
    return {"ranked_count": ranked_count, "elapsed_seconds": elapsed}

if __name__ == "__main__":
    print(rank_signals())
