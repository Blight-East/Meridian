import sys, os
from functools import lru_cache
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import create_engine, text
from config.logging_config import get_logger

logger = get_logger("chat_memory")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux", pool_pre_ping=True, pool_recycle=300)


@lru_cache(maxsize=1)
def _conversation_key_column() -> str:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'conversations'
        """)).fetchall()
    columns = {str(row[0]) for row in rows}
    if "user_id" in columns:
        return "user_id"
    if "conversation_id" in columns:
        return "conversation_id"
    raise RuntimeError("conversations table must have either user_id or conversation_id")


def save_message(user_id, role, content):
    key_column = _conversation_key_column()
    with engine.connect() as conn:
        conn.execute(text(f"""
            INSERT INTO conversations ({key_column}, role, content)
            VALUES (:user_id, :role, :content)
        """), {"user_id": str(user_id), "role": role, "content": content})
        conn.commit()

def get_recent_context(user_id, limit=10):
    key_column = _conversation_key_column()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT role, content
            FROM (
                SELECT id, role, content FROM conversations
                WHERE {key_column} = :user_id
                ORDER BY id DESC LIMIT :limit
            ) AS recent
            ORDER BY id ASC
        """), {"user_id": str(user_id), "limit": limit})
        return [{"role": row._mapping["role"], "content": row._mapping["content"]} for row in rows]

def get_message_count(user_id):
    key_column = _conversation_key_column()
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM conversations WHERE {key_column} = :user_id"), {"user_id": str(user_id)}).scalar()

def summarize_and_truncate(user_id, summary_content):
    key_column = _conversation_key_column()
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM conversations WHERE {key_column} = :user_id"), {"user_id": str(user_id)})
        conn.execute(text(f"""
            INSERT INTO conversations ({key_column}, role, content)
            VALUES (:user_id, 'system', :summary)
        """), {"user_id": str(user_id), "summary": f"Previous conversation summary: {summary_content}"})
        conn.commit()
