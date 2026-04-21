from sqlalchemy import create_engine, text
from config.logging_config import get_logger

logger = get_logger("db_query")

engine = create_engine("postgresql://localhost/agent_flux")


def db_query(query_str: str):
    """Execute a read-only database query."""
    q = query_str.strip().lower()
    if any(kw in q for kw in ["insert", "update", "delete", "drop", "alter", "truncate"]):
        return {"error": "Only SELECT queries are allowed"}
    try:
        with engine.connect() as c:
            rows = c.execute(text(query_str)).fetchall()
            return {"rows": [dict(r._mapping) for r in rows][:100]}
    except Exception as e:
        logger.error(f"DB query error: {e}")
        return {"error": str(e)}
