"""
Semantic Memory Module
Uses local sentence-transformers to generate embeddings for distress signals
and stores them in pgvector for similarity search to augment LLM reasoning.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text
from config.logging_config import get_logger

logger = get_logger("semantic_memory")
engine = create_engine("postgresql://postgres@127.0.0.1/agent_flux")

# Load globally once on module init
logger.info("Loading SentenceTransformer model 'all-MiniLM-L6-v2'...")
try:
    model = SentenceTransformer("all-MiniLM-L6-v2")
    logger.info("SentenceTransformer model loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load sentence-transformers model: {e}")
    model = None


def generate_embedding(text_content):
    """Generate a 384-dimensional dense vector representation of the text."""
    if not model or not text_content:
        return None
    try:
        # Convert tensor to python list of floats
        return model.encode(text_content).tolist()
    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        return None


def store_signal_embedding(signal_id, text_content):
    """Generate and store the embedding for a newly ingested signal."""
    embedding = generate_embedding(text_content)
    if not embedding:
        return False
        
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO signal_embeddings (signal_id, embedding)
                VALUES (:sid, :emb)
                ON CONFLICT (signal_id) DO UPDATE SET embedding = EXCLUDED.embedding
            """), {"sid": signal_id, "emb": str(embedding)})
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error storing embedding for signal {signal_id}: {e}")
        return False


def retrieve_similar_signals(query_text, limit=5, threshold=0.3):
    """
    Search pgvector for the most semantically similar historical signals.
    Cosine distance <=> is used; smaller distance = highly similar.
    We convert distance to similarity score (1 - distance) and filter.
    """
    embedding = generate_embedding(query_text)
    if not embedding:
        return []

    try:
        with engine.connect() as conn:
            # <=> is the cosine distance operator in pgvector
            rows = conn.execute(text("""
                SELECT s.id, s.content, 
                       (1 - (se.embedding <=> CAST(:query_embedding AS vector))) as similarity
                FROM signal_embeddings se
                JOIN signals s ON s.id = se.signal_id
                WHERE (1 - (se.embedding <=> CAST(:query_embedding AS vector))) > :threshold
                ORDER BY se.embedding <=> CAST(:query_embedding AS vector)
                LIMIT :limit
            """), {
                "query_embedding": str(embedding),
                "threshold": threshold,
                "limit": limit
            }).fetchall()
            
            return [{"id": r[0], "content": r[1], "similarity": round(r[2], 3)} for r in rows]
    except Exception as e:
        logger.error(f"Error retrieving similar signals: {e}")
        return []
