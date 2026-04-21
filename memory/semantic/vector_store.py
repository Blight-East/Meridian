from config.logging_config import get_logger

logger = get_logger("vector_store")


def store_embedding(content, metadata=None):
    """Store an embedding vector. No-op if sentence_transformers unavailable."""
    try:
        from runtime.intelligence.semantic_memory import store_signal_embedding
        signal_id = (metadata or {}).get("signal_id")
        if signal_id is None:
            return False
        return store_signal_embedding(signal_id, content)
    except Exception as e:
        logger.warning(f"store_embedding skipped: {e}")
        return False


def search_similar(content, limit=5, threshold=0.85):
    """Search for similar signals using semantic memory."""
    try:
        from runtime.intelligence.semantic_memory import retrieve_similar_signals
        return retrieve_similar_signals(content, limit=limit, threshold=threshold)
    except Exception as e:
        logger.warning(f"search_similar skipped: {e}")
        return []
