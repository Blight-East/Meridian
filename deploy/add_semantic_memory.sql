CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS signal_embeddings (
    signal_id INTEGER REFERENCES signals(id),
    embedding vector(384),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (signal_id)
);

CREATE INDEX IF NOT EXISTS idx_signal_embeddings ON signal_embeddings USING hnsw (embedding vector_cosine_ops);
