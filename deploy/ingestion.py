import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sqlalchemy import text
from memory.structured.db import engine, save_event
from memory.semantic.vector_store import store_embedding, search_similar
from runtime.utils.signal_hash import generate_signal_hash
from runtime.intelligence.confidence import compute_signal_confidence
from runtime.intelligence.merchant_extractor import extract_merchant, classify_industry, classify_region

from runtime.intelligence.semantic_memory import store_signal_embedding

def process_and_store_signal(source, content):
    signal_hash = generate_signal_hash(content)
    
    with engine.connect() as conn:
        existing = conn.execute(text("""
            SELECT id, canonical_lead_id
            FROM signals
            WHERE signal_hash = :h
            ORDER BY id ASC
            LIMIT 1
        """), {"h": signal_hash}).fetchone()
        if existing:
            save_event("signal_deduplicated", {
                "duplicate_signal_hash": signal_hash,
                "duplicate_of": existing[0],
                "canonical_lead_id": existing[1],
                "source": source,
            })
            return None
            
        similar = search_similar(content, limit=1)
        duplicate_of_id = None
        canonical_id = None
        is_semantic_duplicate = False
        
        if similar and similar[0]["score"] >= 0.92:
            matched_text = similar[0]["text"]
            orig_row = conn.execute(text("SELECT id, canonical_lead_id FROM signals WHERE content = :c ORDER BY id ASC LIMIT 1"), {"c": matched_text}).fetchone()
            if orig_row:
                duplicate_of_id, canonical_id = orig_row[0], orig_row[1]
                is_semantic_duplicate = True

        confidence = compute_signal_confidence(content, source, is_semantic_duplicate)
        merchant_name = extract_merchant(content)
        industry = classify_industry(content, merchant_name)
        region = classify_region(content)
        
        res = conn.execute(text("""
            INSERT INTO signals (source, content, signal_hash, duplicate_of, canonical_lead_id, confidence, merchant_name, industry, region)
            VALUES (:source, :content, :hash, :dup, :can, :conf, :merch, :ind, :reg)
            RETURNING id
        """), {
            "source": source, "content": content, "hash": signal_hash, 
            "dup": duplicate_of_id, "can": canonical_id, "conf": confidence,
            "merch": merchant_name, "ind": industry, "reg": region
        })
        new_signal_id = res.scalar()
        
        if is_semantic_duplicate and canonical_id:
            conn.execute(text("UPDATE canonical_leads SET duplicate_count = duplicate_count + 1 WHERE id = :cid"), {"cid": canonical_id})
            save_event("signal_deduplicated", {"new_signal_id": new_signal_id, "duplicate_of": duplicate_of_id, "canonical_lead_id": canonical_id})
        else:
            can_res = conn.execute(text("INSERT INTO canonical_leads (primary_signal_id, duplicate_count) VALUES (:pid, 0) RETURNING id"), {"pid": new_signal_id})
            canonical_id = can_res.scalar()
            conn.execute(text("UPDATE signals SET canonical_lead_id = :cid WHERE id = :sid"), {"cid": canonical_id, "sid": new_signal_id})
            save_event("canonical_lead_created", {"canonical_lead_id": canonical_id, "primary_signal_id": new_signal_id})
            if merchant_name != "unknown":
                save_event("merchant_detected", {"signal_id": new_signal_id, "merchant": merchant_name, "industry": industry, "region": region})
            
        conn.commit()
        
        try: store_embedding(content)
        except Exception: pass
        
        # --- NEW: Semantic Memory Vector Storage ---
        try:
            store_signal_embedding(new_signal_id, content)
        except Exception as e:
            from config.logging_config import get_logger
            get_logger("ingestion").error(f"Failed semantic memory embedding: {e}")
            
        return new_signal_id
