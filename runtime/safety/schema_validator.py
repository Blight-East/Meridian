import os
import sys
from sqlalchemy import create_engine, text
from config.logging_config import get_logger

logger = get_logger("schema_validator")

# Critical columns that MUST exist for the system to function correctly
REQUIRED_SCHEMA = {
    "signals": ["classification"],
    "clusters": ["signal_ids"],
    "merchant_contacts": ["contact_name"]
}

def validate_schema():
    """
    Validates that critical database columns exist.
    Returns (True, "OK") if valid, (False, "Error message") otherwise.
    """
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres@127.0.0.1/agent_flux")
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            missing = []
            for table, columns in REQUIRED_SCHEMA.items():
                # Query information_schema for existing columns in this table
                query = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = :table
                """)
                res = conn.execute(query, {"table": table})
                existing_columns = {r[0] for r in res.fetchall()}
                
                for col in columns:
                    if col not in existing_columns:
                        missing.append(f"{table}.{col}")
            
            if missing:
                error_msg = f"CRITICAL SCHEMA MISMATCH: Missing columns: {', '.join(missing)}"
                logger.critical(error_msg)
                return False, error_msg
            
            logger.info("Database schema validation successful.")
            return True, "OK"
            
    except Exception as e:
        error_msg = f"SCHEMA VALIDATION FAILED (Connection Error): {str(e)}"
        logger.error(error_msg)
        return False, error_msg

if __name__ == "__main__":
    success, msg = validate_schema()
    if not success:
        print(msg)
        sys.exit(1)
    print("Schema is valid.")
