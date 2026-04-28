import sys
sys.path.insert(0, "/opt/agent-flux")
from sqlalchemy import text

from memory.structured.db import engine

with engine.connect() as conn:
    print("Inserting mock cluster...")
    try:
        conn.execute(text("""
            INSERT INTO clusters (cluster_topic, cluster_size, trend_change, trend_status)
            VALUES ('Stripe account freezes', 30, 25.0, 'spiking')
        """))
        conn.commit()
    except Exception as e:
        print("Cluster already exists or error:", e)

from runtime.ops.cluster_investigator import run_cluster_investigation
print("Running investigator...")
run_cluster_investigation()
