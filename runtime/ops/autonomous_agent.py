import sys, os, time, redis, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from runtime.ingestion.merchant_scanner import scan_merchants
from runtime.ranking.run_ranking import rank_signals
from runtime.ranking.signal_clusterer import cluster_signals
from runtime.ranking.trend_detector import detect_trends
from runtime.ranking.opportunity_extractor import extract_opportunities
from runtime.qualification.run_qualification import run_qualification
from runtime.investigation.run_investigation import run_investigation
from memory.structured.db import save_event

# Operator logging
op_logger = logging.getLogger("operator")
_log_dir = os.environ.get("AGENT_FLUX_LOG_DIR", "/tmp/agent-flux/logs")
os.makedirs(_log_dir, exist_ok=True)
op_handler = logging.FileHandler(os.path.join(_log_dir, "operator.log"))
op_handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] [%(levelname)s] %(message)s"))
if not op_logger.handlers: op_logger.addHandler(op_handler)
op_logger.setLevel(logging.INFO)

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def autonomous_market_cycle():
    op_logger.info("Starting autonomous market intelligence cycle")
    save_event("autonomous_cycle_started", {})
    t = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    r.set("last_scan_time", t)
    
    try:
        op_logger.info("Running scan_merchants")
        scan_merchants()
        op_logger.info("Running rank_signals")
        rank_signals()
        op_logger.info("Running cluster_signals")
        cluster_signals()
        op_logger.info("Running detect_trends")
        detect_trends()
        op_logger.info("Running extract_opportunities")
        extract_opportunities()
        op_logger.info("Running run_qualification")
        run_qualification()
        op_logger.info("Running run_investigation")
        run_investigation()
        
        save_event("autonomous_cycle_completed", {"time": t})
        op_logger.info("Autonomous cycle completed successfully")
    except Exception as e:
        op_logger.error(f"Error during autonomous cycle: {e}")
        save_event("autonomous_cycle_error", {"error": str(e)})

if __name__ == "__main__":
    autonomous_market_cycle()
