import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# ============================================================================
# BOOT INTEGRITY CHECKS — MUST RUN BEFORE ANY OTHER IMPORT.
# See runtime/main.py for the full rationale. Briefly: the verifiers
# are stdlib-only and crash boot in `enforce` mode before any
# framework byte loads or any socket has the chance to open.
# ============================================================================
from runtime.safety.fingerprint_check import (
    verify_or_die,
    verify_config_or_die,
    verify_runtime_or_die,
)
verify_or_die("worker")
verify_config_or_die("worker")
verify_runtime_or_die("worker")

import redis
import time
import json
import signal
from memory.structured.db import save_task, save_event
from runtime.dispatcher.router import dispatch
from safety.guard import safe_execute
from safety.rate_limiter import rate_limiter
from tools.web_fetch import web_fetch
from tools.web_search import web_search
from tools.db_query import db_query
from config.logging_config import get_logger
from runtime.health.telemetry import clear_component_fields, heartbeat, record_component_state, utc_now_iso

logger = get_logger("worker")
WORKER_TASK_TIMEOUT_SECONDS = int(os.getenv("AGENT_FLUX_WORKER_TASK_TIMEOUT_SECONDS", "300"))

TOOLS = {
    "web_fetch": web_fetch,
    "web_search": web_search,
    "db_query": db_query,
    "scan_signals": None,
    "fix_pipeline": None,
    "run_sales_cycle": None,
}

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


class WorkerTaskTimeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise WorkerTaskTimeout("worker task exceeded timeout")


def _run_with_timeout(fn, timeout_seconds):
    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(max(1, int(timeout_seconds)))
    try:
        return fn()
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


from runtime.safety.schema_validator import validate_schema
success, msg = validate_schema()
if not success:
    logger.critical(f"WORKER STARTUP BLOCKED: {msg}")
    sys.exit(1)

logger.info("Agent Flux worker started")
print("Agent Flux worker started")
heartbeat("worker", ttl=300, status="starting", queue_depth=r.llen("agent_tasks"))
clear_component_fields("worker", "active_task", "active_task_started_at")

INFLIGHT_KEY = "agent_tasks_inflight"

# LMOVE was introduced in Redis 6.2. The VPS still runs 6.0.x, so fall back
# to a Lua script that atomically replicates `LMOVE src dst LEFT RIGHT` on
# any Redis ≥ 2.6. We register once and reuse the SHA via EVALSHA.
_LMOVE_LUA = """
local v = redis.call('LPOP', KEYS[1])
if v then redis.call('RPUSH', KEYS[2], v) end
return v
"""
_lmove_script = r.register_script(_LMOVE_LUA)


def _detect_lmove() -> bool:
    try:
        r.lmove("__lmove_probe_src__", "__lmove_probe_dst__", "LEFT", "RIGHT")
        return True
    except redis.exceptions.ResponseError as e:
        if "unknown command" in str(e).lower():
            return False
        return True  # any other error means LMOVE itself works


_HAS_LMOVE = _detect_lmove()
logger.info(f"redis LMOVE support: {_HAS_LMOVE}")


def _claim_task():
    if _HAS_LMOVE:
        return r.lmove("agent_tasks", INFLIGHT_KEY, "LEFT", "RIGHT")
    return _lmove_script(keys=["agent_tasks", INFLIGHT_KEY])


while True:
    heartbeat("worker", ttl=300, status="running", queue_depth=r.llen("agent_tasks"))

    # ── At-least-once: move to inflight before processing ───────────
    task = _claim_task()

    if task:
        try:
            try:
                payload_json = json.loads(task)
            except (json.JSONDecodeError, TypeError):
                payload_json = {"raw": task}

            save_task(payload_json)
            logger.info(f"Task logged: {task}")
            print(f"Task logged: {task}")

            save_event("task_received", payload_json)

            # Rate limit check for LLM calls (dispatcher may call Claude)
            decision = dispatch(payload_json)
            logger.info(f"Decision: {decision}")
            print(f"Decision: {decision}")

            save_event("agent_decision", decision if isinstance(decision, dict) else {"raw": str(decision)})

            action = decision.get("action") if isinstance(decision, dict) else None
            record_component_state(
                "worker",
                ttl=300,
                active_task=action or "unknown",
                active_task_started_at=utc_now_iso(),
                last_task_payload=json.dumps(payload_json)[:500],
                queue_depth=r.llen("agent_tasks"),
            )

            if action and action in TOOLS:
                task_input = decision.get("input") or decision.get("notes") or str(payload_json)

                # Safety validation
                try:
                    safe_execute(action, task_input)
                except (ValueError, PermissionError) as e:
                    logger.warning(f"Safety blocked: {e}")
                    print(f"Safety blocked: {e}")
                    save_event("safety_block", {"action": action, "reason": str(e)})
                    # Remove from inflight even on safety block (intentional drop)
                    r.lrem(INFLIGHT_KEY, 1, task)
                    continue

                # Rate limit check
                try:
                    rate_limiter.check("tool_execution")
                except RuntimeError as e:
                    logger.warning(f"Rate limited: {e}")
                    print(f"Rate limited: {e}")
                    save_event("rate_limit", {"action": action, "reason": str(e)})
                    # Remove from inflight — rate limited tasks are intentionally dropped
                    r.lrem(INFLIGHT_KEY, 1, task)
                    continue

                # Execute tool
                def _execute_tool():
                    if action == "scan_signals":
                        from ingestion.merchant_scanner import scan_merchants
                        return scan_merchants()
                    if action == "fix_pipeline":
                        from ops.pipeline_diagnostics import run_pipeline_diagnostics
                        return run_pipeline_diagnostics()
                    if action == "run_sales_cycle":
                        from ops.autonomous_sales import run_autonomous_sales_cycle
                        return run_autonomous_sales_cycle()
                    if TOOLS[action]:
                        rate_limiter.check("external_request")
                        return TOOLS[action](task_input)
                    return {"status": "no_handler"}

                tool_started = time.time()
                try:
                    tool_result = _run_with_timeout(_execute_tool, WORKER_TASK_TIMEOUT_SECONDS)
                except WorkerTaskTimeout:
                    tool_duration = round(time.time() - tool_started, 2)
                    logger.error(f"Worker task timed out: {action} after {tool_duration}s")
                    save_event("worker_timeout", {"action": action, "duration_seconds": tool_duration})
                    record_component_state(
                        "worker",
                        ttl=300,
                        last_timeout_at=utc_now_iso(),
                        last_timed_out_task=action,
                        last_task_duration_seconds=tool_duration,
                    )
                    tool_result = {"error": f"timeout after {WORKER_TASK_TIMEOUT_SECONDS}s"}
                    # Timeout: leave in inflight for alert + replay decision
                    continue
                except Exception as e:
                    tool_result = {"error": str(e)}
                else:
                    tool_duration = round(time.time() - tool_started, 2)
                    record_component_state(
                        "worker",
                        ttl=300,
                        last_completed_task=action,
                        last_task_duration_seconds=tool_duration,
                        last_successful_cycle=utc_now_iso(),
                    )

                logger.info(f"Tool result: {tool_result}")
                print(f"Tool result: {tool_result}")
                save_event("tool_execution", {"action": action, "result": tool_result if isinstance(tool_result, dict) else {"raw": str(tool_result)}})

            # ── SUCCESS: remove from inflight ──
            r.lrem(INFLIGHT_KEY, 1, task)

        except Exception as e:
            logger.error(f"Worker error: {e}")
            print(f"Worker error: {e}")
            try:
                save_event("worker_error", {"error": str(e)})
            except:
                pass
            record_component_state("worker", ttl=300, last_error_at=utc_now_iso(), last_error=str(e))
            # Crash: leave in inflight for alert + replay
        finally:
            clear_component_fields("worker", "active_task", "active_task_started_at")
            record_component_state("worker", ttl=300, queue_depth=r.llen("agent_tasks"))

    else:
        time.sleep(2)

