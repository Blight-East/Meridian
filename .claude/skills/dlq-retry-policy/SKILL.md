---
name: dlq-retry-policy
description: Automate the drain of agent_tasks_inflight → agent_tasks_dlq with exponential backoff retries. Use when tasks are observed stuck or after a worker crash.
phase: 0
status: draft
blast_radius: medium (new background loop, touches worker + scheduler)
parallelizable: true
---

# DLQ retry policy

## Why

`runtime/worker.py:205-231` — when a task raises an exception, it is
logged but **not removed from `agent_tasks_inflight`**. There is no
auto-retry, no dead-letter move, no operator alert. Tasks rot until
someone runs `drain_inflight.py` by hand.

The 2026-03-14 incident surfaced this: worker down for hours, tasks
accumulating silently, Telegram still reporting "scan started" because
nothing checked durable state.

Audit risk **R1** (stuck tasks) and **R8** (blind to outages, partial).

## Blast radius

- **New module** `runtime/ops/dlq.py` — retry loop + classifier.
- **Worker** `runtime/worker.py` — on exception, annotate + push to
  retry queue instead of leaving in inflight.
- **Scheduler** `runtime/scheduler/cron_tasks.py` — add
  `run_dlq_reaper` every 2 minutes.
- **Existing** `deploy/drain_inflight.py` — keep as manual escape hatch;
  this skill automates the routine case.

Does not touch: DB schema, chat engine, approval gate, outreach.

## Preconditions

1. `apply-db-hardening-prod` applied (for the session watchdog — helps
   classify "stuck task" vs "slow DB").
2. `prom-exporter-on-telemetry` either in progress or planned — DLQ
   metrics are most useful with Grafana visibility.

## Design summary

Three Redis queues:
- `agent_tasks` — normal FIFO (existing)
- `agent_tasks_inflight` — claimed-but-not-completed (existing)
- `agent_tasks_dlq` — abandoned (existing, currently unused by code)

New queue:
- `agent_tasks_retry` — failed, eligible for retry with `retry_after`
  timestamp

Retry policy:
- Attempt 1 → 1 min backoff
- Attempt 2 → 5 min
- Attempt 3 → 30 min
- Attempt 4 → 2 hr
- Attempt 5 → DLQ + operator alert

## Steps

1. **Augment task payload shape.** Add `attempts: int` and
   `last_error: str` fields; default 0 and empty. Back-compat: missing
   fields = attempts 0.
   → **verify**: unit test that old-shape tasks still process.

2. **Modify worker on-exception path (`worker.py:205-231`).** Instead
   of `continue`:
   ```python
   task["attempts"] = task.get("attempts", 0) + 1
   task["last_error"] = f"{type(e).__name__}: {e}"
   if task["attempts"] >= 5:
       redis.lpush("agent_tasks_dlq", json.dumps(task))
       send_operator_alert(...)
   else:
       task["retry_after"] = now + BACKOFF[task["attempts"]]
       redis.zadd("agent_tasks_retry", {json.dumps(task): task["retry_after"]})
   redis.lrem(INFLIGHT_KEY, 1, original_payload)
   ```
   → **verify**: kill a worker mid-execution of a tool; observe task
   move to retry queue, then to DLQ after 5 attempts.

3. **Write `runtime/ops/dlq.py`** with:
   - `run_dlq_reaper()`: zrange tasks with score ≤ now, move back to
     `agent_tasks`.
   - `classify_error(last_error: str) -> Literal['retryable',
     'permanent']`: permanent errors (KeyError, AssertionError, schema
     errors) skip retry and go straight to DLQ. Retryable (ConnectionError,
     TimeoutError, HTTP 5xx) follow the backoff.
   → **verify**: `pytest tests/ops/test_dlq.py` green.

4. **Register `run_dlq_reaper` in scheduler.**
   `runtime/scheduler/cron_tasks.py` TASK_REGISTRY: every 2 min.
   → **verify**: `pm2 logs agent-flux-scheduler` shows reaper firing.

5. **Operator alert on DLQ.** When a task hits DLQ, emit
   `send_operator_alert()` with task_id, merchant_id (if present),
   attempts, last error. Dedupe by task signature to avoid alert
   storms.
   → **verify**: force a permanent error; operator receives exactly
   one Telegram alert.

6. **Expose metrics.** `dlq_depth` (gauge), `retry_queue_depth` (gauge),
   `task_retries_total{bucket="1m|5m|30m|2h"}` (counter),
   `task_abandoned_total` (counter). Wire via
   `prom-exporter-on-telemetry` skill.
   → **verify**: `curl /metrics/prom | grep dlq_depth` returns a value.

7. **Manual inspection command.** Add `deploy/drain_inflight.py --dry-run`
   or a small Telegram slash command `/dlq` (wire via the
   slash_commands.py from 946eb69) to list current DLQ contents.
   → **verify**: `/dlq` in Telegram returns task list.

## Rollback

- Revert worker.py change — tasks stop moving to retry queue on
  failure; return to old "stuck in inflight" behavior.
- Disable `run_dlq_reaper` in scheduler — reaper stops; retry queue
  stops draining.
- Existing tasks in retry queue can be drained manually with
  `deploy/drain_inflight.py`.

No schema change to revert.

## Exit criterion

- Chaos test: kill worker mid-task, verify (a) task moves to retry,
  (b) retries fire at 1m, 5m, 30m, 2h, (c) hits DLQ on 5th failure,
  (d) operator alert fires exactly once.
- `dlq_depth` Prom metric visible in Grafana.
- 7 days in prod with no tasks in `agent_tasks_inflight` older than
  60s (transient claims only).
- Audit risks R1 and partial-R8 cleared.

## Notes

- **Idempotency requirement.** Tasks become retry-safe only if the
  underlying tool is idempotent. Tools that aren't (e.g. "send email")
  must check state before acting. This is already true for outreach
  via the approval gate; other tools may need a dedup key.
- **DLQ is not a purgatory.** Operators should drain + investigate DLQ
  entries; a `/dlq clear` command that archives to DB (not deletes) is
  a future enhancement.
