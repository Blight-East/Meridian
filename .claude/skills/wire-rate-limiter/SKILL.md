---
name: wire-rate-limiter
description: safety/rate_limiter.py is defined but never called. Wire it into the chat engine's LLM call site. Use when fixing cost controls or after a runaway-token incident.
phase: 0
status: draft
blast_radius: low (single call site, fails closed by default)
parallelizable: true
---

# Wire rate limiter

## Why

`safety/rate_limiter.py` defines buckets (`120 tool_executions/hour`,
`60 external_requests/hour`, `60 llm_call_sonnet/hour`,
`10 llm_call_opus/hour`) but **no production code calls it**. Confirmed
via grep: zero call sites in `runtime/conversation/chat_engine.py` where
the actual LLM call happens (line ~1505, `call_anthropic`).

This means:
- Runaway agent loops have no cost ceiling
- Per-user rate limiting doesn't exist
- The safety config looks active but isn't

## Blast radius

Touches only:
- `runtime/conversation/chat_engine.py` — add 2 lines before
  `call_anthropic` / `call_llm` (post-PR #6 this is the provider-adapter
  entry point).
- `safety/rate_limiter.py` — add `RateLimitExceeded` exception class if
  not present.
- `tests/safety/test_rate_limiter_integration.py` — new test file.

Does not touch scheduler, worker, API, or any persistence.

## Preconditions

1. PR #6 (LLM provider adapter) merged. This skill must target the
   post-PR call site, not the pre-PR `call_anthropic`. If PR #6 is
   still open, hold.
2. `payflux-guardrails` rule #16 understood: no new LLM call site
   ships without rate limiting.

## Steps

1. **Read `safety/rate_limiter.py` in full.** Confirm the API:
   `check(key: str, user_id: str | None = None) -> bool` and
   `consume(key: str, user_id: str | None = None) -> bool`.
   → **verify**: `grep -n 'def ' safety/rate_limiter.py` lists the
   expected functions.

2. **Add `RateLimitExceeded` exception** if not present in
   `safety/rate_limiter.py`. Include `bucket: str`, `user_id: str | None`,
   and `retry_after_seconds: int` attributes.
   → **verify**: `python -c "from safety.rate_limiter import RateLimitExceeded"`
   exits 0.

3. **Gate the LLM call in `chat_engine.py`.** Immediately before the
   `call_llm(payload)` invocation (post-PR #6), insert:
   ```python
   bucket = _bucket_for_model(payload.get("model", ""))  # sonnet/opus/haiku
   if not rate_limiter.consume(bucket, user_id=user_id):
       raise RateLimitExceeded(
           bucket=bucket, user_id=user_id, retry_after_seconds=60
       )
   ```
   → **verify**: `grep -n 'rate_limiter.consume' runtime/conversation/chat_engine.py`
   returns at least one match.

4. **Handle `RateLimitExceeded` upstream.** In the Telegram entry
   point (`runtime/telegram_bot.py`) and the autonomous workloop
   (`runtime/ops/agent_workloop.py`), catch `RateLimitExceeded` and
   convert to a user-facing message ("I've hit my per-hour limit —
   try again in Nm") or reschedule the workloop task with backoff.
   → **verify**: integration test in step 5 passes.

5. **Write integration test.** `tests/safety/test_rate_limiter_integration.py`:
   - Spin up the chat engine with a stubbed LLM provider.
   - Call it (10 + bucket_limit) times in a row.
   - Assert the first `bucket_limit` succeed, the rest raise
     `RateLimitExceeded`.
   - Assert the telemetry event `rate_limit_hit` is emitted.
   → **verify**: `pytest tests/safety/test_rate_limiter_integration.py -v`
   green.

6. **Add `rate_limit_hit` as a telemetry event.** Use
   `runtime/ops/tool_telemetry.record_tool_call` (introduced in 946eb69)
   with `ok=False, error='rate_limit'`. This lets the Prom exporter
   (skill `prom-exporter-on-telemetry`) pick it up automatically.
   → **verify**: a DB query `SELECT count(*) FROM events WHERE event_type='tool_invoked' AND data->>'error'='rate_limit'`
   returns ≥1 after test run.

## Rollback

Single-commit revert. The exception class left in place is harmless.
No schema change, no config migration.

## Exit criterion

- All tests in step 5 passing in CI.
- Deployed to prod. Manual smoke: trigger the bucket limit via a test
  operator account; confirm (a) the user sees a friendly rate-limit
  message, (b) a `rate_limit_hit` event lands in `events` table,
  (c) subsequent calls still work after the bucket refills.
- Removes audit risk **R5** ("Rate limiter stubbed, not wired").

## Notes for the implementer

- Don't refactor `rate_limiter.py`. If buckets need tuning, do that in
  a separate PR. Only wire it.
- The `_bucket_for_model` helper is tiny; inline it if it makes the
  diff smaller.
- Anthropic vs NIM: after PR #6, rate limits should probably apply per
  *effective provider*, not per model alias. Bucket naming stays
  model-alias based (`llm_call_sonnet`) for continuity, but this is
  an intentional simplification — revisit in phase 3 if it matters.
