---
name: payflux-guardrails
description: Hard rules for any work in this repo. Behavioral, not procedural. Read at the start of any PayFlux/Meridian task.
phase: always
status: shipped
blast_radius: n/a
---

# PayFlux / Meridian Guardrails

These are non-negotiable. If a task seems to require breaking one, **stop and
surface the conflict** — don't silently comply. Derived from CLAUDE.md +
audit findings + incident history.

## Approval & outreach

1. **Gmail/email outreach is approval-gated.** Never flip `approval_required`
   to auto-send. `runtime/channels/policy.py` is the gate; every path to
   `gmail_adapter.py` must route through it. Drafts only until operator
   approves via Telegram.
2. **No autonomous money movement, no auto-send to merchants.** Human
   approval blocks even in `/loop` / autonomous mode. See CLAUDE.md
   autonomous-mode carve-out.
3. **`auto_send_high_confidence` mode must stay off in prod** until phase 0
   `remove-auto-send-path` has been either shipped or explicitly deferred
   by the operator.

## Code tree

4. **`runtime/` is canonical.** Do not add new code to `deploy/`. If you
   find logic only in `deploy/`, migrate it to `runtime/` or flag it in
   `docs/WORK_IN_FLIGHT.md`.
5. **Migrations live in `deploy/migrations/*.sql`.** Until phase 1
   `unify-tree` lands, this is the one deploy/ directory that stays.

## Signal & opportunity pipeline

6. **`noise_system` and `test_thread` rows are filtered** at
   `runtime/reasoning/control_plane.py`. Never bypass the filter in
   classifier, trigger engine, or outreach.
7. **Consumer complaints are down-ranked** in
   `runtime/intelligence/merchant_signal_classifier.py`. Preserve the bias.
8. **Merchant-side intent wins ties.** When a signal could be either
   merchant-operator or consumer, the classifier requires
   `merchant_score >= consumer_score + 2` to flip. Don't loosen this.

## State & truth

9. **One owner per truth.** If you find yourself writing the same field
   (name, domain, processor, email) to two tables, stop and use a FK
   instead. Drift will bite.
10. **Before reporting "X succeeded," verify durable state.** The
    2026-03-14 incident: Telegram said "scan started" without confirming
    Redis enqueue. Check the database or the queue; don't trust the
    function that just ran.

## Errors & observability

11. **Every `except Exception` must emit a metric.** Silent-logged errors
    are invisible errors. Use `save_event()` or `record_tool_call()` so
    the failure appears in `tool_invoked` telemetry.
12. **Respect the 300s scheduler task budget.** Any single-merchant /
    single-item operation that could exceed ~30s needs a per-item
    deadline. Precedent: `contact_discovery` fix (commit 5d02dd5).
13. **Advisory lock schema changes.** DDL against prod goes through
    `runtime/ops/schema_migrations.py` (introduced in b0eed03). No raw
    `ALTER TABLE` from application code outside migrations.

## Cost & loops

14. **Max tool rounds: 3** (specialist agents: up to 5 for Investigator
    only). Enforce in the LLM wrapper, not the prompt.
15. **Per-run token cap.** No single user turn should exceed 40K input /
    8K output. Gate in the wrapper.
16. **Wire the rate limiter before shipping new LLM call sites.**
    `safety/rate_limiter.py` exists; `wire-rate-limiter` skill makes it
    active. Don't add a new call site that bypasses it.

## Coordination

17. **Multiple AI agents write to this repo.** Check
    `docs/WORK_IN_FLIGHT.md` before starting; update it when you begin.
    If Devin (or another agent) is in the same file, coordinate before
    stomping.
18. **Rotate keys pasted in chat.** If API keys appear in any
    transcript — yours, mine, the user's — treat as compromised and
    rotate immediately. Never commit keys; use env vars.

## If these conflict with a user request

Stop. Name the conflict. Ask. Don't resolve silently.
