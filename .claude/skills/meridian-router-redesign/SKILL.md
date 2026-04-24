---
name: meridian-router-redesign
description: Break the single god-agent into a router + specialist subagents with scoped tools. Phase 3. Use after phase 0-1 stabilization and when per-turn LLM cost needs to drop.
phase: 3
status: draft
blast_radius: high (touches chat_engine + every tool registration)
parallelizable: false (foundational for other phase 3 work)
---

# Meridian router redesign

## Why

`runtime/conversation/chat_engine.py:1374` `generate_chat_response` is
a single-turn reasoning loop that hands **all 130 tools** to the LLM
(`chat_engine.py:1509 ALLOWED_TOOLS`). Consequences:

- **Cost.** Tool definitions alone consume ~8K tokens *per call*.
  Multiply by autonomous-loop cadence and you have a perpetual tax.
- **Quality.** The LLM picks from 130 — often confused, often wrong.
  No embedding-based or heuristic tool selection.
- **Debug.** When the wrong tool is called, there's no trail showing
  *which subset* the model considered.

Also:
- `MAX_TOOL_ROUNDS = 3` is a blunt instrument. Some tasks (investigation)
  legitimately need more. Others (acks) need 0.
- Memory is a 10-message window with no consolidation.
- No per-turn cost tracking.

Audit risks **R4** (130 tools), partial fix to runaway loops, and the
foundation for cost sanity.

## Blast radius

- `runtime/conversation/chat_engine.py` — major refactor. Single
  `generate_chat_response` function splits into:
  - `route(user_message, context) -> Specialist`
  - Specialist-specific execution loops.
- **New modules:** `runtime/reasoning/specialists/` with one file per
  specialist.
- **New tool registry:** `runtime/reasoning/tool_registry.py` with
  `.scope(specialist_name) -> list[Tool]`.
- **New cost ledger:** `agent_cost_ledger` table.
- Every tool — metadata annotated with the specialist(s) that use it.

Does not touch: ingestion, approval gate, DB schema beyond the ledger
table.

## Preconditions

1. Phase 0 and phase 1 complete.
2. [PR #6 LLM provider adapter](https://github.com/Blight-East/Meridian/pull/6)
   merged. This skill targets the post-PR `call_llm` interface.
3. Budget for a ~2-week focused effort. This is the largest phase 3
   skill.

## Design

### Specialists (initial set — 4 + fallback)

| Specialist | Model tier | Tools (count) | Max tool rounds | Primary use |
|---|---|---|---|---|
| **Router** | Haiku | 0 (classification only) | 0 | Intent → specialist |
| **MarketAnalyst** | Sonnet | 8 | 3 | Signal/cluster/opportunity queries |
| **OutreachDrafter** | Sonnet | 12 | 3 | Draft + revise outreach, respect approval gate |
| **Investigator** | Sonnet (→ Opus on low confidence) | 15 | 5 | Merchant deep-dives, contact discovery |
| **OpsAgent** | Haiku | 6 (read-only) | 2 | Status, heartbeat, briefing |
| **Generalist (fallback)** | Sonnet | top-20 tools by embedding match | 3 | When router can't classify |

### Tool scoping

New attribute on every tool registration: `specialists: tuple[str, ...]`.
`tool_registry.scope("MarketAnalyst")` returns only tools that list
`"MarketAnalyst"`. A tool can belong to multiple specialists (e.g.
`get_merchant_profile` used by all four).

### Routing

`route()` takes the user message + last-3 turns summary, produces a
Haiku classification: one of {MarketAnalyst, OutreachDrafter,
Investigator, OpsAgent, Generalist}. Haiku prompt is ~200 tokens; call
latency ~200ms. 40x cheaper than routing via Sonnet.

### Cost ledger

```sql
CREATE TABLE agent_cost_ledger (
  id BIGSERIAL PRIMARY KEY,
  turn_id UUID NOT NULL,
  user_id TEXT,
  specialist TEXT NOT NULL,
  model TEXT NOT NULL,           -- 'sonnet'/'opus'/'haiku' or NIM equiv
  input_tokens INT NOT NULL,
  output_tokens INT NOT NULL,
  tool_calls INT NOT NULL,
  wall_clock_ms INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Each LLM call writes one row. Enables "what does Meridian cost per
turn / per user / per specialist?" queries.

### Memory layers (see audit §4)

| Layer | Store | Scope | Lifecycle |
|---|---|---|---|
| Working | in-turn Python context | single turn | discard on turn end |
| Session | `conversations` | per user_id | rolling window N=20 |
| Episodic | new `agent_episodes` table | per task_id | 30d, pruned if unreferenced |
| Semantic | `signal_embeddings` + new `operator_knowledge` | cross-session | consolidator merges weekly |
| Identity | `operator_profile`, `merchant_profile` (exist) | durable | only named write tools, no LLM free-text |

### Consolidator + Adversary + Judge (weekly cron, NOT per-turn)

Run as a scheduler task:
- **Consolidator** (Haiku): summarizes last 7 days of episodic into
  candidate semantic facts.
- **Adversary** (Haiku): for each candidate, searches episodic/session
  for contradicting evidence. Flags conflicts.
- **Judge** (Sonnet, only on conflicts): resolves or discards.

Debate pattern skipped — too expensive, marginal gain at this scale.

## Steps

1. **Tool-registry refactor.** Annotate every tool with its specialist
   set. Ship as a non-breaking PR (registry grows an attribute; old
   call sites keep working until the router PR lands).
   → **verify**: every tool has a non-empty `specialists` tuple.

2. **Cost ledger migration.** New `agent_cost_ledger` table.
   → **verify**: `\d agent_cost_ledger` shows the schema.

3. **Router skeleton.** `runtime/reasoning/router.py` with Haiku call
   + classification. Fallback to `Generalist` when uncertain.
   → **verify**: unit test with 20 sample messages; ≥15 classified
   correctly.

4. **Specialist skeletons.** One file per specialist in
   `runtime/reasoning/specialists/`. Shared base class with the loop,
   tool-round cap, token cap, cost-ledger write.
   → **verify**: each specialist can answer a trivial prompt end-to-end
   in isolation.

5. **Wire into `chat_engine.generate_chat_response`.** Replace
   monolith with:
   ```python
   specialist = route(message, context)
   return specialist.run(message, context, user_id)
   ```
   → **verify**: existing integration tests still pass; new tests
   per-specialist pass.

6. **Cost measurement.** Run 100 synthetic turns before/after. Target:
   ≥40% reduction in input tokens per turn. If <20%, stop — the
   specialist split isn't tight enough; revisit tool scoping.
   → **verify**: comparison table in PR description.

7. **Memory layer migration.** Add `agent_episodes` table. Implement
   consolidator/adversary/judge as a scheduler task
   (`run_memory_consolidator`, weekly).
   → **verify**: consolidator runs once in staging; produces reasonable
   semantic facts (operator spot-check).

8. **Session rotation.** Add `session_id` to `conversations`. Router
   detects topic-delta via embedding similarity threshold; new
   `session_id` starts fresh working memory but inherits
   semantic+identity.
   → **verify**: conversation smoke test — user switches topic
   mid-thread; new session_id issued.

9. **Remove MAX_TOOL_ROUNDS = 3 constant.** Replace with per-specialist
   cap (3 default, 5 for Investigator, 0 for Router, 2 for Ops).
   → **verify**: Investigator test uses 5 rounds on a genuinely deep
   query.

10. **Decommission the old single-agent path.** Leave a feature flag
    `MERIDIAN_LEGACY_SINGLE_AGENT=true` for 2 weeks so we can flip
    back. Then delete.
    → **verify**: flag default off for 14 days; no incidents; delete
    the legacy code.

## Rollback

- Flip `MERIDIAN_LEGACY_SINGLE_AGENT=true`. Old path resumes.
- `agent_cost_ledger` table is append-only; can stay.
- Specialists directory can remain unused.

## Exit criterion

- 40%+ reduction in mean input tokens per turn (measured via cost
  ledger).
- Specialist routing is correct ≥85% on the 20-sample test.
- Consolidator runs weekly, produces ≥5 semantic facts per week with
  ≥80% operator-accepted rate.
- Per-turn wall clock < 90s on ≥95% of turns.
- Investigator reaches 5 tool rounds only when confidence < 0.6.
- Audit risk R4 cleared.

## Notes

- **Don't ship the consolidator per-turn.** That's the cost-disaster
  pattern. Consolidation is a batch operation on off-hours.
- **Start with 4 specialists; resist the urge to add more.** Every
  specialist is a new prompt-debugging surface. MarketAnalyst +
  OutreachDrafter + Investigator + OpsAgent covers 80% of workflows.
  Add only when a genuine fifth use case emerges.
- **Don't rewrite prompts during the split.** Keep specialist prompts
  as close to the existing single-agent prompt subsets as possible,
  for comparable performance. Prompt tuning is a follow-up PR.
