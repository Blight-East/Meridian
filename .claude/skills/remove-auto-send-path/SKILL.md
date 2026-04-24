---
name: remove-auto-send-path
description: Kill the auto_send_high_confidence mode in channels/policy.py. Use to remove the one-env-var bypass of the approval gate that could burn the Gmail domain.
phase: 0
status: draft
blast_radius: low (dead-code removal, feature-flagged off in prod today)
parallelizable: true
---

# Remove auto-send path

## Why

`runtime/channels/policy.py:61` reads mode from settings with a default of
`approval_required`, but also supports `auto_send_high_confidence` mode
(`policy.py:126-129`). That mode is enabled by setting
`GMAIL_CHANNEL_MODE=auto_send_high_confidence` in the environment.

Risks:
1. **One config change burns your domain.** If the env var gets flipped by
   accident (typo, copy-paste, misread docs), every pending draft sends
   without operator approval. Gmail classifies pattern-matched cold
   outreach as spam after a relatively small volume; domain reputation
   recovers slowly.
2. **CLAUDE.md hard rule says "Never flip approval_required to auto-send."**
   Having the code path present is an invitation.
3. **This business motion isn't using it.** Per the audit, cold-outbound
   is NOT the acquisition channel — the free risk report (phase 2) is.
   Dead code.

Audit risk **R3**.

## Blast radius

- `runtime/channels/policy.py` — remove the `auto_send_high_confidence`
  branch.
- `runtime/channels/store.py` — remove the env-var lookup for the mode
  if it's only used in policy.
- `runtime/channels/service.py` — remove any caller that passes
  `mode=auto_send_high_confidence`.
- Tests that reference the mode.

Does not touch: Gmail adapter, Telegram approval, Meridian reasoning.

## Preconditions

1. **Confirm no current reliance in prod.** Check production env:
   `ssh 159.65.45.64 "grep GMAIL_CHANNEL_MODE /opt/agent-flux/.env"`.
   Expected result: unset, or `approval_required`. If
   `auto_send_high_confidence` is set, **stop and ask** — someone
   intentionally enabled it.
2. Operator sign-off: cold auto-send will not be reintroduced without
   explicit re-design (not just a revert).

## Steps

1. **Grep every call site** touching the mode:
   ```
   grep -rn 'auto_send_high_confidence' runtime/ tests/ deploy/
   grep -rn 'GMAIL_CHANNEL_MODE' runtime/ deploy/
   ```
   → **verify**: list all files; include findings in the PR description.

2. **Remove the branch in `policy.py`.** Replace the mode dispatcher
   with a hard `approval_required` path. Keep the function signature
   so callers don't break; just always return the approval-gated
   decision.
   → **verify**: `grep auto_send_high_confidence runtime/channels/policy.py`
   returns nothing.

3. **Remove the mode-setter in `store.py`.** If `store.py` reads the env
   var and persists the mode, remove that code path.
   → **verify**: same grep returns nothing in store.py.

4. **Audit `service.py`** for callers that pass a mode argument. If any
   call site sets `mode=auto_send_high_confidence`, remove it — they'll
   fall back to approval.
   → **verify**: test suite still passes.

5. **Update tests.** Remove tests that assert the auto-send path works;
   add one test asserting that even with `GMAIL_CHANNEL_MODE` set to
   the old value, the gate still fires.
   → **verify**: `pytest tests/channels/` green.

6. **Update docs.** If `CHANNEL_ACTION_LAYER.md` references the auto-send
   mode, remove the reference. Add a note: "Cold auto-send is
   deliberately absent. If reintroduced in the future, it requires a
   separate design review."
   → **verify**: `grep auto_send_high_confidence CHANNEL_ACTION_LAYER.md`
   returns nothing.

7. **Telemetry check.** Emit a `policy_legacy_mode_seen` event if the
   env var is still set at startup (someone left it in `.env`). This
   lets you catch and clean up stray configs without rollback risk.
   → **verify**: startup log shows warning if env var is set.

## Rollback

Single revert. The feature flag in env is harmless either way after
revert.

## Exit criterion

- `grep -r auto_send_high_confidence` in the repo returns only the
  "deliberately absent" note and this skill.
- Tests green.
- 7 days in prod with no `policy_legacy_mode_seen` events (or, if
  present, operator has cleaned up `.env`).
- Audit risk R3 cleared.

## Notes

- **Not the same as disabling outreach.** Operator-approved outreach
  via Telegram still works identically. You're only removing the path
  where no human has to click approve.
- **Post-sale drip/nurture sequences** (once you have customers) will
  need a different mechanism — sequenced sends to already-consented
  contacts. That's a separate design, not a revival of this code path.
