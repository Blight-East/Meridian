# Channel Action Layer (Phase 1)

## 1. Architecture Summary

Phase 1 adds a conservative cross-channel action layer for Gmail and Reddit on top of the existing Agent Flux runtime.

- `runtime/channels/base.py`
  - shared adapter dataclasses and interface
- `runtime/channels/store.py`
  - channel settings, audit log, journal/idempotency, queue metrics
- `runtime/channels/policy.py`
  - mode gating, allowlists, deny/kill switches, cooldowns, duplicate checks, rate limits
- `runtime/channels/gmail_adapter.py`
  - official Gmail API read/draft/send plus label management and optional Drive upload
- `runtime/channels/reddit_adapter.py`
  - official Reddit OAuth search/read/reply/edit
- `runtime/channels/service.py`
  - operator-facing orchestration, telemetry, audit-first send flow

The policy engine is authoritative. Confidence is advisory only. The model does not promote its own mode; only explicit operator commands can move a channel between `dry_run`, `approval_required`, and `auto_send_high_confidence`.

## 2. Files Added / Changed

Added:

- `runtime/channels/__init__.py`
- `runtime/channels/base.py`
- `runtime/channels/store.py`
- `runtime/channels/templates.py`
- `runtime/channels/policy.py`
- `runtime/channels/gmail_adapter.py`
- `runtime/channels/reddit_adapter.py`
- `runtime/channels/service.py`
- `CHANNEL_ACTION_LAYER.md`

Changed:

- `runtime/ops/operator_commands.py`
- `runtime/conversation/chat_engine.py`
- `runtime/main.py`

## 3. Env Vars Required

Shared:

- `CHANNEL_ACTION_LAYER_ENABLED=true`
- `REDDIT_CHANNEL_ENABLED=true`
- `GMAIL_CHANNEL_ENABLED=true`

Reddit:

- `REDDIT_CLIENT_ID`
- `REDDIT_CLIENT_SECRET`
- `REDDIT_REFRESH_TOKEN`
- `REDDIT_USER_AGENT`
- `REDDIT_ACCOUNT_USERNAME`
- `REDDIT_ALLOWLIST`
- `REDDIT_DENYLIST`
- `REDDIT_CHANNEL_MODE=approval_required`
- `REDDIT_CHANNEL_MIN_CONFIDENCE=0.82`
- `REDDIT_CHANNEL_AUTO_SEND_CONFIDENCE=0.9`
- `REDDIT_CHANNEL_COOLDOWN_HOURS=72`
- `REDDIT_MAX_REPLIES_PER_HOUR=3`
- `REDDIT_MAX_REPLIES_PER_SUBREDDIT_PER_DAY=5`

Gmail / Workspace:

- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REFRESH_TOKEN`
- `GMAIL_SENDER_EMAIL`
- `GMAIL_SENDER_NAME`
- `GOOGLE_DRIVE_AUDIT_FOLDER_ID` (optional)
- `GMAIL_CHANNEL_MODE=approval_required`
- `GMAIL_CHANNEL_MIN_CONFIDENCE=0.72`
- `GMAIL_CHANNEL_AUTO_SEND_CONFIDENCE=0.85`
- `GMAIL_CHANNEL_COOLDOWN_HOURS=72`
- `GMAIL_MAX_SENDS_PER_DAY=20`
- `GMAIL_DENYLIST`

## 4. OAuth / Scopes Required

Reddit:

- `identity`
- `read`
- `history`
- `submit`
- `edit`
- `privatemessages` only if inbox read is explicitly enabled later

Gmail:

- `https://www.googleapis.com/auth/gmail.readonly`
- `https://www.googleapis.com/auth/gmail.send`
- `https://www.googleapis.com/auth/gmail.modify`
- `https://www.googleapis.com/auth/gmail.labels`
- `https://www.googleapis.com/auth/drive.file` only if Drive artifact upload is enabled

Do not request broad Admin SDK or full-domain scopes in Phase 1.

## 5. Install / Setup Steps

1. Install the Workspace CLI:
   - `npm install -g @googleworkspace/cli`
2. Add the env vars above to `.env`.
3. Set both channels to `approval_required`.
4. Start with:
   - Reddit allowlist only
   - Gmail inbox/label review only
5. For Gmail:
   - create OAuth client
   - obtain refresh token for the mailbox
   - ensure labels exist: `bot-draft`, `bot-sent`, `bot-needs-review`, `bot-replied`, `bot-do-not-contact`
6. For Reddit:
   - create OAuth app
   - use refresh-token flow
   - start with a short allowlist of low-risk subreddits

## 6. Operator Workflow

Reddit:

1. `list_reddit_candidates`
2. `draft_reddit_reply <target>`
3. review confidence, rationale, and subreddit
4. `send_reddit_reply <target>` only after approval

Gmail:

1. `list_gmail_threads`
2. `draft_gmail_reply <thread>`
3. review subject/body/rationale/confidence
4. `send_gmail_reply <thread>` only after approval

Audit / controls:

- `show_channel_audit_log`
- `set_channel_mode <reddit|gmail> <dry_run|approval_required|auto_send_high_confidence>`
- `set_channel_kill_switch <reddit|gmail> <true|false>`

## 7. Safety Guardrails

- Official APIs only
- All outbound actions are audited
- `dry_run` and `approval_required` are the defaults
- Human approval is required unless the operator explicitly sets `auto_send_high_confidence`
- Confidence is numeric `0.0-1.0` and advisory only
- Hostile, moderation-sensitive, or denylisted Reddit threads are blocked
- Gmail respects `bot-do-not-contact` and denylist recipients
- Duplicate attempts are logged as `skipped_duplicate`
- Reddit allows at most one proactive reply per thread unless explicitly overridden
- Rate limits are enforced before send
- Kill switch exists per channel and globally
- Only bot-authored Reddit replies may be edited
- Gmail send path uses in-thread replies only; no mass send path is enabled here

## 8. Test Plan

Unit / local:

- adapter import and syntax checks
- policy tests for:
  - dry-run block
  - approval-required gate
  - duplicate prevention
  - cooldown enforcement
  - allowlist / denylist enforcement

Sandbox / integration:

- Reddit:
  - test subreddit only
  - list threads, read context, draft reply, send one approved reply, edit bot-authored reply
- Gmail:
  - test workspace mailbox
  - list inbox, read thread, ensure labels, draft reply, send approved in-thread reply
- Audit:
  - verify audit log row for every draft/send/blocked/duplicate
- Telemetry:
  - verify `reddit_status`, `gmail_status`, `approval_queue_depth`, `action_queue_depth`, `last_action_sent_at`

## 9. Phased Rollout Plan

Phase 1:

- Gmail and Reddit integrations installed
- official APIs only
- `approval_required` by default
- dry-run / draft-first operator workflow
- monitor audit log and metrics

Phase 2:

- Gmail only
- enable `auto_send_high_confidence` for narrow allowlisted cases
- keep Reddit on approval

Phase 3:

- Reddit high-confidence auto-reply only for allowlisted communities and low-risk trigger classes

## Safe Enablement Notes

### How to enable Reddit safely

- start with 2-3 allowlisted subreddits
- keep `REDDIT_CHANNEL_MODE=approval_required`
- set low reply caps
- do not enable DMs in Phase 1

### How to enable Gmail safely

- start with one test mailbox
- keep `GMAIL_CHANNEL_MODE=approval_required`
- require `bot-do-not-contact` handling before any send
- stay well below Google send quotas

### How to prevent spam / double-send

- idempotency key per action target + body
- per-channel cooldowns
- journaled proactive-thread limits
- audit row for every duplicate skip
- hard rate limits before send

### How to disable instantly

- set `CHANNEL_ACTION_LAYER_ENABLED=false` and restart impacted services
- or set `REDDIT_CHANNEL_ENABLED=false`
- or set `GMAIL_CHANNEL_ENABLED=false`
- or use `set_channel_kill_switch reddit true`
- or use `set_channel_kill_switch gmail true`
