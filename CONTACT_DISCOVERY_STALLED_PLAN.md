# CONTACT_DISCOVERY_STALLED_PLAN

## 1. Current Observed Behavior
- **Pipeline Status**: `fix_pipeline` reports `contact_discovery_stalled`.
- **Operational Status**: System is otherwise healthy; scheduler is firing, signals are being ingested, and merchants are being created/clustered.
- **Missing Outcome**: Valid contact emails with sufficient confidence are not appearing in `merchant_contacts` for new opportunities, preventing automated outreach.

## 2. Exact Execution Path
1. **Signal Ingestion**: `merchant_scanner.py` finds intent on Reddit/Brave.
2. **Merchant Attribution**: `merchant_attribution.py` maps signals to a `merchant_id`.
3. **Opportunity Creation**: `opportunity_trigger_engine.py` creates a record in `merchant_opportunities` status `pending_review`.
4. **Contact Discovery JOB**: `worker.py` (or scheduler) should trigger `contact_discovery.py`.
5. **Contact Scoring**: Discovery results are scored via `confidence.py`.
6. **Outreach Eligibility**: `autonomous_sales.py` checks for `mc.confidence >= 0.8` and `is_outreach_allowed`.

**Primary Suspect Location**: Step 4 & 5. The job might be missing or the scoring threshold might be too aggressive for recent data.

## 3. Where Failures Surface
- **Logs**:
  - `/opt/agent-flux/logs/worker-out.log`: Look for `contact_discovery` task starts.
  - `/opt/agent-flux/logs/scheduler-out.log`: Look for `Lead review` or `Discovery` heartbeats.
- **Metrics**: `/metrics` showing `qualified_leads: 0`.
- **Database**:
  - `merchant_contacts`: Checking `confidence` distribution.
  - `merchant_opportunities`: Checking `status` of items without contacts.
- **Redis**: `agent_tasks` queue (monitor for stuck or missing discovery tasks).

## 4. Likely Causes (to evaluate)
- **Job Not Scheduled**: `cron_tasks.py` might be missing the autonomous discovery trigger.
- **Zero Results**: `contact_discovery.py` is running but Google/Brave search patterns for emails are failing or blocked.
- **Domain Extraction**: `domain_utils.py` failing to find the base-domain from raw signal URLs.
- **Threshold Gating**: `confidence.py` might be assigning $<0.8$ to valid contacts, keeping them hidden from the sales cycle.
- **Worker Handler**: `router.py` might lack the explicit "discovery" keyword routing, causing it to fall back to generic reasoning (LLM) instead of the specialized tool.

## 5. Production Evidence Available
- `fix_pipeline` diagnostics report: `Failures=['contact_discovery_stalled']`.
- `run_sales_cycle` report: `emails_sent: 0` with `No actionable conversion opportunities with valid endpoints found`.
- `merchant_contacts` schema was recently repaired (missing `contact_name`), which may have caused historical write failures.

## 6. Smallest Safe Fix Plan
1. **Validation**: Run a one-off `contact_discovery` test on a specific known-distressed merchant.
2. **Routing**: Ensure `router.py` has an explicit `discovery` -> `contact_discovery.py` tool mapping.
3. **Trigger**: If mapping exists, verify `cron_tasks.py` triggers it every 4 hours.
4. **Threshold Tuning**: If records exist with `0.7` confidence, evaluate lowering the gate to `0.75` vs. improving discovery patterns.

## 7. Explicit Guardrails
- **DO NOT** change `GMAIL_CHANNEL_MODE` or any sending logic.
- **DO NOT** modify `outreach_generator.py` or email templates.
- **DO NOT** change signal sources or frequency.
- **DO NOT** modify merchant identity resolution logic.

**Goal**: Restore the automated flow of valid contacts into the sales pipeline.
