# Agent Flux Autonomous CEO — Implementation Plan

## Vision
Transform Agent Flux from "intelligence tool that assists the operator" into a fully autonomous revenue-generating system that runs PayFlux end-to-end: finds merchants, publishes thought leadership, sends outreach, closes deals, and notifies the operator when money comes in.

---

## PHASE 1 — Intelligence Report Engine (auto-publish thought leadership)

### 1A. Database: `merchant_intelligence_reports` table
```sql
CREATE TABLE merchant_intelligence_reports (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    slug TEXT UNIQUE NOT NULL,
    executive_summary TEXT,
    content TEXT NOT NULL,
    processor TEXT,
    industry TEXT,
    cluster_size INTEGER,
    risk_level TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_reports_slug ON merchant_intelligence_reports(slug);
CREATE INDEX idx_reports_created ON merchant_intelligence_reports(created_at DESC);
```

### 1B. New module: `report_generator.py`
- `generate_market_reports()` — entry point, runs every 6 hours
- Queries report-worthy clusters: `cluster_size >= 15` AND `risk_level IN ('high','critical')`
- Joins: clusters → cluster_analysis → cluster_intelligence → signals
- Deduplicates against existing reports (skip if slug already exists)
- Calls Claude via `reason_fast()` with structured prompt requesting:
  - Title, Executive Summary, Merchant Impact, Financial Impact, Root Cause, Forecast, How PayFlux Helps
- Generates URL-safe slug from title
- Stores in `merchant_intelligence_reports`
- Calls `send_operator_alert()` with report summary
- Calls `save_event("report_generated", {...})`

### 1C. New API routes in `main.py`
- `GET /intelligence/reports` — list all reports (title, slug, summary, processor, risk_level, created_at)
- `GET /intelligence/reports/{slug}` — full report by slug
- Update CORS to allow GET method (currently POST+OPTIONS only)

### 1D. Frontend: Reports page + Report detail
- `Reports.jsx` — fetches `/intelligence/reports`, renders card grid with title, summary, date, risk badge
- `ReportDetail.jsx` — fetches `/intelligence/reports/{slug}`, renders full report with structured sections
- Add routes: `/reports` and `/reports/:slug` in App.jsx
- Both pages use dark theme (bg-slate-950) matching existing design
- Add "Intelligence" link in nav/footer

### 1E. Scheduler: add to `cron_tasks.py`
```python
schedule.every(6).hours.do(lambda: _safe("generate_market_reports", generate_market_reports))
```

---

## PHASE 2 — Sales Assistant (AI-drafted outreach)

### 2A. Database: `lead_conversations` table
```sql
CREATE TABLE lead_conversations (
    id SERIAL PRIMARY KEY,
    lead_id INTEGER REFERENCES qualified_leads(id),
    merchant_name TEXT,
    message TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('operator', 'assistant', 'merchant')),
    channel TEXT DEFAULT 'draft',
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_conversations_lead ON lead_conversations(lead_id);
```

### 2B. New module: `sales_assistant.py`
- `generate_sales_message(lead_id)` — generates personalized outreach draft
  - Fetches lead context: merchant_name, processor, industry, distress signals, qualification_score
  - Fetches contact info if available
  - Calls `reason_fast()` with sales-specific prompt (subject line + body)
  - Stores draft in `lead_conversations` (role='assistant')
  - Returns structured output: { subject, body, checkout_url, merchant_name }
- `get_sales_context(lead_id)` — returns full merchant intelligence for operator review
  - Lead data, signals, cluster membership, contact info, conversation history
- Constraint: NEVER auto-sends. All messages are drafts for operator review.

### 2C. Chat engine integration — new tools
Add to `chat_engine.py`:
- `draft_outreach` tool — calls `generate_sales_message(lead_id)`, returns formatted draft
- `sales_context` tool — calls `get_sales_context(lead_id)`, returns full merchant context
- `list_pending_outreach` tool — lists conversion_opportunities with status='pending'
- `approve_outreach` tool — marks an opportunity as 'approved' (future: triggers email send)

### 2D. Conversion tracking — webhook enhancement
Update `stripe_checkout.py` webhook handler for `checkout.session.completed`:
- Match session_id against `conversion_opportunities.checkout_session_id`
- Update status to `'converted'`
- Send Telegram alert: "💰 New PayFlux customer converted — {merchant_name}"
- Save event: `"customer_converted"`

---

## PHASE 3 — Autonomous Email Outreach (himalaya CLI)

### 3A. Email infrastructure
- Install/configure `himalaya` CLI on server for sending/receiving email
- Create dedicated outreach email: outreach@payflux.dev (or similar)
- Store email config in environment variables

### 3B. New module: `email_outreach.py`
- `send_outreach_email(opportunity_id)` — sends approved outreach via himalaya
  - Only sends opportunities with status='approved' (operator must approve first)
  - Uses himalaya CLI: `himalaya send --from outreach@payflux.dev`
  - Updates opportunity status to 'sent'
  - Logs to `lead_conversations` (role='operator', channel='email')
  - Sends Telegram alert: "📧 Outreach sent to {merchant_name}"
- `check_email_replies()` — checks for inbound replies
  - Uses himalaya to read inbox
  - Matches replies to existing conversations
  - Stores in `lead_conversations` (role='merchant', channel='email')
  - Alerts operator of replies
- Add to scheduler: `schedule.every(15).minutes.do(check_email_replies)`

### 3C. Approval flow
The agent generates drafts automatically. The operator reviews via Telegram:
1. Agent creates opportunity + draft → alerts operator
2. Operator sends `/approve {opportunity_id}` via Telegram
3. Agent sends email via himalaya
4. Agent monitors for replies and alerts operator

---

## PHASE 4 — Expanded Autonomy (browser + GitHub)

### 4A. Browser automation (future)
- Add Playwright or similar for merchant research
- Deep company profiling: revenue estimates, tech stack, team size
- Monitor merchant websites for distress signals (checkout broken, etc.)

### 4B. GitHub CLI access (future)
- Agent can push code changes (self-improvement cycle)
- Auto-deploy frontend updates (new report pages, etc.)
- Create PRs for review

### 4C. Enhanced heartbeat
Update `cron_tasks.py` to be the agent's "executive loop":
```
Every 15 min  → check_email_replies()
Every 20 min  → run_cluster_investigation()
Every 30 min  → autonomous_market_cycle()
Every 30 min  → check_conversion_opportunities()
Every 30 min  → run_processor_discovery()
Every 30 min  → run_pre_distress_monitor()
Every 60 min  → run_distress_radar()
Every 60 min  → run_failure_forecast()
Every 60 min  → run_contact_discovery()
Every 6 hrs   → generate_market_reports()
Every 12 hrs  → self_improvement_cycle()
Every 24 hrs  → generate_daily_report()
```

---

## Implementation Order (this session)

### Step 1: SQL Migrations
- Create `merchant_intelligence_reports` table
- Create `lead_conversations` table

### Step 2: `report_generator.py`
- Full module with cluster querying, Claude report generation, storage, alerts

### Step 3: `sales_assistant.py`
- Full module with outreach drafting, context retrieval, conversation storage

### Step 4: Update `main.py`
- Add GET routes for reports API
- Update CORS to allow GET
- Add reports list + detail endpoints

### Step 5: Update `stripe_checkout.py`
- Add conversion tracking to webhook handler

### Step 6: Update `chat_engine.py`
- Add new tools: draft_outreach, sales_context, list_pending_outreach, approve_outreach

### Step 7: Update `cron_tasks.py`
- Add report generation every 6 hours

### Step 8: Frontend — Reports.jsx + ReportDetail.jsx
- Reports listing page
- Individual report page with dynamic slug routing
- Update App.jsx routes
- Update _redirects for SPA

### Step 9: Deploy everything
- SCP all backend files to server
- Run SQL migrations
- Restart PM2
- Build and deploy frontend to Netlify

### Step 10: Verify end-to-end
- Trigger report generation manually
- Verify reports appear on site
- Test outreach draft generation
- Verify new Telegram tools
- Test conversion tracking webhook

---

## Files Created/Modified

### New files:
- `/Users/ct/Agent Flux/deploy/report_generator.py`
- `/Users/ct/Agent Flux/deploy/sales_assistant.py`
- `/Users/ct/payflux-prod/payflux-site/src/pages/Reports.jsx`
- `/Users/ct/payflux-prod/payflux-site/src/pages/ReportDetail.jsx`

### Modified files:
- `/Users/ct/Agent Flux/deploy/main.py` (new routes + CORS GET)
- `/Users/ct/Agent Flux/deploy/cron_tasks.py` (new scheduler entries)
- `/Users/ct/Agent Flux/deploy/chat_engine.py` (new tools)
- `/Users/ct/Agent Flux/deploy/stripe_checkout.py` (conversion tracking)
- `/Users/ct/payflux-prod/payflux-site/src/App.jsx` (new routes)
- `/Users/ct/payflux-prod/payflux-site/public/_redirects` (SPA routes for /reports)
