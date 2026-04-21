-- 006_deal_lifecycle.sql
-- Creates the deal_lifecycle and deal_stage_transitions tables.
-- Idempotent: uses IF NOT EXISTS throughout.

CREATE TABLE IF NOT EXISTS deal_lifecycle (
    id BIGSERIAL PRIMARY KEY,
    opportunity_id BIGINT UNIQUE,
    merchant_id BIGINT,
    merchant_domain TEXT NOT NULL DEFAULT '',
    signal_id BIGINT,
    current_stage TEXT NOT NULL DEFAULT 'signal_detected',
    previous_stage TEXT,
    stage_entered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stage_actor TEXT DEFAULT 'system',
    processor TEXT DEFAULT 'unknown',
    distress_type TEXT DEFAULT 'unknown',
    contact_email TEXT DEFAULT '',
    contact_trust_score INT DEFAULT 0,
    outreach_status TEXT DEFAULT 'no_outreach',
    gmail_thread_id TEXT DEFAULT '',
    outcome_status TEXT DEFAULT 'pending',
    icp_fit_score INT DEFAULT 0,
    commercial_readiness_score INT DEFAULT 0,
    priority_score FLOAT DEFAULT 0.0,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS deal_lifecycle_stage_priority_idx
ON deal_lifecycle (current_stage, priority_score DESC);

CREATE INDEX IF NOT EXISTS deal_lifecycle_domain_idx
ON deal_lifecycle (merchant_domain);

CREATE INDEX IF NOT EXISTS deal_lifecycle_updated_idx
ON deal_lifecycle (updated_at DESC);

CREATE TABLE IF NOT EXISTS deal_stage_transitions (
    id BIGSERIAL PRIMARY KEY,
    opportunity_id BIGINT NOT NULL,
    from_stage TEXT,
    to_stage TEXT NOT NULL,
    actor TEXT DEFAULT 'system',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    transitioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS deal_stage_transitions_opp_idx
ON deal_stage_transitions (opportunity_id, transitioned_at DESC);
