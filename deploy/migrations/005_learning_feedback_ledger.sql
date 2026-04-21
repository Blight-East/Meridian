-- Create learning_feedback_ledger table
CREATE TABLE IF NOT EXISTS learning_feedback_ledger (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    signal_id INTEGER REFERENCES signals(id),
    source TEXT,
    merchant_id_candidate INTEGER,
    merchant_domain_candidate TEXT,
    opportunity_id INTEGER,
    contact_id INTEGER,
    decision_type TEXT,
    decision_payload_json JSONB,
    decision_score FLOAT,
    decision_model_version TEXT,
    outcome_type TEXT,
    outcome_value FLOAT,
    outcome_confidence FLOAT,
    outcome_at TIMESTAMP WITH TIME ZONE,
    reward_score FLOAT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_lfl_signal_id ON learning_feedback_ledger(signal_id);
CREATE INDEX IF NOT EXISTS idx_lfl_merchant_candidate ON learning_feedback_ledger(merchant_id_candidate);
CREATE INDEX IF NOT EXISTS idx_lfl_decision_type ON learning_feedback_ledger(decision_type);
CREATE INDEX IF NOT EXISTS idx_lfl_outcome_type ON learning_feedback_ledger(outcome_type);
