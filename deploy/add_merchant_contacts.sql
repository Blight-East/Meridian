-- Merchant Contact Discovery Migration
-- Creates merchant_contacts table

CREATE TABLE IF NOT EXISTS merchant_contacts (
    id SERIAL PRIMARY KEY,
    merchant_id INTEGER REFERENCES merchants(id),
    contact_name TEXT,
    email TEXT,
    linkedin_url TEXT,
    source TEXT,
    confidence FLOAT DEFAULT 0.5,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_merchant_contacts_merchant ON merchant_contacts(merchant_id);
CREATE INDEX IF NOT EXISTS idx_merchant_contacts_email ON merchant_contacts(email);
