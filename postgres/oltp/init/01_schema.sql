CREATE SCHEMA IF NOT EXISTS banking;

CREATE TABLE IF NOT EXISTS banking.customers (
    customer_id TEXT PRIMARY KEY,
    customer_segment TEXT NOT NULL,
    customer_region TEXT NOT NULL,
    country_code TEXT NOT NULL,
    risk_tier TEXT NOT NULL,
    kyc_status TEXT NOT NULL,
    customer_status TEXT NOT NULL,
    onboarding_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS banking.accounts (
    account_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE REFERENCES banking.customers(customer_id),
    account_type TEXT NOT NULL,
    currency_code TEXT NOT NULL,
    account_status TEXT NOT NULL,
    available_balance NUMERIC(14, 2) NOT NULL,
    opened_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS banking.cards (
    card_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL UNIQUE REFERENCES banking.accounts(account_id),
    customer_id TEXT NOT NULL UNIQUE REFERENCES banking.customers(customer_id),
    card_type TEXT NOT NULL,
    card_network TEXT NOT NULL,
    card_status TEXT NOT NULL,
    issued_at TIMESTAMP NOT NULL,
    expiry_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS banking.terminals (
    terminal_id TEXT PRIMARY KEY,
    merchant_id TEXT NOT NULL,
    merchant_category TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    city TEXT NOT NULL,
    terminal_region TEXT NOT NULL,
    country_code TEXT NOT NULL,
    risk_band TEXT NOT NULL,
    terminal_status TEXT NOT NULL,
    installed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS banking.transactions (
    transaction_id BIGINT PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    customer_id TEXT NOT NULL REFERENCES banking.customers(customer_id),
    account_id TEXT NOT NULL REFERENCES banking.accounts(account_id),
    card_id TEXT NOT NULL REFERENCES banking.cards(card_id),
    terminal_id TEXT NOT NULL REFERENCES banking.terminals(terminal_id),
    amount NUMERIC(12, 2) NOT NULL,
    currency_code TEXT NOT NULL,
    transaction_type TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    auth_status TEXT NOT NULL,
    tx_time_seconds INTEGER NOT NULL,
    tx_time_days INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_night BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS banking.fraud_cases (
    case_id TEXT PRIMARY KEY,
    transaction_id BIGINT NOT NULL UNIQUE REFERENCES banking.transactions(transaction_id),
    customer_id TEXT NOT NULL REFERENCES banking.customers(customer_id),
    card_id TEXT NOT NULL REFERENCES banking.cards(card_id),
    fraud_scenario INTEGER NOT NULL,
    case_status TEXT NOT NULL,
    resolution_source TEXT NOT NULL,
    reported_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    loss_amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_customer_time
    ON banking.transactions (customer_id, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_terminal_time
    ON banking.transactions (terminal_id, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_transactions_card_time
    ON banking.transactions (card_id, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_fraud_cases_reported_at
    ON banking.fraud_cases (reported_at);

CREATE INDEX IF NOT EXISTS idx_fraud_cases_scenario
    ON banking.fraud_cases (fraud_scenario);