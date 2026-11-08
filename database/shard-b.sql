
CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(5) PRIMARY KEY
        CHECK (
            account_id ~ '^A[0-9]{4}$'
            AND account_id BETWEEN 'A1001' AND 'A2000'
        ),

    balance NUMERIC(18, 2) NOT NULL DEFAULT 0
        CHECK (balance >= 0),

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
