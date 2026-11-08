CREATE TABLE IF NOT EXISTS distributed_transactions (
    transaction_id UUID PRIMARY KEY,
    status VARCHAR(30) NOT NULL,
    from_account_id VARCHAR(5) NOT NULL,
    to_account_id VARCHAR(5) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transaction_participants (
    transaction_id UUID NOT NULL
        REFERENCES distributed_transactions(transaction_id),
    shard_id VARCHAR(10) NOT NULL,
    status VARCHAR(30) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (transaction_id, shard_id)
);