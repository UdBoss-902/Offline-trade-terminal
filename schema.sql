-- SQLite Local Trade Store & Transactional Outbox Schema

DROP TABLE IF EXISTS outbox_queue;
DROP TABLE IF EXISTS trades;

-- 1. Local Trades Table
CREATE TABLE trades (
    trade_id TEXT PRIMARY KEY,          -- Client-side UUIDv4 (Idempotency key)
    sku TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY', 'SELL')),
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    price_cents INTEGER NOT NULL CHECK(price_cents > 0),
    timestamp_ns INTEGER NOT NULL,      -- High-precision Nanosecond Epoch
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 2. Transactional Outbox Queue Table
CREATE TABLE outbox_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'PENDING_SYNC' CHECK(status IN ('PENDING_SYNC', 'IN_FLIGHT', 'SYNCED', 'FAILED')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    synced_at DATETIME,
    FOREIGN KEY(trade_id) REFERENCES trades(trade_id) ON DELETE CASCADE
);

-- Index for fast outbox querying during high-volume sync sweeps
CREATE INDEX idx_outbox_pending ON outbox_queue(status, id);