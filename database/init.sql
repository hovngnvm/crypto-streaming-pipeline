CREATE TABLE IF NOT EXISTS raw_trade (
    symbol VARCHAR(20),
    trade_type VARCHAR(10),
    price DOUBLE PRECISION,
    quantity DOUBLE PRECISION,
    total_value DOUBLE PRECISION,
    timestamp TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sliding_wd_trade (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    symbol VARCHAR(20),
    trade_type VARCHAR(10),
    total_volume DOUBLE PRECISION,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

