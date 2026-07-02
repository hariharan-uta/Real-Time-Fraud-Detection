-- High Value Alerts
-- Captures any single transaction above $5,000 immediately on insert
-- Engine: MergeTree with no aggregation — full event row is preserved

CREATE TABLE mv_high_value_alerts
(
    event_id   String,
    account_id String,
    amount     Float64,
    merchant   String,
    country    String,
    city       String,
    device_id  String,
    ip_address String,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, account_id);

CREATE MATERIALIZED VIEW mv_high_value_alerts_mv
TO mv_high_value_alerts
AS
SELECT
    event_id,
    account_id,
    amount,
    merchant,
    country,
    city,
    device_id,
    ip_address,
    event_time
FROM bank_transactions
WHERE amount > 5000;
