-- Login Anomaly
-- Counts failed login attempts per account per hour
-- Engine: SummingMergeTree automatically sums failed_attempts on merges

CREATE TABLE mv_login_anomaly
(
    account_id      String,
    hour_bucket     DateTime,
    failed_attempts UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (account_id, hour_bucket);

CREATE MATERIALIZED VIEW mv_login_anomaly_mv
TO mv_login_anomaly
AS
SELECT
    account_id,
    toStartOfHour(event_time) AS hour_bucket,
    count()                   AS failed_attempts
FROM bank_transactions
WHERE event_type = 'login_attempt'
  AND status = 'failed'
GROUP BY account_id, hour_bucket;
