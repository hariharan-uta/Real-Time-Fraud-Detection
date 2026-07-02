-- High Value Alert Detection
-- Returns all transactions above $5,000 in the last 30 minutes
-- Reads from mv_high_value_alerts (MergeTree) not the raw table

SELECT
    account_id,
    amount,
    merchant,
    country,
    city,
    event_time
FROM mv_high_value_alerts
WHERE event_time >= now() - INTERVAL 30 MINUTE
ORDER BY amount DESC
LIMIT 10;
