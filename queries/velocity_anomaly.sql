-- Velocity Anomaly Detection
-- Returns accounts with more than 5 transactions in any 5-minute window
-- Reads from mv_velocity_check (SummingMergeTree) not the raw table

SELECT
    account_id,
    window_start,
    sum(tx_count)     AS total_tx,
    sum(total_amount) AS total_amount,
    if(sum(tx_count) > 5, 'HIGH', 'MEDIUM') AS risk_level
FROM mv_velocity_check
WHERE window_start >= now() - INTERVAL 30 MINUTE
GROUP BY account_id, window_start
HAVING total_tx > 3
ORDER BY total_tx DESC
LIMIT 10;
