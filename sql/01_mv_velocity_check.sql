-- Velocity Check
-- Flags accounts with more than 5 transactions in any 5-minute window
-- Engine: SummingMergeTree automatically sums tx_count on background merges

CREATE TABLE mv_velocity_check
(
    account_id   String,
    window_start DateTime,
    tx_count     UInt64,
    total_amount Float64
)
ENGINE = SummingMergeTree()
ORDER BY (account_id, window_start);

CREATE MATERIALIZED VIEW mv_velocity_check_mv
TO mv_velocity_check
AS
SELECT
    account_id,
    toStartOfFiveMinutes(event_time) AS window_start,
    count()                          AS tx_count,
    sum(amount)                      AS total_amount
FROM bank_transactions
WHERE event_type != 'login_attempt'
GROUP BY account_id, window_start;
