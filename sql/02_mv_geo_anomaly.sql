-- Geo Anomaly
-- Tracks the most recent country per account
-- Engine: ReplacingMergeTree always retains the latest row per account_id

CREATE TABLE mv_geo_anomaly
(
    account_id      String,
    last_country    String,
    last_event_time DateTime
)
ENGINE = ReplacingMergeTree(last_event_time)
ORDER BY account_id;

CREATE MATERIALIZED VIEW mv_geo_anomaly_mv
TO mv_geo_anomaly
AS
SELECT
    account_id,
    country    AS last_country,
    event_time AS last_event_time
FROM bank_transactions
WHERE event_type IN ('purchase_event', 'atm_withdrawal', 'online_transfer');
