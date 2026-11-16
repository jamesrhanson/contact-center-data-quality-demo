-- cc_dq_alert_1e53d4: fires when breaches > 0.
-- A FAIL records no expectation metrics for that update, so we alert on the quarantine
-- share AND on the failure event itself.
WITH share AS (
  SELECT 'calls_quarantine' AS quarantine_table,
         (SELECT count(*) FROM {fq}.calls_quarantine WHERE _ingested_at >= current_timestamp() - INTERVAL 1 HOUR) AS quarantined,
         (SELECT count(*) FROM {fq}.calls_raw WHERE _ingested_at >= current_timestamp() - INTERVAL 1 HOUR) AS rows_in
  UNION ALL
  SELECT 'quality_scores_quarantine',
         (SELECT count(*) FROM {fq}.quality_scores_quarantine WHERE _ingested_at >= current_timestamp() - INTERVAL 1 HOUR),
         (SELECT count(*) FROM {fq}.quality_scores_raw WHERE _ingested_at >= current_timestamp() - INTERVAL 1 HOUR)
),
share_breach AS (
  SELECT count_if(rows_in > 0 AND quarantined / rows_in > 0.05) AS n,
         concat_ws(', ', collect_list(IF(rows_in > 0 AND quarantined / rows_in > 0.05,
           concat(quarantine_table, ' ', round(100 * quarantined / rows_in, 1), '%'), NULL))) AS detail
  FROM share
),
fail AS (
  SELECT count(*) AS n,
         concat_ws(', ', collect_set(concat(coalesce(expectation, 'error'), ' on ', table_name))) AS detail
  FROM {fq}.dq_pipeline_failures
  WHERE event_ts >= current_timestamp() - INTERVAL 1 HOUR
)
SELECT (SELECT n FROM share_breach) + IF((SELECT n FROM fail) > 0, 1, 0) AS breaches,
       (SELECT detail FROM share_breach) AS quarantine_share_breaches,
       (SELECT n FROM fail) AS failed_updates_last_hour,
       (SELECT detail FROM fail) AS failed_rules
