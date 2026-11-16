-- Quarantine share per hour of ingestion, per quarantine table
WITH r AS (
  SELECT 'calls_quarantine' AS quarantine_table, date_trunc('HOUR', _ingested_at) AS hour, count(*) AS rows_in FROM {fq}.calls_raw GROUP BY ALL
  UNION ALL
  SELECT 'quality_scores_quarantine', date_trunc('HOUR', _ingested_at), count(*) FROM {fq}.quality_scores_raw GROUP BY ALL
), q AS (
  SELECT 'calls_quarantine' AS quarantine_table, date_trunc('HOUR', _ingested_at) AS hour, count(*) AS quarantined FROM {fq}.calls_quarantine GROUP BY ALL
  UNION ALL
  SELECT 'quality_scores_quarantine', date_trunc('HOUR', _ingested_at), count(*) FROM {fq}.quality_scores_quarantine GROUP BY ALL
)
SELECT r.quarantine_table, r.hour, r.rows_in, coalesce(q.quarantined, 0) AS quarantined,
       coalesce(q.quarantined, 0) / r.rows_in AS quarantine_share
FROM r LEFT JOIN q USING (quarantine_table, hour)
ORDER BY hour DESC, quarantine_table
