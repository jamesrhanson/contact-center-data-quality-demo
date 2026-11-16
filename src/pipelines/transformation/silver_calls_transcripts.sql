-- =====================================================================================
-- Transformation pipeline (silver / quarantine / gold) — cc_dq_transformation_1e53d4
--
-- "A stray null is a row problem, so drop and quarantine it. FAIL is for incidents."
-- DROP rules remove bad rows from the clean table; the quarantine table keeps exactly
-- those rows (NOT(all DROP rules)) with the reasons, so clean + quarantine = raw.
-- WARN rules keep the row and only count the violation.
-- =====================================================================================

CREATE OR REFRESH STREAMING TABLE calls_clean (
  CONSTRAINT call_id_not_null EXPECT (call_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_duration   EXPECT (duration_seconds > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamps EXPECT (call_end_ts >= call_start_ts) ON VIOLATION DROP ROW,
  CONSTRAINT valid_queue      EXPECT (queue IN ('Pharmacy', 'Claims', 'Benefits', 'Enrollment', 'Billing')),
  CONSTRAINT known_agent      EXPECT (agent_matched)
)
COMMENT 'Validated calls joined to the agent lookup. DROP rows go to calls_quarantine.'
AS SELECT
  c.call_id, c.agent_id, c.member_id, c.queue, c.channel, c.language,
  c.call_start_ts, c.call_end_ts, c.duration_seconds, c.transferred,
  a.team AS agent_team, a.site AS agent_site,
  a.agent_id IS NOT NULL AS agent_matched,
  c._source_file, c._ingested_at
FROM STREAM(${catalog}.${schema}.calls_raw) c
LEFT JOIN ${catalog}.${schema}.agents a ON c.agent_id = a.agent_id;

CREATE OR REFRESH STREAMING TABLE calls_quarantine
COMMENT 'Calls rows that broke at least one DROP rule, with the reasons. clean + quarantine = raw.'
AS SELECT
  call_id, agent_id, member_id, queue, channel, language,
  call_start_ts, call_end_ts, duration_seconds, transferred,
  filter(array(
    IF(call_id IS NULL, 'call_id_not_null', NULL),
    IF(NOT coalesce(duration_seconds > 0, false), 'valid_duration', NULL),
    IF(NOT coalesce(call_end_ts >= call_start_ts, false), 'valid_timestamps', NULL)
  ), x -> x IS NOT NULL) AS failed_rules,
  _source_file, _ingested_at,
  current_timestamp() AS _loaded_at
FROM STREAM(${catalog}.${schema}.calls_raw)
WHERE NOT coalesce(call_id IS NOT NULL AND duration_seconds > 0 AND call_end_ts >= call_start_ts, false);

CREATE OR REFRESH STREAMING TABLE transcripts_clean (
  CONSTRAINT min_word_count EXPECT (word_count >= 20) ON VIOLATION DROP ROW,
  CONSTRAINT no_pii_pattern EXPECT (
    NOT (transcript_text RLIKE '555-01[0-9]{2}' OR transcript_text RLIKE '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}')
  )
)
COMMENT 'Transcripts with at least 20 words. PII pattern is a WARN (kept, counted).'
AS SELECT call_id, transcript_text, word_count, _source_file, _ingested_at
FROM STREAM(${catalog}.${schema}.transcripts_raw);

-- Extended table-level check: aggregates in a materialized view, expectations on the result.
CREATE OR REFRESH MATERIALIZED VIEW daily_call_checks (
  CONSTRAINT no_duplicate_call_ids_per_day EXPECT (duplicate_call_ids = 0),
  CONSTRAINT day_has_rows EXPECT (call_count > 0)
)
COMMENT 'Per-day table-level checks on calls_clean (duplicates, empty days).'
AS SELECT
  call_date,
  sum(n) AS call_count,
  count_if(n > 1) AS duplicate_call_ids
FROM (
  SELECT to_date(call_start_ts) AS call_date, call_id, count(*) AS n
  FROM ${catalog}.${schema}.calls_clean
  GROUP BY ALL
)
GROUP BY call_date;

-- Table-level volume checks (WARN only): aggregate in the MV, then a per-row rule on each day.
-- Rule 1: within 3 standard deviations of the prior 7 days (a band that fits the data's normal noise).
-- Rule 2: within 2% of the prior day (a fixed band; expect it to warn on normal day-to-day swings).
-- Only complete days are checked (today is excluded). For week over week, group by
-- date_trunc('WEEK', call_start_ts) instead of to_date(...) and keep the rest the same.
-- To stop the pipeline on a breach, add ON VIOLATION FAIL UPDATE to the sigma rule.
CREATE OR REFRESH MATERIALIZED VIEW daily_volume_checks (
  CONSTRAINT volume_within_3_stddev EXPECT (
    stddev_prior_7d IS NULL OR abs(call_count - avg_prior_7d) <= 3 * stddev_prior_7d),
  CONSTRAINT volume_within_2pct_of_prior_day EXPECT (
    prior_day_count IS NULL OR abs(call_count - prior_day_count) <= 0.02 * prior_day_count)
)
COMMENT 'Per-day volume checks on calls_clean (WARN): vs prior 7 days (3 standard deviations) and vs prior day (2%).'
AS WITH daily AS (
  SELECT to_date(call_start_ts) AS call_date, count(*) AS call_count
  FROM ${catalog}.${schema}.calls_clean
  WHERE to_date(call_start_ts) < current_date()
  GROUP BY ALL
)
SELECT
  call_date,
  call_count,
  lag(call_count) OVER (ORDER BY call_date) AS prior_day_count,
  avg(call_count) OVER (ORDER BY call_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg_prior_7d,
  stddev(call_count) OVER (ORDER BY call_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS stddev_prior_7d
FROM daily;

CREATE OR REFRESH MATERIALIZED VIEW call_quality_daily
COMMENT 'Gold: daily call volume and average QA scores per queue.'
AS SELECT
  to_date(c.call_start_ts) AS call_date,
  c.queue,
  count(*) AS calls,
  avg(q.empathy_score) AS avg_empathy_score,
  avg(q.resolution_score) AS avg_resolution_score,
  avg(q.compliance_score) AS avg_compliance_score,
  avg(CAST(c.transferred AS INT)) AS transferred_rate
FROM ${catalog}.${schema}.calls_clean c
LEFT JOIN ${catalog}.${schema}.quality_scores_clean q ON c.call_id = q.call_id
GROUP BY ALL;
