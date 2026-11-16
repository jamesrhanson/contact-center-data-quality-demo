-- Data quality monitoring views over the two published pipeline event logs.
-- Executed by demo_reset (one statement per semicolon). Both views only show events
-- since the latest full refresh of each pipeline. The fq placeholder = catalog.schema.

CREATE OR REPLACE VIEW {fq}.dq_expectation_metrics
COMMENT 'One row per pipeline update x dataset x expectation (passed / failed records), from both event logs.'
AS
WITH ev_all AS (
  SELECT 'cc_dq_ingestion_1e53d4' AS pipeline, * FROM {fq}.ingestion_event_log
  UNION ALL
  SELECT 'cc_dq_transformation_1e53d4' AS pipeline, * FROM {fq}.transformation_event_log
),
-- Scope to events since the latest full refresh of each pipeline (demo_reset), so a
-- rehearsal's failures don't show on the dashboard or re-fire the alert after a reset.
since AS (
  SELECT pipeline, max(`timestamp`) AS since_ts FROM ev_all
  WHERE event_type = 'create_update' AND details:create_update.full_refresh::boolean
  GROUP BY pipeline
),
ev AS (
  SELECT e.* FROM ev_all e LEFT JOIN since s USING (pipeline)
  WHERE s.since_ts IS NULL OR e.`timestamp` >= s.since_ts
),
x AS (
  SELECT pipeline, origin.update_id AS update_id, `timestamp` AS event_ts,
         explode(from_json(details:flow_progress.data_quality.expectations::string,
           'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>')) AS e
  FROM ev
  WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality.expectations IS NOT NULL
)
SELECT pipeline, update_id, event_ts,
       element_at(split(e.dataset, '\\.'), -1) AS dataset,
       e.name AS expectation,
       e.passed_records, e.failed_records,
       e.failed_records / nullif(e.passed_records + e.failed_records, 0) AS failure_rate
FROM x;

CREATE OR REPLACE VIEW {fq}.dq_pipeline_failures
COMMENT 'One row per failed pipeline update: error message, failing expectation, table and an example violating record.'
AS
WITH ev_all AS (
  SELECT 'cc_dq_ingestion_1e53d4' AS pipeline, * FROM {fq}.ingestion_event_log
  UNION ALL
  SELECT 'cc_dq_transformation_1e53d4' AS pipeline, * FROM {fq}.transformation_event_log
),
-- Scope to events since the latest full refresh of each pipeline (demo_reset), so a
-- rehearsal's failures don't show on the dashboard or re-fire the alert after a reset.
since AS (
  SELECT pipeline, max(`timestamp`) AS since_ts FROM ev_all
  WHERE event_type = 'create_update' AND details:create_update.full_refresh::boolean
  GROUP BY pipeline
),
ev AS (
  SELECT e.* FROM ev_all e LEFT JOIN since s USING (pipeline)
  WHERE s.since_ts IS NULL OR e.`timestamp` >= s.since_ts
),
err AS (
  SELECT pipeline, origin.update_id AS update_id, `timestamp` AS event_ts, origin.flow_name AS flow_name,
         message, coalesce(error.exceptions[0].message, message) AS error_message
  FROM ev
  WHERE level = 'ERROR'
),
failed AS (
  SELECT DISTINCT pipeline, origin.update_id AS update_id
  FROM ev
  WHERE event_type = 'update_progress' AND details:update_progress.state::string = 'FAILED'
),
ranked AS (
  SELECT e.*, row_number() OVER (PARTITION BY e.pipeline, e.update_id
           ORDER BY (e.error_message ILIKE '%expectation%') DESC, e.event_ts) AS rn
  FROM err e JOIN failed f USING (pipeline, update_id)
)
SELECT pipeline, update_id, event_ts, message,
       nullif(regexp_extract(error_message, "Violated expectations: '([^']+)'", 1), '') AS expectation,
       element_at(split(coalesce(nullif(flow_name, ''),
                nullif(regexp_extract(error_message, "[Ff]low '([^']+)'", 1), '')), '\\.'), -1) AS table_name,
       nullif(regexp_extract(error_message, "Output record: '(.*)'\\. Missing input data", 1), '') AS example_record,
       error_message
FROM ranked
WHERE rn = 1;
