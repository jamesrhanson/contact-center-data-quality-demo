-- =====================================================================================
-- Ingestion pipeline (bronze) — cc_dq_ingestion_1e53d4
-- Auto Loader (read_files) from the volume. Rules live next to the code.
--
-- * Declared schema on every read: a missing key arrives as NULL instead of silently
--   changing the schema — that is what makes the contract rule below fire.
-- * _rescued_data captures unexpected fields; schema evolution stays off.
-- * pipelines.reset.allowed: "Bronze is the only copy of what arrived. Allow full refresh
--   only in dev, so a reset can never wipe history in prod."
-- * Bronze is WARN-only except ONE contract check on calls_raw, which FAILs the flow.
--   FAIL is for incidents (a broken file), not for stray bad rows.
-- =====================================================================================

CREATE OR REFRESH STREAMING TABLE calls_raw (
  -- Contract: every vendor calls file must carry the timing columns. If not, stop loudly:
  -- the flow fails and nothing from the file reaches calls_raw (atomic rollback).
  CONSTRAINT required_columns_present
    EXPECT (call_start_ts IS NOT NULL AND call_end_ts IS NOT NULL AND duration_seconds IS NOT NULL)
    ON VIOLATION FAIL UPDATE,
  -- Informational only at bronze; silver drops + quarantines null ids.
  CONSTRAINT call_id_present EXPECT (call_id IS NOT NULL)
)
COMMENT 'Raw calls from the vendor feed (Auto Loader). Contract FAIL on required timing columns.'
TBLPROPERTIES ('pipelines.reset.allowed' = '${bronze_reset_allowed}')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/calls/',
  format => 'json',
  schema => 'call_id STRING, agent_id STRING, member_id STRING, queue STRING, channel STRING, language STRING, call_start_ts TIMESTAMP, call_end_ts TIMESTAMP, duration_seconds INT, transferred BOOLEAN',
  rescuedDataColumn => '_rescued_data',
  -- Stock Auto Loader option. After a contract FAIL the presenter moves the bad file to
  -- rejected/. The retried micro-batch still lists that file; without this option the
  -- retry fails with CloudFileNotFoundException (verified on the real build).
  ignoreMissingFiles => 'true'
);

CREATE OR REFRESH STREAMING TABLE transcripts_raw (
  CONSTRAINT transcript_text_present EXPECT (transcript_text IS NOT NULL AND transcript_text <> '')
)
COMMENT 'Raw call transcripts (Auto Loader). Warn-only at bronze.'
TBLPROPERTIES ('pipelines.reset.allowed' = '${bronze_reset_allowed}')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/transcripts/',
  format => 'json',
  schema => 'call_id STRING, transcript_text STRING, word_count INT',
  rescuedDataColumn => '_rescued_data'
);

CREATE OR REFRESH STREAMING TABLE quality_scores_raw (
  CONSTRAINT raw_model_output_present EXPECT (raw_model_output IS NOT NULL)
)
COMMENT 'Raw AI QA model scores per call (Auto Loader). Warn-only at bronze.'
TBLPROPERTIES ('pipelines.reset.allowed' = '${bronze_reset_allowed}')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/quality_scores/',
  format => 'json',
  schema => 'call_id STRING, model_name STRING, model_version STRING, empathy_score INT, resolution_score INT, compliance_score INT, raw_model_output STRING, scored_ts TIMESTAMP',
  rescuedDataColumn => '_rescued_data'
);

CREATE OR REFRESH STREAMING TABLE agents (
  CONSTRAINT agent_id_present EXPECT (agent_id IS NOT NULL)
)
COMMENT 'Agent lookup (~200 rows).'
TBLPROPERTIES ('pipelines.reset.allowed' = '${bronze_reset_allowed}')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${landing_path}/agents/',
  format => 'json',
  schema => 'agent_id STRING, team STRING, site STRING, hire_date DATE, active BOOLEAN',
  rescuedDataColumn => '_rescued_data'
);
