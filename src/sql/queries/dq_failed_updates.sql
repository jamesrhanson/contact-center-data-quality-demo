-- Failed pipeline updates: which rule, which table, an example violating record
SELECT event_ts, pipeline, update_id, expectation, table_name, example_record, error_message
FROM {fq}.dq_pipeline_failures
ORDER BY event_ts DESC
