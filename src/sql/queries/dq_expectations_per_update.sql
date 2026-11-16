-- Passed / failed records per rule, per table, per pipeline update (latest first)
SELECT event_ts, pipeline, update_id, dataset, expectation, passed_records, failed_records,
       round(100 * failure_rate, 2) AS failure_pct
FROM {fq}.dq_expectation_metrics
ORDER BY event_ts DESC, dataset, expectation
