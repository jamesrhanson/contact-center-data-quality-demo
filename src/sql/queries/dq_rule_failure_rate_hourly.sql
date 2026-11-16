-- Failure rate per rule per hour, last 24 hours
SELECT date_trunc('HOUR', event_ts) AS hour, dataset, expectation,
       sum(failed_records) AS failed_records,
       sum(failed_records) / nullif(sum(passed_records + failed_records), 0) AS failure_rate
FROM {fq}.dq_expectation_metrics
WHERE event_ts >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY ALL
ORDER BY hour DESC, failure_rate DESC
