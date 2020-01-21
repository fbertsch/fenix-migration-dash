WITH  client_attempts AS (
  SELECT
    client_info.client_id,
    '{reason_type}' as reason_type,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    metrics.counter.migration_{reason_type}_success_reason AS success_reason,
    udf.get_key(metrics.labeled_string.migration_migration_versions, '{reason_type}') AS migration_version
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND metrics.counter.migration_{reason_type}_success_reason IS NOT NULL
), counts AS (
  SELECT
    minute,
    reason_type AS migration_type,
    migration_version,
    success_reason,
    COUNT(*) AS count
  FROM
    client_attempts
  GROUP BY
    minute,
    reason_type,
    migration_version,
    success_reason 
)

SELECT
  DATE(minute) AS date,
  *
FROM
  counts
