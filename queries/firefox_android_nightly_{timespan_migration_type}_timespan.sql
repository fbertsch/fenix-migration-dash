WITH client_attempts AS (
  SELECT
    client_info.client_id,
    '{timespan_migration_type}' as timespan_migration_type,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    metrics.timespan.migration_{timespan_migration_type}_duration AS duration,
    udf.get_key(metrics.labeled_string.migration_migration_versions, '{timespan_migration_type}') AS migration_version
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= '2020-01-10'
    AND DATE(submission_timestamp) <= current_date
    AND metrics.timespan.migration_{timespan_migration_type}_duration.value IS NOT NULL
), counts AS (
  SELECT
    minute,
    timespan_migration_type,
    migration_version,
    CAST(FLOOR(duration.value / 5) As INT64) * 5 AS bucket,
    COUNT(*) AS count
  FROM
    client_attempts
  GROUP BY
    minute,
    timespan_migration_type,
    migration_version,
    bucket
)

SELECT
  DATE(minute) AS date,
  *
FROM
  counts
