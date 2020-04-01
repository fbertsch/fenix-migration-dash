WITH client_attempts AS (
  SELECT
    client_info.client_id,
    '{migration_type}' as migration_type,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    metrics.timespan.migration_{migration_type}_total_duration AS duration,
    udf.get_key(metrics.labeled_string.migration_migration_versions, '{migration_type}') AS migration_version
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND metrics.timespan.migration_{migration_type}_total_duration.value IS NOT NULL
), counts AS (
  SELECT
    minute,
    migration_type,
    migration_version,
    CAST(FLOOR(duration.value / 5) As INT64) * 5 AS bucket,
    COUNT(*) AS count
  FROM
    client_attempts
  GROUP BY
    minute,
    migration_type,
    migration_version,
    bucket
)

SELECT
  DATE(minute) AS date,
  *
FROM
  counts
