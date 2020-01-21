WITH  client_attempts AS (
  SELECT
    client_info.client_id,
    '{migration_type}' as migration_type,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    ROW_NUMBER() OVER (PARTITION BY client_info.client_id ORDER BY submission_timestamp ASC) AS attempt_number,
    metrics.boolean.migration_{migration_type}_any_failures AS any_failures,
    coalesce(udf.get_key(metrics.labeled_string.migration_migration_versions, '{migration_type}'), '1') AS migration_version
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND (metrics.boolean.migration_{migration_type}_any_failures IS NOT NULL
         OR ('{migration_type}' = 'settings' AND metrics.counter.migration_settings_success_reason IS NOT NULL)
         OR ('{migration_type}' = 'addons' AND udf.get_key(metrics.labeled_string.migration_migration_versions, '__other__') IS NOT NULL)
         OR udf.get_key(metrics.labeled_string.migration_migration_versions, '{migration_type}') IS NOT NULL)
), counts AS (
  SELECT
    minute,
    migration_type,
    migration_version,
    COUNT(*) AS attempts,
    SUM(IF(any_failures, 1, 0)) AS failures,
    SUM(IF(attempt_number > 1, 1, 0)) AS retries
  FROM
    client_attempts
  GROUP BY
    minute,
    migration_type,
    migration_version
)

SELECT
  DATE(minute) AS date,
  minute,
  migration_type,
  migration_version,
  attempts,
  retries,
  failures,
  attempts - failures AS successes,
FROM
  counts
