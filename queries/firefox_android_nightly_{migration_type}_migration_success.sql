WITH  client_attempts AS (
  SELECT
    client_info.client_id,
    '{migration_type}' as migration_type,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    metrics.counter.migration_{migration_type}_success_reason AS success_reason,
    udf.get_key(metrics.labeled_string.migration_migration_versions, '{migration_type}') AS migration_version
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND metrics.counter.migration_{migration_type}_success_reason IS NOT NULL
), counts AS (
  SELECT
    minute,
    migration_type,
    migration_version,
    success_reason,
    COUNT(*) AS count
  FROM
    client_attempts
  GROUP BY
    minute,
    migration_type,
    migration_version,
    success_reason 
), codes AS (
  SELECT success_reason + 1 AS success_reason, code
  FROM UNNEST([
    'LOGINS_MP_SET',
    'LOGINS_MIGRATED',
    'FXA_NO_ACCOUNT',
    'FXA_BAD_AUTH',
    'FXA_SIGNED_IN',
    'SETTINGS_NO_PREFS',
    'SETTINGS_MIGRATED',
    'ADDONS_NO',
    'ADDONS_MIGRATED']) AS code WITH OFFSET success_reason
)

SELECT
  DATE(minute) AS date,
  counts.* REPLACE (COALESCE(code, CAST(counts.success_reason AS STRING)) AS success_reason)
FROM
  counts
LEFT JOIN
  codes USING (success_reason)
