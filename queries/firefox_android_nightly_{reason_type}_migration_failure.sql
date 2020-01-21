WITH  client_attempts AS (
  SELECT
    client_info.client_id,
    '{reason_type}' as reason_type,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    metrics.counter.migration_{reason_type}_failure_reason AS failure_reason,
    udf.get_key(metrics.labeled_string.migration_migration_versions, '{reason_type}') AS migration_version
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND metrics.counter.migration_{reason_type}_failure_reason IS NOT NULL
), counts AS (
  SELECT
    minute,
    reason_type AS migration_type,
    migration_version,
    failure_reason,
    COUNT(*) AS count
  FROM
    client_attempts
  GROUP BY
    minute,
    reason_type,
    migration_version,
    failure_reason
), codes AS (
  SELECT failure_reason + 1 AS failure_reason, code
  FROM UNNEST([
    'LOGINS_MP_CHECK',
    'LOGINS_UNSUPPORTED_LOGINS_DB',
    'LOGINS_ENCRYPTION',
    'LOGINS_GET',
    'LOGINS_RUST_IMPORT',
    'SETTINGS_MISSING_FHR_VALUE',
    'SETTINGS_WRONG_TELEMETRY_VALUE',
    'ADDON_QUERY',
    'FXA_CORRUPT_ACCOUNT_STATE',
    'FXA_UNSUPPORTED_VERSIONS',
    'FXA_SIGN_IN_FAILED',
    'FXA_CUSTOM_SERVER']) AS code WITH OFFSET failure_reason
)

SELECT
  DATE(minute) AS date,
  counts.* REPLACE (COALESCE(code, CAST(counts.failure_reason AS STRING)) AS failure_reason)
FROM
  counts
LEFT JOIN
  codes USING (failure_reason)
