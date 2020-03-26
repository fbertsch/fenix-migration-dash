WITH  client_attempts AS (
  SELECT
    client_info.client_id,
    '{migration_type}' as migration_type,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    metrics.counter.migration_{migration_type}_failure_reason AS failure_reason,
    udf.get_key(metrics.labeled_string.migration_migration_versions, '{migration_type}') AS migration_version
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND metrics.counter.migration_{migration_type}_failure_reason IS NOT NULL
), counts AS (
  SELECT
    minute,
    migration_type,
    migration_version,
    failure_reason,
    COUNT(*) AS count
  FROM
    client_attempts
  GROUP BY
    minute,
    migration_type,
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
    'FXA_CUSTOM_SERVER',
    'HISTORY_MISSING_DB_PATH',
    'HISTORY_RUST_EXCEPTION',
    'HISTORY_TELEMETRY_EXCEPTION',
    'BOOKMARKS_MISSING_DB_PATH',
    'BOOKMARKS_RUST_EXCEPTION',
    'BOOKMARKS_TELEMETRY_EXCEPTION',
    'LOGINS_UNEXPECTED_EXCEPTION',
    'LOGINS_MISSING_PROFILE',
    'OPEN_TABS_MISSING_PROFILE',
    'OPEN_TABS_MIGRATE_EXCEPTION',
    'OPEN_TABS_NO_SNAPSHOT',
    'OPEN_TABS_RESTORE_EXCEPTION',
    'GECKO_MISSING_PROFILE',
    'GECKO_UNEXPECTED_EXCEPTION',
    'SETTINGS_MIGRATE_EXCEPTION',
    'ADDON_UNEXPECTED_EXCEPTION',
    'TELEMETRY_IDENTIFIERS_MISSING_PROFILE',
    'TELEMETRY_IDENTIFIERS_MIGRATE_EXCEPTION',
    'SEARCH_NO_DEFAULT',
    'SEARCH_NO_MATCH',
    'SEARCH_EXCEPTION',
    'PINNED_SITES_MISSING_DB_PATH',
    'PINNED_SITES_MISSING_READ_FAILURE'
    ]) AS code WITH OFFSET failure_reason
)

SELECT
  DATE(minute) AS date,
  counts.* REPLACE (COALESCE(code, CAST(counts.failure_reason AS STRING)) AS failure_reason)
FROM
  counts
LEFT JOIN
  codes USING (failure_reason)
