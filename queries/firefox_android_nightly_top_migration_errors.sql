WITH all_migration_attempts AS (
  SELECT
    client_info.client_id,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    TRUE as attempted,
    COALESCE(metrics.boolean.migration_history_any_failures, FALSE) AS history_failed,
    COALESCE(metrics.boolean.migration_bookmarks_any_failures, FALSE) AS bookmarks_failed,
    COALESCE(metrics.boolean.migration_open_tabs_any_failures, FALSE) AS open_tabs_failed,
    COALESCE(metrics.boolean.migration_fxa_any_failures, FALSE) AS fxa_failed,
    COALESCE(metrics.boolean.migration_gecko_any_failures, FALSE) AS gecko_failed,
    COALESCE(metrics.boolean.migration_logins_any_failures, FALSE) AS logins_failed,
    COALESCE(metrics.boolean.migration_settings_any_failures, FALSE) AS settings_failed,
    COALESCE(metrics.boolean.migration_addons_any_failures, FALSE) AS addons_failed,
    COALESCE(metrics.boolean.migration_telemetry_identifiers_any_failures, FALSE) AS telemetry_ids_failed
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
), client_status AS (
  SELECT
    MIN(minute) AS minute,
    LOGICAL_OR(history_failed) AS history_failed,
    LOGICAL_OR(bookmarks_failed) AS bookmarks_failed,
    LOGICAL_OR(open_tabs_failed) AS open_tabs_failed,
    LOGICAL_OR(fxa_failed) AS fxa_failed,
    LOGICAL_OR(gecko_failed) AS gecko_failed,
    LOGICAL_OR(logins_failed) AS logins_failed,
    LOGICAL_OR(settings_failed) AS settings_failed,
    LOGICAL_OR(addons_failed) AS addons_failed,
    LOGICAL_OR(telemetry_ids_failed) AS telemetry_ids_failed,
  FROM
    all_migration_attempts
  GROUP BY
    client_id
), attempt_info AS (
  SELECT
    minute,
    SUM(IF(history_failed, 1, 0)) AS history_failed_count,
    SUM(IF(bookmarks_failed, 1, 0)) AS bookmarks_failed_count,
    SUM(IF(open_tabs_failed, 1, 0)) AS open_tabs_failed_count,
    SUM(IF(fxa_failed, 1, 0)) AS fxa_failed_count,
    SUM(IF(gecko_failed, 1, 0)) AS gecko_failed_count,
    SUM(IF(logins_failed, 1, 0)) AS logins_failed_count,
    SUM(IF(settings_failed, 1, 0)) AS settings_failed_count,
    SUM(IF(addons_failed, 1, 0)) AS addons_failed_count,
    SUM(IF(telemetry_ids_failed, 1, 0)) AS telemetry_ids_failed_count,
  FROM
    client_status
  GROUP BY
    minute
)

SELECT
    DATE(minute) AS date,
    *
FROM attempt_info
