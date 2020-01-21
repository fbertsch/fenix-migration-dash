WITH all_migration_attempts AS (
  SELECT
    client_info.client_id,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    TRUE as attempted,
    COALESCE(metrics.boolean.migration_history_any_failures, FALSE)
      OR COALESCE(metrics.boolean.migration_bookmarks_any_failures, FALSE)
      OR COALESCE(metrics.boolean.migration_open_tabs_any_failures, FALSE)
      OR COALESCE(metrics.boolean.migration_fxa_any_failures, FALSE)
      OR COALESCE(metrics.boolean.migration_gecko_any_failures, FALSE)
      OR COALESCE(metrics.boolean.migration_logins_any_failures, FALSE)
      OR COALESCE(metrics.boolean.migration_settings_any_failures, FALSE)
      OR COALESCE(metrics.boolean.migration_addons_any_failures, FALSE)
      OR COALESCE(metrics.boolean.migration_telemetry_identifiers_any_failures, FALSE) AS any_failures,
    COALESCE(metrics.boolean.migration_history_any_failures, TRUE)
      AND COALESCE(metrics.boolean.migration_bookmarks_any_failures, TRUE)
      AND COALESCE(metrics.boolean.migration_open_tabs_any_failures, TRUE)
      AND COALESCE(metrics.boolean.migration_fxa_any_failures, TRUE)
      AND COALESCE(metrics.boolean.migration_gecko_any_failures, TRUE)
      AND COALESCE(metrics.boolean.migration_logins_any_failures, TRUE)
      AND COALESCE(metrics.boolean.migration_settings_any_failures, TRUE)
      AND COALESCE(metrics.boolean.migration_addons_any_failures, TRUE)
      AND COALESCE(metrics.boolean.migration_telemetry_identifiers_any_failures, TRUE) AS all_failures
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
), client_status AS (
  SELECT
    MIN(minute) AS minute,
    LOGICAL_OR(attempted) AS attempted,
    NOT LOGICAL_OR(any_failures) AS succeeded,
    LOGICAL_OR(any_failures) AND NOT LOGICAL_AND(all_failures) AS partially_succeeded,
    LOGICAL_AND(all_failures) AS failed
  FROM
    all_migration_attempts
  GROUP BY
    client_id
), attempt_info AS (
  SELECT
    minute,
    SUM(IF(attempted, 1, 0)) AS attempts,
    SUM(IF(succeeded, 1, 0)) AS successes,
    SUM(IF(partially_succeeded, 1, 0)) AS partial_successes,
    SUM(IF(failed, 1, 0)) AS failures
  FROM
    client_status
  GROUP BY
    minute
)

SELECT
    DATE(minute) AS date,
    *
FROM attempt_info
