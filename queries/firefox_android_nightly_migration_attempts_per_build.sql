WITH all_migration_attempts AS (
  SELECT
    client_info.client_id,
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    TRUE as attempted,
    client_info.app_build,
    COALESCE(metrics.boolean.migration_history_any_failures, FALSE) AS history_failed,
    COALESCE(metrics.boolean.migration_bookmarks_any_failures, FALSE) AS bookmarks_failed,
    COALESCE(metrics.boolean.migration_open_tabs_any_failures, FALSE) AS open_tabs_failed,
    COALESCE(metrics.boolean.migration_fxa_any_failures, FALSE) AS fxa_failed,
    COALESCE(metrics.boolean.migration_gecko_any_failures, FALSE) AS gecko_failed,
    COALESCE(metrics.boolean.migration_logins_any_failures, FALSE) AS logins_failed,
    COALESCE(metrics.boolean.migration_settings_any_failures, FALSE) AS settings_failed,
    COALESCE(metrics.boolean.migration_addons_any_failures, FALSE) AS addons_failed,
    COALESCE(metrics.boolean.migration_telemetry_identifiers_any_failures, FALSE) AS telemetry_ids_failed,
    COALESCE(metrics.boolean.migration_search_any_failures, FALSE) AS search_failed,
    COALESCE(metrics.boolean.migration_pinned_sites_any_failures, FALSE) AS pinned_sites_failed
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
), client_status AS (
  SELECT
    app_build,
    MIN(minute) AS minute,
    LOGICAL_OR(attempted) AS attempted,

    LOGICAL_OR(history_failed)
        OR LOGICAL_OR(bookmarks_failed)
        OR LOGICAL_OR(open_tabs_failed)
        OR LOGICAL_OR(fxa_failed)
        OR LOGICAL_OR(gecko_failed)
        OR LOGICAL_OR(logins_failed)
        OR LOGICAL_OR(settings_failed)
        OR LOGICAL_OR(addons_failed)
        OR LOGICAL_OR(telemetry_ids_failed) 
        OR LOGICAL_OR(search_failed)
        OR LOGICAL_OR(pinned_sites_failed) AS any_failed,

    LOGICAL_OR(history_failed)
        AND LOGICAL_OR(bookmarks_failed)
        AND LOGICAL_OR(open_tabs_failed)
        AND LOGICAL_OR(fxa_failed)
        AND LOGICAL_OR(gecko_failed)
        AND LOGICAL_OR(logins_failed)
        AND LOGICAL_OR(settings_failed)
        AND LOGICAL_OR(addons_failed)
        AND LOGICAL_OR(telemetry_ids_failed) 
        AND LOGICAL_OR(search_failed)
        AND LOGICAL_OR(pinned_sites_failed) AS all_failed
  FROM
    all_migration_attempts
  GROUP BY
    client_id,
    app_build
), attempt_info AS (
  SELECT
    minute,
    app_build,
    SUM(IF(attempted, 1, 0)) AS attempts,
    SUM(IF(NOT any_failed, 1, 0)) AS successes,
    SUM(IF(any_failed AND NOT all_failed, 1, 0)) AS partial_successes,
    SUM(IF(all_failed, 1, 0)) AS failures
  FROM
    client_status
  GROUP BY
    minute,
    app_build
)

SELECT
    DATE(minute) AS date,
    *
FROM attempt_info
