WITH client_attempts AS (
  SELECT
    client_info.client_id,
    'Overall' as migration_type,
    MIN(DATE(submission_timestamp)) AS date,
    SUM(COALESCE(metrics.timespan.migration_history_total_duration.value, 0)) AS history_duration,
    SUM(COALESCE(metrics.timespan.migration_bookmarks_total_duration.value, 0)) AS bookmarks_duration,
    SUM(COALESCE(metrics.timespan.migration_open_tabs_total_duration.value, 0)) AS open_tabs_duration,
    SUM(COALESCE(metrics.timespan.migration_fxa_total_duration.value, 0)) AS fxa_duration,
    SUM(COALESCE(metrics.timespan.migration_gecko_total_duration.value, 0)) AS gecko_duration,
    SUM(COALESCE(metrics.timespan.migration_logins_total_duration.value, 0)) AS logins_duration,
    SUM(COALESCE(metrics.timespan.migration_settings_total_duration.value, 0)) AS settings_duration,
    SUM(COALESCE(metrics.timespan.migration_addons_total_duration.value, 0)) AS addons_duration,
    SUM(COALESCE(metrics.timespan.migration_telemetry_identifiers_total_duration.value, 0)) AS telemetry_identifiers_duration,
    SUM(COALESCE(metrics.timespan.migration_search_total_duration.value, 0)) AS search_duration,
    SUM(COALESCE(metrics.timespan.migration_pinned_sites_total_duration.value, 0)) AS pinned_sites_duration
  FROM
    `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
  GROUP BY
    client_info.client_id
), total_durations AS (
  SELECT
    date,
    history_duration
     + bookmarks_duration
     + open_tabs_duration
     + fxa_duration
     + gecko_duration
     + logins_duration
     + settings_duration
     + addons_duration
     + telemetry_identifiers_duration
     + search_duration
     + pinned_sites_duration AS total_duration, 
  FROM
    client_attempts
), counts AS (
  SELECT
    date,
    CAST(FLOOR(total_duration / 50) As INT64) * 50 AS bucket,
    COUNT(*) AS count
  FROM
    total_durations
  WHERE
    total_duration > 0
  GROUP BY
    date,
    bucket
)

SELECT
  *
FROM
  counts
