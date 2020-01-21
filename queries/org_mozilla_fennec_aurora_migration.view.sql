CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration` AS
WITH migration_pings AS (
  SELECT
    DATE(submission_timestamp) AS date,
    submission_timestamp,
    `moz-fx-data-shared-prod.udf_js.gunzip`(payload) AS payload,
  FROM
    `moz-fx-data-shared-prod.payload_bytes_error.structured`
  WHERE
    document_namespace = 'org-mozilla-fennec-aurora'
    AND document_type = 'migration'
), all_migration_types AS (
  SELECT
    date,
    submission_timestamp,
    STRUCT(JSON_EXTRACT_SCALAR(payload, '$.client_info.client_id') AS client_id) AS client_info,
    STRUCT(
      STRUCT(
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.history.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_history_any_failures,
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.bookmarks.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_bookmarks_any_failures,
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.open_tabs.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_open_tabs_any_failures,
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.fxa.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_fxa_any_failures,
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.gecko.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_gecko_any_failures,
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.logins.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_logins_any_failures,
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.settings.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_settings_any_failures,
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.addons.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_addons_any_failures,
        CASE JSON_EXTRACT_SCALAR(payload, "$.metrics.boolean['migration.telemetry_identifiers.any_failures']") WHEN 'true' THEN TRUE ELSE FALSE END AS migration_telemetry_identifiers_any_failures
      ) AS boolean,
      STRUCT(
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.counter['migration.fxa.success_reason']") AS INT64) AS migration_fxa_success_reason,
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.counter['migration.logins.success_reason']") AS INT64) AS migration_logins_success_reason,
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.counter['migration.settings.success_reason']") AS INT64) AS migration_settings_success_reason,
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.counter['migration.addons.success_reason']") AS INT64) AS migration_addons_success_reason,
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.counter['migration.fxa.failure_reason']") AS INT64) AS migration_fxa_failure_reason,
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.counter['migration.logins.failure_reason']") AS INT64) AS migration_logins_failure_reason,
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.counter['migration.settings.failure_reason']") AS INT64) AS migration_settings_failure_reason,
        SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.counter['migration.addons.failure_reason']") AS INT64) AS migration_addons_failure_reason
      ) AS counter,
      STRUCT(
        `moz-fx-data-shared-prod.analysis.udf_json_extract_string_map`(JSON_EXTRACT(payload, "$.metrics.labeled_string['migration.migration_versions']")) AS migration_migration_versions
      ) AS labeled_string,
      STRUCT(
        STRUCT(
            SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.timespan['migration.bookmarks.duration'].time_unit") AS STRING) AS time_unit,
            SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.timespan['migration.bookmarks.duration'].value") AS INT64) AS value
        ) AS migration_bookmarks_duration,
        STRUCT(
            SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.timespan['migration.history.duration'].time_unit") AS STRING) AS time_unit,
            SAFE_CAST(JSON_EXTRACT_SCALAR(payload, "$.metrics.timespan['migration.history.duration'].value") AS INT64) AS value
        ) AS migration_history_duration
      ) AS timespan
    ) AS metrics
  FROM
    migration_pings
)

SELECT *
FROM all_migration_types
WHERE DATE(submission_timestamp) <= '2020-01-20'
UNION ALL
SELECT
    DATE(submission_timestamp),
    submission_timestamp,
    STRUCT(client_info.client_id AS client_id) AS client_info,
    STRUCT(
        STRUCT(
            metrics.boolean.migration_history_any_failures,
            metrics.boolean.migration_bookmarks_any_failures,
            metrics.boolean.migration_open_tabs_any_failures,
            metrics.boolean.migration_fxa_any_failures,
            metrics.boolean.migration_gecko_any_failures,
            metrics.boolean.migration_logins_any_failures,
            metrics.boolean.migration_settings_any_failures,
            metrics.boolean.migration_addons_any_failures,
            metrics.boolean.migration_telemetry_identifiers_any_failures
        ) AS boolean,
        STRUCT(
            metrics.counter.migration_fxa_success_reason,
            metrics.counter.migration_logins_success_reason,
            metrics.counter.migration_settings_success_reason,
            metrics.counter.migration_addons_success_reason,
            metrics.counter.migration_fxa_failure_reason,
            metrics.counter.migration_logins_failure_reason,
            metrics.counter.migration_settings_failure_reason,
            metrics.counter.migration_addons_failure_reason
        ) AS counter,
        STRUCT(
            metrics.labeled_string.migration_migration_versions
        ) AS labeled_string,
        metrics.timespan AS timespan
    ) AS metrics
FROM `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.migration_v1`
WHERE DATE(submission_timestamp) = current_date
UNION ALL
SELECT
    DATE(submission_timestamp),
    submission_timestamp,
    STRUCT(client_info.client_id AS client_id) AS client_info,
    STRUCT(
        STRUCT(
            metrics.boolean.migration_history_any_failures,
            metrics.boolean.migration_bookmarks_any_failures,
            metrics.boolean.migration_open_tabs_any_failures,
            metrics.boolean.migration_fxa_any_failures,
            metrics.boolean.migration_gecko_any_failures,
            metrics.boolean.migration_logins_any_failures,
            metrics.boolean.migration_settings_any_failures,
            metrics.boolean.migration_addons_any_failures,
            metrics.boolean.migration_telemetry_identifiers_any_failures
        ) AS boolean,
        STRUCT(
            metrics.counter.migration_fxa_success_reason,
            metrics.counter.migration_logins_success_reason,
            metrics.counter.migration_settings_success_reason,
            metrics.counter.migration_addons_success_reason,
            metrics.counter.migration_fxa_failure_reason,
            metrics.counter.migration_logins_failure_reason,
            metrics.counter.migration_settings_failure_reason,
            metrics.counter.migration_addons_failure_reason
        ) AS counter,
        STRUCT(
            metrics.labeled_string.migration_migration_versions
        ) AS labeled_string,
        metrics.timespan AS timespan
    ) AS metrics
FROM `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.migration_v1`
WHERE 
    DATE(submission_timestamp) < current_date
    AND DATE(submission_timestamp) > '2020-01-20'
