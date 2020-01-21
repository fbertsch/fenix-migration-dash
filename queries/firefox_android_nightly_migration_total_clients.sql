WITH pings AS (
  SELECT
    DATE(submission_timestamp) AS date,
    submission_timestamp,
    document_type,
    `moz-fx-data-shared-prod.udf_js.gunzip`(payload) AS payload,
  FROM
    `moz-fx-data-shared-prod.payload_bytes_error.structured`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND document_namespace = "org-mozilla-fennec-aurora"
    AND (document_type = "baseline" OR document_type = "migration")
), migrated_client_pings AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    JSON_EXTRACT(payload, "$.client_info.client_id") AS client_id
  FROM
    pings
  WHERE
    document_type = "baseline"
    AND DATE(submission_timestamp) <= '2020-01-20'
  UNION ALL
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    JSON_EXTRACT(payload, "$.client_info.client_id") AS client_id
  FROM
    pings
  WHERE
    document_type = "migration"
    AND STRPOS(payload, "telemetry_identifiers") > 0
    AND DATE(submission_timestamp) <= '2020-01-20'
  UNION ALL
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    client_info.client_id AS client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.migration_v1`
  WHERE
    DATE(submission_timestamp) >= '2020-01-20'
    AND DATE(submission_timestamp) < current_date
  UNION ALL
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    client_info.client_id AS client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.migration_v1`
  WHERE
    DATE(submission_timestamp) = current_date
), migrated_clients AS (
  SELECT
    minute,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    migrated_client_pings
  GROUP BY
    minute
), unmigrated_clients AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, MINUTE) AS minute,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.{dataset}.core{core_version_suffix}`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND normalized_app_name = "Fennec"
    AND normalized_os = "Android"
    AND metadata.uri.app_update_channel = "nightly"
  GROUP BY
    minute
), all_clients AS (
  SELECT
    minute,
    COALESCE(a.client_count, 0) AS migrated_client_count,
    COALESCE(b.client_count, 0) AS unmigrated_client_count,
    COALESCE(a.client_count, 0) + COALESCE(b.client_count, 0) AS client_count
  FROM
    migrated_clients a
  FULL OUTER JOIN
    unmigrated_clients b
    USING (minute)
)

SELECT
  DATE(minute) AS date,
  *
FROM
  all_clients
