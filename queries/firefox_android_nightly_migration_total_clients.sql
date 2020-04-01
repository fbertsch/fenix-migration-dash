WITH migrated_client_pings AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    client_info.client_id AS client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.migration_v1`
  UNION ALL
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    client_info.client_id AS client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.migration_v1`
  WHERE
    DATE(submission_timestamp) = current_date
  UNION ALL
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    client_info.client_id AS client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.baseline_v1`
  UNION ALL
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    client_info.client_id AS client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.baseline_v1`
  WHERE
    DATE(submission_timestamp) = current_date
), migrated_clients AS (
  SELECT
    hour,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    migrated_client_pings
  WHERE
    DATE(hour) >= {start_date}
    AND DATE(hour) <= {end_date}
  GROUP BY
    hour
), unmigrated_clients AS (
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS hour,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.{dataset}.core{core_version_suffix}`
  WHERE
    DATE(submission_timestamp) >= {start_date}
    AND DATE(submission_timestamp) <= {end_date}
    AND normalized_app_name = "Fennec"
    AND normalized_os = "Android"
    AND metadata.uri.app_update_channel = "beta"
  GROUP BY
    hour
), all_clients AS (
  SELECT
    hour,
    COALESCE(a.client_count, 0) AS migrated_client_count,
    COALESCE(b.client_count, 0) AS unmigrated_client_count,
    COALESCE(a.client_count, 0) + COALESCE(b.client_count, 0) AS client_count
  FROM
    migrated_clients a
  FULL OUTER JOIN
    unmigrated_clients b
    USING (hour)
)

SELECT
  DATE(hour) AS date,
  *
FROM
  all_clients
