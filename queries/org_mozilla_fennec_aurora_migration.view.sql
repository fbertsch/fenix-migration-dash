CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.analysis.org_mozilla_fennec_aurora_migration` AS
SELECT
    DATE(submission_timestamp) AS date,
    *
FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.migration_v1`
WHERE
    DATE(submission_timestamp) = current_date
UNION ALL
SELECT
    DATE(submission_timestamp) AS date,
    *
FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.migration_v1`
WHERE
    DATE(submission_timestamp) < current_date
