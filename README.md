# Fennec to Fenix Nightly Migration Dashboard

This repo has the queries that are fueling the fennec -> fenix migration dash. Those are under "queries".

It also has the deploy script, which is both creating scheduled queries and creating tables for each of those.
For each query, we:
- Template the query for multiple values (e.g. migration types)
- Create a "live" table that is overwritten every 15 minutes, and has that day's data only
- Create a "stable" table that has the history for that query, and is appended to daily
- Create a view that queries both of those, to retrieve the full up-to-date history

This method prevents us from constantly polling all of history for a live query,
while the historical data remains the same.

## Deploying Schemas

First, install requirements: `pip install -r requirements.txt`

To deploy all schemas:
```
python scripts/deploy.py deploy-queries
```

You can also deploy a single templated query:
```
python scripts/deploy.py deploy-queries --query firefox_android_nightly_migration_{migration_type}_attempts
```

Or, a post-templated schema, where the template has been filled in:
```
python scripts/deploy.py deploy-queries --query firefox_android_nightly_migration_settings_attempts
```
