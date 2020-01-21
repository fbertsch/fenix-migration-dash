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
