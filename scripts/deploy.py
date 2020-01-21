import datetime
import json
import os
import subprocess
import click
import sys
import logging

from google.protobuf import json_format
from google.cloud import bigquery
from pathlib import Path
from pprint import pprint

# IMPORTANT:
#
# 1. You need to have the bq cli installed for this to run
# 2. The first time you run this sample, you may need to authorize: https://www.gstatic.com/bigquerydatatransfer/oauthz/auth?client_id=433065040935-hav5fqnc9p9cht3rqneus9115ias2kn1.apps.googleusercontent.com&scope=https://www.googleapis.com/auth/bigquery%20https://www.googleapis.com/auth/drive&redirect_uri=urn:ietf:wg:oauth:2.0:oob

project = 'moz-fx-data-derived-datasets'
dest_dataset = 'analysis'

bq_client = bigquery.Client(project=project)

root = Path(__file__).parent.parent / 'queries'

existing_schedules = subprocess.check_output(['bq', 'ls', '--transfer_config', '--transfer_location=us', '--project_id', project]).decode('utf-8')
schedules = [s.strip().split('   ') for s in existing_schedules.split('\n')[2:]]

query_parameters = {
    # https://github.com/mozilla-mobile/android-components/blob/master/components/support/migration/src/main/java/mozilla/components/support/migration/TelemetryHelpers.kt#L8
    "migration_type": [
        "history",
        "bookmarks",
        "open_tabs",
        "fxa",
        "gecko",
        "logins",
        "settings",
        "addons",
        "telemetry_identifiers"
    ],
    "reason_type": [
        "fxa",
        "logins",
        "settings",
        "addons"
    ],
    "timespan_migration_type": [
        "history",
        "bookmarks"
    ]
}

def _deploy_queries(query=None):
    queries = []
    for filepath in root.iterdir():
        if not filepath.is_file():
            continue

        if 'view' in filepath.name or filepath.name.startswith('.'):
            continue

        with open(filepath) as f:
            base_query = f.read()

        destination_base = filepath.name.split('.')[0]

        has_param = False
        for query_param, values in query_parameters.items():
            if ('{' + query_param + '}') in destination_base:
                for value in values:
                    fstr = {query_param: value}
                    queries.append((destination_base, destination_base.format(**fstr), base_query, fstr))
                break
        else:
            queries.append((destination_base, destination_base, base_query, {}))

    if query is not None:
        queries = [q for q in queries if q[0] == query or q[1] == query]

    for _, destination_base, base_query, addl_params in queries:
        print(f"\nCreating table {destination_base}")

        query_types = [
            {
                "type": "run",
                "start_date": "current_date",
                "end_date": "current_date",
                "dataset": "telemetry_live",
                "destination_table": f"{destination_base}_live",
                "core_version_suffix": "_v10",
                "partitioning_field": "",
            },
            {
                "type": "scheduled",
                "start_date": "current_date",
                "end_date": "current_date",
                "dataset": "telemetry_live",
                "schedule": "every 15 minutes",
                "destination_table": f"{destination_base}_live",
                "core_version_suffix": "_v10",
                "partitioning_field": "",
                "display_name": f"{destination_base}_live",
                "write_disposition": "WRITE_TRUNCATE"
            },
            {
                "type": "run",
                "start_date": "\"2020-01-10\"",
                "end_date": "DATE_SUB(current_date, interval 2 day)",
                "dataset": "telemetry",
                "destination_table": f"{destination_base}_stable",
                "core_version_suffix": "",
                "partitioning_field": "date"
            },
            {
                "type": "scheduled",
                "start_date": "DATE_SUB(current_date, interval 1 day)",
                "end_date": "DATE_SUB(current_date, interval 1 day)",
                "dataset": "telemetry",
                "schedule": "every day 00:20",
                "destination_table": f"{destination_base}_stable",
                "core_version_suffix": "",
                "partitioning_field": "date",
                "display_name": f"{destination_base}_stable",
                "write_disposition": "WRITE_APPEND"
            },
        ]

        for config in query_types:
            config.update(addl_params)
            query = base_query.format(**config)

            if config['type'] == "scheduled":
                for s in schedules:
                    if len(s) < 2:
                        continue
                    resource_id, display_name = s[0], s[1]
                    if display_name == config["display_name"]:
                        print("  ...Removing scheduled query" + resource_id)
                        os.system(f"bq rm -f --transfer_config \"{resource_id}\"")

                params = {
                    "query": query.replace("\n", " ").replace("'", "\""),
                    "destination_table_name_template": config["destination_table"],
                    "write_disposition": config["write_disposition"],
                    "partitioning_field": config["partitioning_field"],
                }

                mk_cmd = (
                    f'bq mk '
                    f'--transfer_config '
                    f'--project_id="{project}" '
                    f'--target_dataset="{dest_dataset}" '
                    f'--display_name="{config["display_name"]}" '
                    f'--params=\'{json.dumps(params)}\' '
                    f'--data_source=scheduled_query '
                    f'--schedule="{config["schedule"]}" '
                )

                print("  ...Creating scheduled query '{}'".format(config["display_name"]))
                os.system(mk_cmd)

            elif config['type'] == "run":
                sql = f'CREATE OR REPLACE TABLE `{dest_dataset}.{config["destination_table"]}` '
                if config.get("partitioning_field"):
                    sql += f'PARTITION BY {config["partitioning_field"]} '
                sql += f'AS {query}'

                print("  ...Creating " + config["destination_table"])
                job = bq_client.query(sql)
                job.result()

        tables = set([c["destination_table"] for c in query_types])
        query = ' UNION ALL '.join([f'SELECT * FROM `{project}.{dest_dataset}.{t}`' for t in tables])
        sql = f'CREATE OR REPLACE VIEW `{dest_dataset}.{destination_base}` AS {query}'
        print("  ...Creating View")
        job = bq_client.query(sql)
        job.result()


@click.command()
@click.option("--query", required=False)
def deploy_queries(query):
    _deploy_queries(query)


@click.group()
def main(args=None):
    """Command line utility"""
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)


main.add_command(deploy_queries)


if __name__ == "__main__":
    sys.exit(main())
