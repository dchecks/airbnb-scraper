import datetime
import json
import logging

from google.cloud import bigquery


def uploadbq(response):

    # Construct a BigQuery client object.
    client = bigquery.Client("dev-aicam")


    # data = json.load(response.body)
    # result = [json.dumps(record) for record in data]
    # for i in result:
    #     result[i].write(i + '\n')

    table = client.get_table('dev-aicam.json_test.states_table')

    job_config = bigquery.job.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    load_job = client.load_table_from_json(response, table, job_config=job_config)  # Waits for the job to complete.
