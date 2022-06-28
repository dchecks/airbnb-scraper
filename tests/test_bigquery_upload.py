import os

import pytest
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "dev-aicam-a12eebd3222c.json"

def test_bigquery_upload():
    project_id = "dev-aicam"
    data_set = "booking"
    table = "pipeline-dev"
    tbl_str = f"{project_id}.{data_set}.{table}"
    client = bigquery.Client()
    test_properties = [{'name': "Sabbi's Backpackers", 'place_id': 'ChIJDY_rVj2hCW0RgKuiQ2HvAAQ', 'url': 'https://www.airbnb.com/rooms/21284566'}]
    client.load_table_from_json(test_properties, tbl_str)

