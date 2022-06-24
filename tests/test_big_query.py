import json

from google.cloud import bigquery

from b_q import uploadbq

import pandas as pd

from deepbnb.data.big_query import UploadBigquery

def test_local_file_bigquery_upload():
    # client = bigquery.Client('dev-aicam')
    # data_set_id = 'booking'
    # project_id = 'dev-aicam'
    # datafile = "locations-nz.txt"

    # UploadBigquery(data, data_set_id, project_id)
    test_file = "resources/test_json.json"
    uploadbq(test_file)

    # query = """
    #         SELECT * FROM `dev-aicam.booking.test-locations`
    # """
    # locations_query = client.query(query)
    # locations = []
    # for row in locations_query:
    #     locations.append(row)
    #
    # print(locations)
