import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pandas.core.frame import DataFrame

from deepbnb.data.schema import airbnb_disco_schema


class UploadBigquery:
    def __init__(self, project_id: str, data_set_id: str):
        self.client = bigquery.Client()
        self.data_set_id = data_set_id
        self.project_id = project_id
        self.job_config = bigquery.LoadJobConfig(schema=airbnb_disco_schema)

    def move_to_his(self):
        source_list = [
            f"{self.project_id}.{self.data_set_id}.airbnb_vrbo",
            f"{self.project_id}.{self.data_set_id}.hotels",
            f"{self.project_id}.{self.data_set_id}.hotels_1",
        ]
        temp_list = [
            f"{self.project_id}.{self.data_set_id}.airbnb_his",
            f"{self.project_id}.{self.data_set_id}.hotels_his",
            f"{self.project_id}.{self.data_set_id}.hotels_his",
        ]
        for s, d in zip(source_list, temp_list):
            job_config = bigquery.QueryJobConfig(
                allow_large_results=True, destination=d, write_disposition="WRITE_APPEND"
            )

            sql = f"select * from {s}"
            # Start the query, passing in the extra configuration.
            query_job = self.client.query(sql, job_config=job_config)  # Make an API request.
            query_job.result()  # Wait for the job to complete.
            self.client.query(f"truncate table {s}")
            query_job.result()
            print(f"success move {s}")

    def table_exist(self, table_id):
        try:
            self.client.get_table(table_id)  # Make an API request.
            print("Table {} already exists.".format(table_id))
            return True
        except NotFound:
            print("Table {} is not found.".format(table_id))
            return False

    def upload_dict(self, data: dict, table: str):
        jdict = [data]
        tbl_str = f"{self.project_id}.{self.data_set_id}.{table}"
        job = self.client.load_table_from_json(jdict, tbl_str, job_config=self.job_config)
        job.result()

    def upload(self, buff, table_id):
        if type(buff) != DataFrame:
            src = pd.DataFrame(buff)
        else:
            src = buff
        # try:
        #     tbl = self.client.get_table(table_id)
        #     self.client.insert_rows_from_dataframe(tbl, src)
        # except:
        job = self.client.load_table_from_dataframe(
            src, f"{self.project_id}.{self.data_set_id}.{table_id}"
        )
        job.result()

    def readRegion(self, website, status):
        query = f"""
            SELECT region as city,SubRegion,{website} as city_url FROM `{self.project_id}.{self.data_set_id}.region` 
        """
        query_job = self.client.query(
            query
        )  # Make an API request.where group_id={status}
        return query_job

    def readDiscover(self, website, table_name, and_qury=""):
        query = f"""
            SELECT id FROM `{self.project_id}.{self.data_set_id}.{table_name}` where website='{website}' {and_qury}
        """
        query_job = self.client.query(query)  # Make an API request.
        result = []
        for row in query_job:
            result.append(row["id"])
        return result

    def readStartUrls(self, website, table_name, status, and_qury=""):
        query = f"""
            SELECT distinct id,starting_url FROM `{self.project_id}.{self.data_set_id}.{table_name}` a join `{self.project_id}.{self.data_set_id}.region` b  on  a.region=b.region where a.website='{website}'  {and_qury}
        """

        query_job = self.client.query(query)  # Make an API request.
        return query_job

    def readStartUrls_rto(self, website, table_name, group, and_qury=""):
        query = f"""
            SELECT distinct id,starting_url FROM `{self.project_id}.{self.data_set_id}.{table_name}` a join `{self.project_id}.{self.data_set_id}.region` b  on  a.region=b.region where a.website='{website}'  and b.group_id>{group} {and_qury}
        """

        query_job = self.client.query(query)  # Make an API request.
        return query_job

    def readExist_hotels(self, table_name):
        query = f"""
            SELECT distinct id,url FROM `{self.project_id}.{self.data_set_id}.{table_name}` 
        """

        query_job = self.client.query(query)  # Make an API request.
        return query_job
