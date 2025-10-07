import asyncio
import aiohttp
import pandas as pd
import os
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
from etl.db import NYC311Database
import shutil
import requests


ROW_LIMIT = 50000
DATA_OUTPUT =  str(Path.cwd().parent) + "/" + "data/311_requests"
API_MIN_DATE = "2010-01-01T00:00:00.000"


class NYC311RequestAPI:
    def __init__(self, url:str, db, api_token:str | None=None):
        self.url = url
        self.api_token = api_token
        self.db = db

    def chunk_request(self, where_query, chunk_num):
        """ Requests a chunk of data from the API based on the where_query. 
        Saves the data as a parquet file partitioned by borough."""

        url = (
            f"{self.url}?$limit={ROW_LIMIT}"
            f"&$where={where_query}"
            f"&$order=created_date,unique_key ASC"
        )

    
        resp = requests.get(url, headers={"X-App-Token": self.api_token} if self.api_token else {})
        data_response = resp.json()

        if data_response:
            current_chunk_df = pd.DataFrame(data_response)

            df_to_pq_table = pa.Table.from_pandas(current_chunk_df)
            pq.write_to_dataset(df_to_pq_table, root_path=DATA_OUTPUT, partition_cols=["borough"])

            print(f"Parquet #{chunk_num} saved to {DATA_OUTPUT} ({len(current_chunk_df)} rows)")
            return current_chunk_df
        else:
            print(f"No data returned for chunk #{chunk_num}")
            return None


    def request_api_data(self, query_min_date):
        chunk_num = 1
        query_max_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

        while True:
            try:
                where_query = f"created_date > '{query_min_date}' AND created_date < '{query_max_date}'"
                df = self.chunk_request(where_query, chunk_num)
                
                if df is None or df.empty:
                    print("Data querying finished")
                    break
            except Exception as e:
                print(f"Error during API request: {e}")
                break

            query_min_date = df["created_date"].max()
            chunk_num += 1
            
    def get_and_save_data(self, query_latest_data: bool = True):
        """ Retrieves data from the NYC 311 Requests API and loads parquet files to a DuckDB.
            If query_latest_data is True (default), it will only request data since the latest created_date in
            the existing database. Otherwise, it will request all data since API_MIN_DATE."""
        
        db = None
        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        if query_latest_data:
            
            print("Creating DB connection to check latest data")
            query_min_date = self.db.query_data_as_df("SELECT MAX(created_date) as max_date FROM nyc311")['max_date'].loc[0]
        else:
            folder = Path(DATA_OUTPUT)
            if folder.exists() and folder.is_dir():
                shutil.rmtree(folder)
                print(f"Deleted {folder} to refresh all data.")
            os.makedirs(DATA_OUTPUT, exist_ok=True)

            query_min_date = API_MIN_DATE
        print("Requesting all data since {} unil {}".format(query_min_date, current_time))
        self.request_api_data(query_min_date)