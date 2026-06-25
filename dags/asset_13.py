from airflow.sdk import dag, task, asset
from pendulum import datetime
import os

@asset(
    schedule="@daily",
    uri="/opt/airflow/logs/data/data_extract.txt", # This is where the data will be stored i.e., it is asset's location
    name="fetch_data"
)
def fetch_data(self):
    
    # ensure the directory exists
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    # simulate data fetching by writing to a file
    with open(self.uri, 'w') as f:
        f.write("Data fetched successfully")

    print(f"Data written to {self.uri}")

