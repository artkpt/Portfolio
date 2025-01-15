import pandas as pd

import os
import json

from google.cloud import bigquery
from google.oauth2 import service_account

import mysql.connector

from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl_bigquery.log", encoding="utf-8"),  # บันทึก log เป็น UTF-8
        logging.StreamHandler()  # แสดง log บน console
    ]
)

#MySQL credential
host = os.getenv("HOST")
user = os.getenv("USER")
database= os.getenv("DATABASE")
password = os.getenv("PASSWORD")

#Bigquery credential
keyfile = "./credentials/ztec-etl-project-credential.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)


project_id = os.getenv("BQ_PROJECT_ID")
dataset_id = os.getenv("BQ_DATASET_ID")
table_id = os.getenv("BQ_TABLE_ID")



# extract data from MySQL
def extract_data_from_mysql(query):
    logging.info("extract_data_from_mysql")
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def load_data_to_bigquery(project_id,dataset_id, table_id, file_path):
    logging.info("load_data_to_bigquery")
    client = bigquery.Client(
        project=project_id,
        credentials= credentials
    )

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        schema=[
            bigquery.SchemaField("sku_id", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("sku_name", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("sku_brand", bigquery.SqlTypeNames.STRING),
            bigquery.SchemaField("item_id", bigquery.SqlTypeNames.STRING),
        ],
    )

    # read csv and load to bigquery
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    # check result
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
    

def main():
    query = "SELECT * FROM dim_sku"

    df = extract_data_from_mysql(query)
    print(df.head(5))
    df.to_csv("dim_sku.csv", index=False)

    load_data_to_bigquery(project_id,dataset_id, table_id, file_path="dim_sku.csv")

    

if __name__ == "__main__":

    main()

    