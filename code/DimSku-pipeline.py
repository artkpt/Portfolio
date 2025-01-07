import pandas as pd

import mysql.connector

import os
import dotenv
import logging

dotenv.load_dotenv()

host = os.getenv("HOST")
user = os.getenv("USER")
database= os.getenv("DATABASE")
password = os.getenv("PASSWORD")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),  # บันทึก log เป็น UTF-8
        logging.StreamHandler()  # แสดง log บน console
    ]
)

def _drop_table_query(cur):
    logging.info("Dropping staging table")
    statement = "drop table if exists staging_dim_sku"
    cur.execute(statement)

def _create_table_queries(cur):
    logging.info("creating table sku")
    create_table_queries = ["""
        create table if not exists staging_dim_sku(
            sku_id varchar(20),
            sku_name varchar(255),
            sku_brand varchar(10),
            item_id varchar(20)
        )   
    """,
    """
        create table if not exists dim_sku(
            sku_id varchar(20),
            sku_name varchar(255),
            sku_brand varchar(10),
            item_id varchar(20),
            primary key(sku_id)
        )   
    """
    ]
    for query in create_table_queries:
        cur.execute(query)

def _insert_data_query(cur):
    logging.info("Inserting data to table staging_dim_sku")
    data = pd.read_excel('C:\\Users\\art\\Desktop\\ZTEC\\data\\ztec.xlsx', sheet_name= 'product')
    data = data.dropna()

    for index, row in data.iterrows():
        insert_statement = f"""
            insert into staging_dim_sku(
                sku_id,
                sku_name,
                sku_brand,
                item_id 
            )
            values(
                '{row["sku_id"]}',
                '{row["sku_name"]}',
                '{row["brand"]}',
                '{row["item_id"]}'
            )  
         """        
        cur.execute(insert_statement)
    
    logging.info("Inserting data to table dim_sku")
    insert_statement = f"""
            insert into dim_sku(
                sku_id,
                sku_name,
                sku_brand,
                item_id 
            )
            select sku_id, sku_name, sku_brand, item_id 
            from staging_dim_sku
            where sku_id not in (select sku_id from dim_sku)
         """      
    cur.execute(insert_statement)

def main():
    conn = None
    cur = None
    try:
        logging.info("starting pipeline")
        conn = mysql.connector.connect(
            host=host,
            user=user,
            database=database,
            password=password
        )

        cur = conn.cursor()

        _drop_table_query(cur)
        _create_table_queries(cur)
        _insert_data_query(cur)
        _drop_table_query(cur)
        
        conn.commit()

        logging.info("success pipeline")

    except Exception as e:
            logging.error(f"error: {e}")
            if conn:
                conn.rollback()  # Rollback การเปลี่ยนแปลง

    finally:
        if cur:
            cur.close()
            logging.info("cusor close")
        if conn:
            conn.close()
            logging.info("connection close")


if __name__ == "__main__":
    main()

