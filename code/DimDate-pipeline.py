import os
from dotenv import load_dotenv

import pandas as pd
from datetime import datetime

import mysql.connector
import logging

# โหลด .env
load_dotenv()

# ดึง Credential จากไฟล์ .env
host = os.getenv("HOST")
user = os.getenv("USER")
database= os.getenv("DATABASE")
password = os.getenv("PASSWORD")


# ตั้งค่าระบบ logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),  # บันทึก log เป็น UTF-8
        logging.StreamHandler()  # แสดง log บน console
    ]
)

def _drop_table(cur):
        logging.info("กำลังลบตาราง staging_dim_date")
        drop_table = "DROP TABLE IF EXISTS staging_dim_date"
        cur.execute(drop_table)

def _create_dim_date(cur):
        logging.info("กำลังสร้างตาราง staging_dim_date")
        create_table_query = ["""
        CREATE TABLE IF NOT EXISTS staging_dim_date (
            date_id DATE,
            date_year INT,
            date_month INT,
            date_monthname VARCHAR(5),
            date_day INT,
            date_dayname VARCHAR(5)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id DATE,
            date_year INT,
            date_month INT,
            date_monthname VARCHAR(5),
            date_day INT,
            date_dayname VARCHAR(5),
            primary key (date_id)
        )
        """
        ]
        for query in create_table_query:
            cur.execute(query)


def _process(cur):
        logging.info("เริ่มการสร้างข้อมูล dim_date")
        current_year = datetime.now().year
        next_year = current_year + 1

        start_date = f"{current_year}-01-01"
        end_date = f"{next_year}-12-31"

        date_range = pd.date_range(start=start_date, end=end_date)
        dim_date = pd.DataFrame({
            "date_id": date_range,
            "date_year": date_range.year,
            "date_month": date_range.month,
            "date_monthname": date_range.strftime('%b'),
            "date_day": date_range.day,
            "date_dayname": date_range.strftime('%a')
        })

        for index, row in dim_date.iterrows():
            insert_statement = f"""
            INSERT INTO staging_dim_date(
                date_id, date_year, date_month, date_monthname, date_day, date_dayname
            )
            VALUES (
                '{row["date_id"]}', {row["date_year"]}, {row["date_month"]}, 
                '{row["date_monthname"]}', {row["date_day"]}, '{row["date_dayname"]}'
            )
            """
            cur.execute(insert_statement)

        insert_statement = f"""
            INSERT INTO dim_date(
                date_id, date_year, date_month, date_monthname, date_day, date_dayname
            )
            SELECT * FROM staging_dim_date 
            WHERE date_id NOT IN (SELECT date_id FROM dim_date)
            """
        cur.execute(insert_statement)

def main():
    conn = None
    cur = None
    try:
        logging.info("เริ่มต้น pipeline")
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cur = conn.cursor()

        _drop_table(cur)
        _create_dim_date(cur)
        _process(cur)
        _drop_table(cur)

        conn.commit()
        logging.info("Pipeline ทำงานสำเร็จ")

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




