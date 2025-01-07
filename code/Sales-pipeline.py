import pandas as pd

import mysql.connector

import os
import dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),  # บันทึก log เป็น UTF-8
        logging.StreamHandler()  # แสดง log บน console
    ]
)


dotenv.load_dotenv()
host= os.getenv("HOST")
user= os.getenv("USER")
password= os.getenv("PASSWORD")
database= os.getenv("DATABASE")

def _staging(cur):
        logging.info("Dropping staging")
        drop_table = "drop table if exists staging"
        cur.execute(drop_table)

        logging.info("Creating staging")
        create_table_query = """
            create table if not exists staging(
                order_id varchar(20),
                customer_id varchar(255),
                order_date date,
                sku_id varchar(20),
                sales int,
                quantity SMALLINT,
                return_q SMALLINT,
                net_sales int,
                shipping_fee int,
                total int       
            )
        """
        cur.execute(create_table_query)

        logging.info("Inserting staging")
        transaction = pd.read_excel('C:\\Users\\art\\Desktop\\ZTEC\\data\\ztec.xlsx',sheet_name="transaction")
        for index, row in transaction.iterrows() :
            insert_statement = f"""
                insert into staging(
                    order_id, customer_id, order_date, sku_id, sales, quantity, return_q, net_sales,
                    shipping_fee, total           
                )
                values(
                    '{row["order_id"]}','{row["customer"]}','{row["order_date"]}','{row["sku_id"]}',
                    {row["sales"]},{row["จำนวน"]},{row["Returned quantity"]},
                    {row["ต้นทุนขายหักคูปองและcoin"]},{row["ค่าจัดส่งที่ชำระโดยผู้ซื้อ"]},{row["total"]}
                )
            """
            cur.execute(insert_statement)

def _table_for_join(cur):
    logging.info("Dropping temp")
    drop = "drop table if exists temp"
    cur.execute(drop)

    logging.info("Creating temp")
    create = """
        create table if not exists temp(
            order_id varchar(20),
            customer_id varchar(255),
            order_date date,
            promotion int
        )
    """
    cur.execute(create)

    logging.info("Inserting temp")
    insert = """
        insert into temp(order_id, customer_id, order_date, promotion)
        select order_id,customer_id,order_date, sum(sales)+avg(shipping_fee)-avg(total)
        from staging
        group by 1,2,3
    """
    cur.execute(insert)


def _staging_sales(cur):
    logging.info("Dropping staging_sales")
    drop = "drop table if exists staging_sales"
    cur.execute(drop)

    logging.info("Creating staging_sales")
    create = """
        create table if not exists staging_sales(
            order_id varchar(20),
            customer_id varchar(255),
            order_date date,
            sku_id varchar(20),
            sales int,
            quantity SMALLINT,
            return_q SMALLINT,
            net_sales int,
            shipping_fee int,
            total int,
            promotion float
        )
    """
    cur.execute(create)

    logging.info("Inserting staging_sales")
    insert = """
        insert into staging_sales(order_id ,customer_id ,order_date ,
        sku_id ,sales ,quantity ,return_q ,net_sales ,shipping_fee ,
        total, promotion)
        select s.order_id, s.customer_id, s.order_date, s.sku_id, s.sales, s.quantity, s.return_q, 
        s.net_sales, s.shipping_fee, s.total , t.promotion
        from staging s left join temp t
        on (s.order_id = t.order_id 
        and s.customer_id = t.customer_id
        and s.order_date = s.order_date)
    """
    cur.execute(insert)


def _staging_sales2(cur):
    logging.info("Dropping staging_sales2")
    drop = "drop table if exists staging_sales2"
    cur.execute(drop)

    logging.info("Creating staging_sales2")
    create = """
        create table if not exists staging_sales2(
            order_id varchar(20),
            customer_id varchar(255),
            order_date date,
            sku_id varchar(20),
            sales int,
            quantity SMALLINT,
            return_q SMALLINT,
            promotion float
        )
    """
    cur.execute(create)

    logging.info("Inserting staging_sales2")
    insert = """
        insert into staging_sales2(order_id ,customer_id ,order_date ,
        sku_id ,sales ,quantity ,return_q ,promotion)
        select order_id ,customer_id ,order_date ,
        sku_id ,sales-promotion as sales ,quantity ,return_q , 
        promotion / COUNT(*) OVER (PARTITION BY order_id) AS promotion
        from staging_sales
        where total > 0
    """
    cur.execute(insert)

def _sales_by_sku(cur):
    logging.info("Dropping sales_by_sku")
    drop = "drop table if exists sales_by_sku"
    cur.execute(drop)

    logging.info("Creating sales_by_sku")
    create = """
        create table if not exists sales_by_sku(
            order_id varchar(20),
            customer_id varchar(255),
            order_date date,
            sku_id varchar(20),
            sales int,
            quantity SMALLINT,
            return_q SMALLINT,
            promotion float,
            primary key(order_id,customer_id,order_date,sku_id),
            FOREIGN KEY (order_date) REFERENCES dim_date(date_id),
            FOREIGN KEY (sku_id) REFERENCES dim_sku(sku_id)
        )
    """
    cur.execute(create)

    logging.info("Inserting sales_by_sku")
    insert = """
        insert into sales_by_sku(order_id ,customer_id ,order_date ,
        sku_id ,sales ,quantity ,return_q ,promotion
        )
        SELECT 
            order_id ,customer_id ,order_date ,
            sku_id ,sales ,quantity ,return_q , promotion
        FROM 
            staging_sales2 s2
        WHERE
            s2.order_id NOT IN( SELECT s.order_id FROM  sales_by_sku s) AND
            s2.customer_id NOT IN( SELECT s.customer_id FROM  sales_by_sku s) AND
            s2.order_date NOT IN( SELECT s.order_date FROM  sales_by_sku s) AND
            s2.sku_id NOT IN( SELECT s.sku_id FROM  sales_by_sku s)
    """
    cur.execute(insert)

def _total_by_order(cur):
    logging.info("Dropping total_by_order")
    drop = "drop table if exists total_by_order"
    cur.execute(drop)

    logging.info("Creating total_by_order")
    create = """
        create table if not exists total_by_order(
            order_id varchar(20),
            customer_id varchar(255),
            order_date date,
            sales int,
            quantity SMALLINT,
            return_q SMALLINT,
            net_sales int,
            shipping_fee int,
            total int,
            promotion float,
            primary key(order_id,customer_id,order_date),
            FOREIGN KEY (order_date) REFERENCES dim_date(date_id)
        )
    """
    cur.execute(create)

    logging.info("Inserting total_by_order")
    insert = """
        insert into total_by_order(order_id ,customer_id ,order_date ,
        sales ,quantity ,return_q , net_sales, shipping_fee, total, promotion
        )
        SELECT 
            order_id ,customer_id ,order_date ,
            sum(sales) ,sum(quantity) ,sum(return_q), avg(net_sales),avg(shipping_fee), 
            avg(total), avg(promotion)
        FROM 
            staging_sales ss
        WHERE
            ss.order_id NOT IN( SELECT t.order_id FROM  total_by_order t) AND
            ss.customer_id NOT IN( SELECT t.customer_id FROM  total_by_order t) AND
            ss.order_date NOT IN( SELECT t.order_date FROM  total_by_order t)
        GROUP BY
            1,2,3
    """
    cur.execute(insert)

def _drop_all_staging(cur):
    logging.info("Dropping ALL Staging")
    drop = [
        "drop table if exists temp",
        "drop table if exists staging",
        "drop table if exists staging_sales",
        "drop table if exists staging_sales2"
    ]
    for query in drop:
        cur.execute(query)


def main():
    try:
        logging.info("Starting pipeline")    
        conn = mysql.connector.connect(
            host= host,
            user= user,
            password= password,
            database = database
        )

        cur = conn.cursor()

        _staging(cur)
        _table_for_join(cur)
        _staging_sales(cur)
        _staging_sales2(cur)
        _sales_by_sku(cur)
        _total_by_order(cur)
        _drop_all_staging(cur)

        conn.commit()

        logging.info("Success")

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



