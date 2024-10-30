import os
import pyspark
from dotenv import load_dotenv
from pathlib import Path

# load .env file
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

# set value postgres
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

spark_context = pyspark.SparkContext.getOrCreate(
    conf=pyspark.SparkConf()\
        .setAppName('dibimbing_read_assignment')\
        .setMaster(spark_host)\
        .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar"))

spark_context.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(spark_context.getOrCreate())


jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}"
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

olist_orders_df = spark.read.jdbc(jdbc_url, 'public.olist_orders', properties=jdbc_properties)
olist_order_payments_df = spark.read.jdbc(jdbc_url, 'public.olist_order_payments', properties=jdbc_properties)
olist_customers = spark.read.jdbc(jdbc_url, 'public.olist_customers', properties=jdbc_properties)

olist_orders_df.show(5)
olist_order_payments_df.show(5)
olist_customers.show(5)


# create or replace temp view for generate table SQL temp
olist_orders_df.createOrReplaceTempView('olist_orders')
olist_order_payments_df.createOrReplaceTempView('olist_order_payments')
olist_customers.createOrReplaceTempView('olist_customers')

print("ANALYSIS IDEA")

"""
Case:
    Team marketing e-commerce ingin memberikan penawaran khusus untuk wilayah sao paolo
    berupa diskon order tergantung pada metode pembayaran (payment_type).
    untuk metode pembayaran yang paling tertinggi nilai transaksi pembayaran nanti yang akan mendapatkan diskon order
    Syarat diberikan diskon berdasarkan berapa banyak status order yang telah delivered
"""

spark.sql("""
    SELECT 
        o_order_pays.payment_type,
        SUM(o_order_pays.payment_value) AS total_payment_value
    FROM olist_orders AS o_orders
    INNER JOIN olist_customers AS o_customers
    ON o_orders.customer_id = o_customers.customer_id
    INNER JOIN olist_order_payments AS o_order_pays
    ON o_orders.order_id = o_order_pays.order_id
    WHERE 
        o_customers.customer_city = 'sao paulo' 
        AND o_orders.order_status = 'delivered'
    GROUP BY payment_type
    ORDER BY total_payment_value DESC 
    LIMIT 1;

""").show()

print("UNTUK PAYMENT TYPE YANG PALING BANYAK NILAI TRANSAKSI DI SAO PAULO ADALAH CREDIT CARD")