import os
import pyspark
from dotenv import load_dotenv
from pathlib import Path

# load variable in file .env
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

# get value from variable in .env
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv("POSTGRES_DW_DB")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

# create spark context
spark_context = pyspark.SparkContext.getOrCreate(
    conf=pyspark.SparkConf()\
        .setAppName('dibimbing_etl_assignment')\
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
