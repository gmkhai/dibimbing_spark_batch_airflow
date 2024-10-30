from configs_etl_extraction import *


schema = "order_id string, customer_id string, order_status string"

df = spark.read.csv("/data/olist/olist_orders_dataset.csv", header=True, schema=schema)
df.write.mode("overwrite").jdbc(jdbc_url, "public.olist_orders", properties=jdbc_properties)

result_df = spark.read.jdbc(jdbc_url, "public.olist_orders", properties=jdbc_properties)
result_df.show(5)