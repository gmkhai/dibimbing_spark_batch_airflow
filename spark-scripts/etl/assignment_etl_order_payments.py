from configs_etl_extraction import *


schema = "order_id string, payment_sequential string, payment_type string, payment_installments string, payment_value float"

df = spark.read.csv("/data/olist/olist_order_payments_dataset.csv", header=True, schema=schema)
df.write.mode("overwrite").jdbc(jdbc_url, "public.olist_order_payments", properties=jdbc_properties)

result_df = spark.read.jdbc(jdbc_url, "public.olist_order_payments", properties=jdbc_properties)
result_df.show(5)