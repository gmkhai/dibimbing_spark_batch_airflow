from configs_etl_extraction import *


schema = "customer_id string, customer_unique_id string, customer_zip_code_prefix string, customer_city string, customer_state string"

df = spark.read.csv("/data/olist/olist_customers_dataset.csv", header=True, schema=schema)
df.write.mode("overwrite").jdbc(jdbc_url, "public.olist_customers", properties=jdbc_properties)

result_df = spark.read.jdbc(jdbc_url, 'public.olist_customers', properties=jdbc_properties)
result_df.show(5)