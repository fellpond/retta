# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/customer_metadata.json')

df = spark.read.csv('/mnt/bronze/sharepoint/input/Customer/*', header=True, sep=',', quote= "\"", escape="\"")
df_final = df.select(list(config.keys()))
print(config.keys())

df_final = cf.check_for_json_value(df_final)

# display(df)
# df_final =df_final.withColumn('CustomerType', F.get_json_object(F.col("CustomerType"), "$.Value"))
# df_final =df_final.withColumn('Accountmanager', F.get_json_object(F.col("Accountmanager"), "$.DisplayName"))
# df_final =df_final.withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))
# df_final =df_final.withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))
#VAETS missing

# rename columns
df_final = cf.df_column_rename(df_final,config)
display(df_final)
df.printSchema()

# COMMAND ----------

ext_customer_path='/mnt/silver/dim_customer_stg'
df_final.write.format('delta').mode('append').option('path', ext_customer_path).saveAsTable('retta.dim_customer_stg')

# COMMAND ----------


