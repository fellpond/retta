# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/Operations_metadata.json')

df = spark.read.csv('/mnt/bronze/sharepoint/input/operations/*', header=True, sep=',', quote= "\"", escape="\"")
df_final = df.select(list(config.keys()))
#print(config.keys())

df_final =df_final.withColumn('Building', F.get_json_object(F.col("Building"), "$.Value"))
df_final =df_final.withColumn('Status', F.get_json_object(F.col("Status"), "$.Value"))
df_final =df_final.withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))
df_final =df_final.withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))

display(df)
df.printSchema()

# COMMAND ----------

display(df_final)

# COMMAND ----------


