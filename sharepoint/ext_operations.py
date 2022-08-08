# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/Operations_metadata.json')

df = spark.read.csv('/mnt/bronze/sharepoint/input/operations/*', header=True, sep=',', quote= "\"", escape="\"")
df_final = df.select(list(config.keys()))
#print(config.keys())

df_final = cf.check_for_json_value(df_final)

# df_final =df_final.withColumn('Building', F.get_json_object(F.col("Building"), "$.Value"))
# df_final =df_final.withColumn('Status', F.get_json_object(F.col("Status"), "$.Value"))
# df_final =df_final.withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))
# df_final =df_final.withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))

# display(df)

#rename columns
df_final = cf.df_column_rename(df_final,config)
display(df_final)

df.printSchema()

# COMMAND ----------

df_final=df_final.filter(df_final.test_No!='2')
display(df_final)

# COMMAND ----------

ext_operations_path='/mnt/silver/dim_operations_stg'
df_final.write.format('delta').mode('append').option('path', ext_operations_path).saveAsTable('retta.dim_operations_stg')
