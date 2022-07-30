# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/Meter_Relationship_metadata.json')

df = spark.read.csv('/mnt/bronze/sharepoint/input/Meter_Relationship/*', header=True, sep=',', quote= "\"", escape="\"")
#display(df)

df_final = df.select(list(config.keys()))
#print(config.keys())

df_final =df_final.withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))
df_final =df_final.withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))

#rename columns
df_final = cf.df_column_rename(df_final,config)

display(df_final)
df.printSchema()

# COMMAND ----------

ext_meter_relantion_path='/mnt/silver/dim_meter_relation_stg'
df_final.write.format('delta').mode('append').option('path', ext_meter_relantion_path).saveAsTable('retta.dim_meter_relation_stg')
