# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/Meter_metadata.json')

df = spark.read.csv('/mnt/bronze/sharepoint/input/Meter/*', header=True, sep=',', quote= "\"", escape="\"")
df_final = df.select(list(config.keys()))
# print(config.keys())

df_final = cf.check_for_json_value(df_final)

#display(df_final)
# df_final =df_final.withColumn('Buidling', F.get_json_object(F.col("Buidling"), "$.Value"))
# df_final =df_final.withColumn('Site', F.get_json_object(F.col("Site"), "$.Value"))
# df_final =df_final.withColumn('Metercatergory', F.get_json_object(F.col("Metercatergory"), "$.Value"))
# df_final =df_final.withColumn('Energytype', F.get_json_object(F.col("Energytype"), "$.Value"))
# df_final =df_final.withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))
# df_final =df_final.withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))


#rename columns
df_final = cf.df_column_rename(df_final,config)
display(df_final)
df.printSchema()

# COMMAND ----------

#load into stg table
ext_meter_path='/mnt/silver/dim_meter_stg'
df_final.write.format('delta').mode('append').option('path', ext_meter_path).saveAsTable('retta.dim_meter_stg')
