# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/contract_metadata.json')

df = spark.read.csv('/mnt/bronze/sharepoint/input/Contract/*', header=True, sep=',', quote= "\"", escape="\"")
df_final = df.select(list(config.keys()))
# print(config.keys())

df_final = cf.check_for_json_value(df_final)

# display(df)
# df_final =df_final.withColumn('Customer', F.get_json_object(F.col("Customer"), "$.Value"))
# df_final =df_final.withColumn('Contracttype', F.get_json_object(F.col("Contracttype"), "$.Value"))
# df_final =df_final.withColumn('Status', F.get_json_object(F.col("Status"), "$.Value"))
# df_final =df_final.withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))
# df_final =df_final.withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))
# # #VAETS missing

# rename columns
df_final = cf.df_column_rename(df_final,config)
display(df_final)

# df.printSchema()

# COMMAND ----------

display(df_final)


# COMMAND ----------

# ext_contract_path='/mnt/silver/dim_contract_stg'
# df_final.write.format('delta').mode('append').option('path', ext_contract_path).saveAsTable('retta.dim_contract_stg')
