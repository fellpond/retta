# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/certificates_metadata.json')

df = spark.read.csv('/mnt/bronze/sharepoint/input/Certificates/*', header=True, sep=',', quote= "\"", escape="\"")
df_final = df.select(list(config.keys()))
print(config.keys())

df_final = cf.check_for_json_value(df_final)

# # display(df)
# df_final =df_final.withColumn('Certificatetype', F.get_json_object(F.col("Certificatetype"), "$.Value"))
# df_final =df_final.withColumn('Building', F.get_json_object(F.col("Building"), "$.Value"))
# df_final =df_final.withColumn('Breeamlevel', F.get_json_object(F.col("Breeamlevel"), "$.Value"))
# df_final =df_final.withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))
# df_final =df_final.withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))
# #Energy class missing

# rename columns
df_final = cf.df_column_rename(df_final,config)
display(df_final)
df.printSchema()

# COMMAND ----------

ext_certificates_path='/mnt/silver/dim_certificates_stg'
df_final.write.format('delta').mode('append').option('path', ext_certificates_path).saveAsTable('retta.dim_certificates_stg')


# COMMAND ----------


