# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/site_metadata.json')

df=spark.read.csv('/mnt/bronze/sharepoint/input/Site/*', header=True, sep=',', quote= "\"", escape="\"")


df_final = df.select(list(config.keys()))
# display(df)

df_final = cf.check_for_json_value(df_final)

# df_final =df_final.withColumn('Coontract', F.get_json_object(F.col("Coontract"), "$.Value"))\
#                     .withColumn('ReferenceMunicipality', F.get_json_object(F.col("ReferenceMunicipality"), "$.Value"))\
#                     .withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))\
#                     .withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))

df_final = cf.df_column_rename(df_final,config)

df_final = df_final.withColumn('address', F.concat(F.col('Street'), F.lit(','), F.col('City'), F.lit(','), F.col('State'), F.lit(','), F.col('PostalCode'), F.lit(','), F.col('CountryOrRegion')))

display(df_final)
df_final.printSchema()

# COMMAND ----------

ext_site_path='/mnt/silver/dim_site_stg'
df_final.write.format('delta').mode('append').option('path', ext_site_path).saveAsTable('retta.dim_site_stg')
