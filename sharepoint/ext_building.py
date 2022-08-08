# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/building_metadata.json')

df = spark.read.csv('/mnt/bronze/sharepoint/input/Building/*', header=True, sep=',', quote= "\"", escape="\"")
df_final = df.select(list(config.keys()))
#print(config.keys())

df_final = cf.check_for_json_value(df_final)

# display(df)
# df_final =df_final.withColumn('Buildingtype', F.get_json_object(F.col("Buildingtype"), "$.Value"))\
#                     .withColumn('Heatingtype', F.get_json_object(F.col("Heatingtype"), "$.Value"))\
#                     .withColumn('Site_x003a__x0020_Briefcase', F.get_json_object(F.col("Site_x003a__x0020_Briefcase"), "$.Value"))\
#                     .withColumn('Editor', F.get_json_object(F.col("Editor"), "$.Email"))\
#                     .withColumn('Author', F.get_json_object(F.col("Author"), "$.Email"))


#df = df.withColumn('Buildingtype', F.json_tuple(F.col("Buildingtype"), "Value"))
#['ItemInternalId', 'Title', 'Site#Id', 'Buildingtype','Flooraream2', 'NetHeatedFloorAream20', 'Volumem3', 'NetHeatedFloorAream2']
#
df_final = cf.df_column_rename(df_final,config)
display(df_final)

# COMMAND ----------

display(df_final)
ext_building_path='/mnt/silver/dim_building_stg'
df_final.write.format('delta').mode('append').option('path', ext_building_path).saveAsTable('retta.dim_building_stg')
