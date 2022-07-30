# Databricks notebook source
dbutils.widgets.text(name = 'energy_dly_tgt_stg_path',   defaultValue = '/mnt/silver/energy_dly_tgt_stg_path')
dbutils.widgets.text(name = 'energy_dly_ref_path', defaultValue = '/mnt/silver/energy_dly_ref')

# COMMAND ----------

#Read input arguments

energy_dly_tgt_stg_path=dbutils.widgets.get('energy_dly_tgt_stg_path')
energy_dly_ref_path = dbutils.widgets.get('energy_dly_ref_path')


# COMMAND ----------

#Read energy daily reading and reference values 

energy_dly_stg_df = spark.table('retta.energy_dly_stg').filter("dly_sum is not null and box <> 'inbox'")
energy_dly_ref = spark.table('retta.energy_dly_ref').filter("dly_sum is not null and box <> 'inbox'")

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F
winSpec = Window.partitionBy('ean').orderBy('dly_end_dt')
energy_df_int = energy_dly_stg_df.withColumn('flag', F.lit(True))

df_union = energy_df_int.select("ean", "dly_end_dt", "dly_sum", "box", "flag").union(energy_dly_ref.withColumn('flag', F.lit(False)))

df_union = df_union.withColumn('dly_units', df_union['dly_sum'] - F.lag('dly_sum').over(winSpec))\
                    .withColumn('dly_strt_dt', F.lag('dly_end_dt').over(winSpec))\
                    .dropDuplicates(['ean', 'dly_end_dt'])


df_load = df_union.filter("flag==True").drop('flag')

energy_dly_stg_df = energy_dly_stg_df.drop('dly_strt_dt')
df_final = energy_dly_stg_df.join(df_load.select('ean', 'dly_strt_dt', 'dly_end_dt', 'dly_units'), on = ['ean', 'dly_end_dt'])

df_ref = df_union.withColumn('rank', F.rank().over(Window.partitionBy('ean').orderBy(F.col('dly_end_dt').desc())))\
                    .filter("rank == 1")\
                    .drop('flag', 'rank', 'dly_units','dly_strt_dt')


 
                             
                          

# COMMAND ----------

df_final.printSchema()

# COMMAND ----------

df_final.write.format('Delta').mode('append').option('path', energy_dly_tgt_stg_path).saveAsTable('retta.energy_dly_tgt_stg')
df_ref.write.format('delta').mode('overwrite').option('path', energy_dly_ref_path).saveAsTable('retta.energy_dly_ref')
