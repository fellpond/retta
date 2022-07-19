# Databricks notebook source
#Read energy daily reading and reference values 

energy_dly_stg_df = spark.table('retta.energy_monthly_stg').filter("dly_sum is not null")
energy_dly_ref = spark.table('retta.energy_dly_ref').filter("dly_sum is not null")

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as F

energy_df_int = energy_dly_stg_df.withColumn('flag', F.lit(True))
winSpec = Window.partitionBy('ean').orderBy('dly_end_dt')
df_union = energy_df_int.select("ean", "dly_end_dt", "dly_sum", "flag").union(energy_dly_ref.withColumn('flag', F.lit(False)))

df_union = df_union.withColumn('dly_units', df_union['dly_sum'] - F.lag('dly_sum').over(winSpec))\
                    .dropDuplicates(['ean', 'dly_end_dt'])


df_load = df_union.filter("flag==True").drop('flag')


df_final = energy_dly_stg_df.join(df_load.select('ean', 'dly_end_dt', 'dly_units'), on = ['ean', 'dly_end_dt'])

df_ref = df_union.withColumn('rank', F.rank().over(Window.partitionBy('ean').orderBy(F.col('dly_end_dt').desc())))\
                    .filter("rank == 1")\
                    .drop('flag', 'rank', 'dly_units')
 
                             
                             

# COMMAND ----------

df_final.write.format('Delta').mode('append').saveAsTable('retta.energy_daily_tgt_stg')
df_ref.write.format('Delta').mode('overwrite').saveAsTable('retta.energy_dly_ref')
