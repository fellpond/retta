# Databricks notebook source
#Add these values to cofig file

dbutils.widgets.text(name = 'energy_mnthly_tgt_stg_path',   defaultValue = '/mnt/silver/energy_mnthly_tgt_stg_path')
dbutils.widgets.text(name = 'energy_mnthly_ref_path', defaultValue = '/mnt/silver/energy_mnthly_ref')

# COMMAND ----------

#Read input arguments

energy_mnthly_tgt_stg_path=dbutils.widgets.get('energy_mnthly_tgt_stg_path')
energy_mnthly_ref_path = dbutils.widgets.get('energy_mnthly_ref_path')


# COMMAND ----------

#Read energy daily reading and reference values 
from pyspark.sql.window import Window
import pyspark.sql.functions as F
energy_mnthly_stg_df = spark.table('retta.energy_monthly_stg').filter("mnthly_sum is not null and box <> 'inbox'")
energy_mnthly_ref = spark.table('retta.energy_monthly_ref').filter("mnthly_sum is not null and box <> 'inbox'")



# COMMAND ----------

display(energy_mnthly_ref.filter('ean=20003317'))

display(energy_mnthly_stg_df.filter('ean=20003317'))

# COMMAND ----------

#Check if duplicate record on ean, mmthly_end_dt and mnthly_sum
#if duplicate found reject
#if mnthly_sum updated then update ref
#if mnthly_end_dt change then transform
#display(energy_mnthly_stg_df)
#display(energy_mnthly_ref)
energy_mnthly_ref= energy_mnthly_ref.withColumnRenamed('mnthly_sum', 'prev_mnthly_sum').drop('box')
df =energy_mnthly_stg_df.join(energy_mnthly_ref, on = ['ean','mnthly_end_dt'], how='leftouter')

df_update = df.filter("mnthly_sum <> prev_mnthly_sum").select('ean', 'mnthly_end_dt', 'mnthly_sum', 'box')
df_update.createOrReplaceTempView('monthly_ref_update')
df_xfm = df.filter("prev_mnthly_sum is null").drop('prev_mnthly_sum')


df_xfm = df_xfm.withColumn('rank', F.rank().over(Window.partitionBy('ean', 'mnthly_end_dt').orderBy(F.col('reading_dt').desc())))\
                .filter('rank = 1')\
                .drop('rank')

display(df_update.where("ean in ('20003317', '20003298')"))
display(df_xfm.where("ean in ('20003317', '20003298')"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into retta.energy_monthly_ref tgt using monthly_ref_update src on tgt.ean = src.ean
# MAGIC and tgt.mnthly_end_dt = src.mnthly_end_dt
# MAGIC when matched and tgt.mnthly_sum <> src.mnthly_sum then
# MAGIC update
# MAGIC   set tgt.mnthly_sum = src.mnthly_sum --select * from retta.energy_monthly_ref;

# COMMAND ----------


winSpec = Window.partitionBy('ean').orderBy('mnthly_end_dt')

energy_mnthly_ref = spark.table('retta.energy_monthly_ref').filter("mnthly_sum is not null and box <> 'inbox'")
df_xfm = df_xfm.drop('mnthly_strt_dt')
energy_df_int = df_xfm.withColumn('flag', F.lit(True))

df_union = energy_df_int.select("ean", "mnthly_end_dt", "mnthly_sum", "box", "flag").union(energy_mnthly_ref.withColumn('flag', F.lit(False)))

df_union = df_union.withColumn('mnthly_units', df_union['mnthly_sum'] - F.lag('mnthly_sum').over(winSpec))\
                    .withColumn('mnthly_strt_dt', F.lag('mnthly_end_dt').over(winSpec))


df_load = df_union.filter("flag==True").drop('flag')

#energy_mnthly_stg_df = energy_mnthly_stg_df.drop('mnthly_strt_dt')
df_final = df_xfm.join(df_load.select('ean', 'mnthly_strt_dt', 'mnthly_end_dt', 'mnthly_units'), on = ['ean', 'mnthly_end_dt'])

df_ref = df_union.withColumn('rank', F.rank().over(Window.partitionBy('ean').orderBy(F.col('mnthly_end_dt').desc())))\
                    .filter("rank == 1")\
                    .drop('flag', 'rank', 'mnthly_units','mnthly_strt_dt')


 
                             
                          

# COMMAND ----------

display(df_final.filter('ean =20003317 '))
display(df_ref.filter('ean =20003317 '))

# COMMAND ----------

df_final.write.format('Delta').mode('append').option('path', energy_mnthly_tgt_stg_path).saveAsTable('retta.energy_monthly_tgt_stg')
df_ref.write.format('delta').mode('overwrite').option('path', energy_mnthly_ref_path).saveAsTable('retta.energy_monthly_ref')
