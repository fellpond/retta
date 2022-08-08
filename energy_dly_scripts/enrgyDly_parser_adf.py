# Databricks notebook source
dbutils.widgets.text(name = 'input_path',   defaultValue = '/mnt/bronze/energy_dly_mnthly/input/')
dbutils.widgets.text(name = 'file_name', defaultValue = 'energy_dly*')
dbutils.widgets.text(name = 'daily_stg_path', defaultValue = '/mnt/silver/energy_dly_stg')
dbutils.widgets.text(name = 'mnthly_stg_path', defaultValue = '/mnt/silver/energy_mnthly_stg')

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
import common.common_functions as cf
enrgyDlyPath=dbutils.widgets.get('input_path')

file_name = dbutils.widgets.get('file_name')
daily_stg_path = dbutils.widgets.get('daily_stg_path')
monthly_stg_path = dbutils.widgets.get('mnthly_stg_path')

enrgyDly_df = cf.readJson(spark,enrgyDlyPath + file_name)

df=enrgyDly_df

#Select the main node data rst are excluded
df = df.select('data')



#As data is array, using explode to create a row per array element
df = df.withColumn('data_e', F.explode('data')).drop('data')
display(df)
# #Fetch required keys from resulting object using getItem

df = df.withColumn('Metry_id', df['data_e'].getItem('_id'))\
        .withColumn('ean', df['data_e'].getItem('ean'))\
        .withColumn('box', df['data_e'].getItem('box'))\
        .withColumn('type', df['data_e'].getItem('type'))\
        .withColumn('consumption_stats', df['data_e'].getItem('consumption_stats'))\
        .drop('data_e')

# #Comsumption_stats unfolding
df = df.withColumn('energy', df['consumption_stats'].getItem('energy')).drop('consumption_stats')
df = df.withColumn('daily',df['energy'].getItem('day'))\
        .withColumn('hour',df['energy'].getItem('hour'))\
        .withColumn('month',df['energy'].getItem('month')).drop('energy')


df_dly = df.withColumn('dly_cnt', df['daily'].getItem('count'))\
        .withColumn('dly_strt_dt', df['daily'].getItem('first'))\
        .withColumn('dly_end_dt', df['daily'].getItem('last'))\
        .withColumn('dly_max', df['daily'].getItem('max'))\
        .withColumn('dly_min', df['daily'].getItem('min'))\
        .withColumn('dly_sum', df['daily'].getItem('sum')) 
         

df_mnthly = df.withColumn('mnthly_cnt', df['month'].getItem('count'))\
        .withColumn('reading_dt', df['daily'].getItem('last'))\
        .withColumn('mnthly_strt_dt', df['month'].getItem('first'))\
        .withColumn('mnthly_end_dt', df['month'].getItem('last'))\
        .withColumn('mnthly_max', df['month'].getItem('max'))\
        .withColumn('mnthly_min', df['month'].getItem('min'))\
        .withColumn('mnthly_sum', df['month'].getItem('sum'))  




col = ('daily', 'month', 'hour')
df_dly = df_dly.drop(*col)
df_mnthly = df_mnthly.drop(*col)

df_dly = df_dly.withColumn('dly_sum',df_dly['dly_sum'].cast(DoubleType()))\
        .withColumn('dly_strt_dt', F.to_date(df_dly['dly_strt_dt'].cast('string'), 'yyyyMMdd'))\
        .withColumn('dly_end_dt', F.to_date(df_dly['dly_end_dt'].cast('string'), 'yyyyMMdd'))\


df_mnthly = df_mnthly.withColumn('mnthly_sum',df_mnthly['mnthly_sum'].cast(DoubleType()))\
        .withColumn('reading_dt', F.to_date(df_mnthly['reading_dt'].cast('string'), 'yyyyMMdd'))\
        .withColumn('mnthly_strt_dt', F.to_date(df_mnthly['mnthly_strt_dt'].cast('string'), 'yyyyMM'))\
        .withColumn('mnthly_end_dt', F.to_date(df_mnthly['mnthly_end_dt'].cast('string'), 'yyyyMM'))\
       
 

df_mnthly.drop('last')
df_dly.write.mode("overwrite").format("delta").option('path', daily_stg_path).saveAsTable('retta.energy_dly_stg')
df_mnthly.distinct().write.mode("overwrite").format("delta").option('path', monthly_stg_path).saveAsTable('retta.energy_monthly_stg')




# COMMAND ----------

# MAGIC %sql
# MAGIC select ean , count(1) from  retta.energy_monthly_stg group by ean;
