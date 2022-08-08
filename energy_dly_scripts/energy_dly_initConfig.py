# Databricks notebook source
# MAGIC %run ../common/mount_adls

# COMMAND ----------

dbutils.widgets.text(name = 'input_path',   defaultValue = '/mnt/bronze/energy_dly_mnthly/input/')
dbutils.widgets.text(name = 'file_name', defaultValue = 'energy_dly*')
dbutils.widgets.text(name = 'dly_ref_path', defaultValue = '/mnt/silver/energy_dly_ref')
dbutils.widgets.text(name = 'mnthly_ref_path', defaultValue = '/mnt/silver/energy_mnthly_ref')

# COMMAND ----------

enrgyDlyPath=dbutils.widgets.get('input_path')

file_name = dbutils.widgets.get('file_name')
dly_ref_path = dbutils.widgets.get('dly_ref_path')
mnthly_ref_path = dbutils.widgets.get('mnthly_ref_path')

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
import common.common_functions as cf

 

enrgyDly_df = cf.readJson(spark,enrgyDlyPath + file_name)

df=enrgyDly_df

#Select the main node data rst are excluded
df = df.select('data')

#As data is array, using explode to create a row per array element
df = df.withColumn('data_e', F.explode('data')).drop('data')

#Fetch required keys from resulting object using getItem

df = df.withColumn('Metry_id', df['data_e'].getItem('_id'))\
        .withColumn('ean', df['data_e'].getItem('ean'))\
        .withColumn('box', df['data_e'].getItem('box'))\
        .withColumn('type', df['data_e'].getItem('type'))\
        .withColumn('consumption_stats', df['data_e'].getItem('consumption_stats'))\
        .drop('data_e')

#Comsumption_stats unfolding
df = df.withColumn('energy', df['consumption_stats'].getItem('energy')).drop('consumption_stats')
df = df.withColumn('daily',df['energy'].getItem('day'))\
        .withColumn('hour',df['energy'].getItem('hour'))\
        .withColumn('month',df['energy'].getItem('month')).drop('energy')


df = df.withColumn('dly_cnt', df['daily'].getItem('count'))\
        .withColumn('dly_strt_dt', df['daily'].getItem('first'))\
        .withColumn('dly_end_dt', df['daily'].getItem('last'))\
        .withColumn('dly_max', df['daily'].getItem('max'))\
        .withColumn('dly_min', df['daily'].getItem('min'))\
        .withColumn('dly_sum', df['daily'].getItem('sum'))\
        .withColumn('mnthly_cnt', df['month'].getItem('count'))\
        .withColumn('mnthly_strt_dt', df['month'].getItem('first'))\
        .withColumn('mnthly_end_dt', df['month'].getItem('last'))\
        .withColumn('mnthly_max', df['month'].getItem('max'))\
        .withColumn('mnthly_min', df['month'].getItem('min'))\
        .withColumn('mnthly_sum', df['month'].getItem('sum'))  

         

col = ('daily', 'month', 'hour')
df = df.drop(*col)


df = df.withColumn('dly_sum',df['dly_sum'].cast(DoubleType()))\
       .withColumn('dly_end_dt', F.to_date(df['dly_end_dt'].cast('string'), 'yyyyMMdd'))\
       .withColumn('mnthly_sum',df['mnthly_sum'].cast(DoubleType()))\
       .withColumn('mnthly_end_dt', F.to_date(df['mnthly_end_dt'].cast('string'), 'yyyyMM'))\


dly_ref = df.select('ean', 'dly_end_dt', 'dly_sum', 'box')
mnthly_ref = df.select('ean', 'mnthly_end_dt', 'mnthly_sum', 'box')



                   
dly_ref.write.format('delta').mode('overwrite').option('path', dly_ref_path).saveAsTable('retta.energy_dly_ref')
mnthly_ref.write.format('delta').mode('overwrite').option('path', mnthly_ref_path).saveAsTable('retta.energy_monthly_ref')



# COMMAND ----------

import common.common_functions as cf
cf.archiveProcess(dbutils, '/mnt/bronze/energy_dly_mnthly/input/','/mnt/bronze/energy_dly_mnthly/archive/test/')
