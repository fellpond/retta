# Databricks notebook source
# MAGIC %run ../common/mount_adls

# COMMAND ----------

def readJson(path):
    try:
        df = spark.read.json(path)
        return df
    except:
        raise Exception("No file present")

    
    
def archiveProcess(inpath, archive):
    fileList = dbutils.fs.ls(inpath)
    for i in fileList:
        dbutils.fs.mv(i[0], archive )

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
enrgyDlyPath='/mnt/bronze/energy_dly_mnthly/input/energy_dly*'



enrgyDly_df = readJson(enrgyDlyPath)

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



df = df.withColumn('mnthly_cnt', df['month'].getItem('count'))\
        .withColumn('mnthly_strt_dt', df['month'].getItem('first'))\
        .withColumn('mnthly_end_dt', df['month'].getItem('last'))\
        .withColumn('mnthly_max', df['month'].getItem('max'))\
        .withColumn('mnthly_min', df['month'].getItem('min'))\
        .withColumn('mnthly_sum', df['month'].getItem('sum'))  


         

col = ('daily', 'month', 'hour')
df = df.drop(*col)

df = df.withColumn('mnthly_sum',df['mnthly_sum'].cast(DoubleType()))\
        .withColumn('mnthly_strt_dt', F.to_date(df['mnthly_strt_dt'].cast('string'), 'yyyyMM'))\
        .withColumn('mnthly_end_dt', F.to_date(df['mnthly_end_dt'].cast('string'), 'yyyyMM'))\



df_ref = df.select('ean', 'mnthly_end_dt', 'mnthly_sum', 'box')
df_ref.persist()
path = '/mnt/silver/energy_mnthly_ref'
df_ref.write.format('delta').mode('overwrite').option('path', path).saveAsTable('retta.energy_monthly_ref')



# COMMAND ----------

archiveProcess('/mnt/bronze/energy_dly_mnthly/input/','/mnt/bronze/energy_dly_mnthly/archive')
