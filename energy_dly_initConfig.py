# Databricks notebook source
# MAGIC %run ./Mount_ADLS

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
enrgyDlyPath='/mnt/energy_dly_in/input/energy_dly*'



enrgyDly_df = readJson('/mnt/energy_dly_in/input/energy_dly*')

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
        .withColumn('dly_sum', df['daily'].getItem('sum')) 
         

col = ('daily', 'month', 'hour')
df = df.drop(*col)

df = df.withColumn('dly_sum',F.round(df['dly_sum'].cast(DoubleType()),2))\
        .withColumn('dly_strt_dt', F.to_date(df['dly_strt_dt'].cast('string'), 'yyyyMMdd'))\
        .withColumn('dly_end_dt', F.to_date(df['dly_end_dt'].cast('string'), 'yyyyMMdd'))\



df_ref = df.select('ean', 'dly_end_dt', 'dly_sum')
df_ref.persist()
df_ref.write.format('delta').mode('overwrite').saveAsTable('retta.energy_dly_ref')



# COMMAND ----------

archiveProcess('/mnt/energy_dly_in/input/','/mnt/energy_dly_in/Archive/')
