# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
import common.common_functions as cf
enrgyHrlyPath='/mnt/bronze/energy_hrly/input/energy_hrly_*'



enrgyHrly_df = cf.readJson(spark, enrgyHrlyPath)

df=enrgyHrly_df.persist()

#Select the main node data rest are excluded
df = df.select('data')


df = df.withColumn('records', df['data'].getItem('records'))\
        .withColumn('offset', df.data.offset)\
        .drop('data')


df = df.withColumn('records_e', F.explode('records')).drop('records')

df = df.withColumn('meter_id', df['records_e'].getItem('meter').getItem('_id'))\
        .withColumn('ean', df['records_e'].getItem('meter').getItem('ean'))\
        .withColumn('values', F.explode(df['records_e'].getItem('values')))\
        .drop('records_e')

df = df.withColumn('granularity', df['values'].getItem('granularity'))\
        .withColumn('units', df['values'].getItem('value'))\
        .withColumn('metric', df['values'].getItem('metric'))\
        .withColumn('updated', df['values'].getItem('updated'))\
        .withColumn('period', df['values'].getItem('period'))\
        .withColumn('pushed_at', df['values'].getItem('pushed_at'))\
        .withColumn('source', F.lit('Metry'))\
        .drop('values')


df = df.persist()
df_offset = df.select('offset').dropDuplicates()


# COMMAND ----------

#Write offset data to storage
target='/mnt/bronze/energy_hrly/config/hourly_offset.csv'
offset_loc='/mnt/bronze/energy_hrly/config/offset.csv'

df_offset.write.mode('overwrite').option('path', target).option('header',True).format('csv').save()
[dbutils.fs.mv(items[0], offset_loc) for items in dbutils.fs.ls(target) if (items[0].endswith('csv'))]

# COMMAND ----------

#write hourly energy data to storage
hourly_stg = '/mnt/silver/energy_hrly'
df.write.format('delta')\
        .mode('append')\
        .option('path', hourly_stg)\
        .saveAsTable('retta.energy_hourly')
