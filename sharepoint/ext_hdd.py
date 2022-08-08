# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

#config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/site_metadata.json')

df=spark.read.csv('/mnt/bronze/sharepoint/input/hdd_ref/*', header=True, sep=',', quote= "\"", escape="\"")
display(df.select('field_' + str(1)))
