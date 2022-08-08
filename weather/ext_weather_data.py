# Databricks notebook source
import common.common_functions as cf

#df = cf.readJson(spark,'/mnt/bronze/weather/input/*.json')
df = spark.read.json('/mnt/bronze/weather/input/weather*')
display(df)

# COMMAND ----------

import pyspark.sql.functions as F

def exploding(colname,datfram):
    f=False
    previcol=datfram.columns[0]
    for col in datfram.columns:
        if(('struct' in datfram.select(f'{col}').dtypes[0][1]) or ('map' in datfram.select(f'{col}').dtypes[0][1])):
            colname=col
            f=True
            print(2)
            break
        previcol=col
    
    if(f==True):
        newdat=datfram.select(f'{previcol}',F.explode(f'{colname}'))
        print(3)
        exploding(previcol,newdat)
        datfram.join(newdat)
        
    return datfram.show()

exploding(df.columns[0],df)

# COMMAND ----------


