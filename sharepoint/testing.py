# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import common.common_functions as cf

config=cf.load_config('/dbfs/mnt/bronze/sharepoint/config/contract_metadata.json')
print(list(config.keys()))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retta.dim_site_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table df_add select address from retta.dim_site_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_add

# COMMAND ----------

df_te=sqlContext.sql("select * from retta.dim_site_stg")
# display(df_te)

js=df_te.select(F.col('contract_ID')).collect()[0][0]
print(type(js))
if(js==None):
    js=df_te.select(F.col('contract_ID')).collect()[1][0]
print(js)

# COMMAND ----------

df_ad1=sqlContext.sql("select address from retta.dim_site_stg")
df_ad1.show()

df_ad1_r=df_ad1.collect()

print(df_ad1_r[0][0])

df_ad1.count()

# COMMAND ----------

a=df_ad1.count()
for i in range(0,a):
    print(df_ad1_r[i][0])
    print('\n')

# COMMAND ----------

#url=''+a+''
#print(url)

print(df_ad1.select('address').count())

# COMMAND ----------

def extract_rows(colmn):
    a=colmn.count()
    column=colmn.collect()
    for i in range(0,a):
        print(column[i][0])
    
df_c1=sqlContext.sql("select address from retta.dim_site_stg")
extract_rows(df_c1)

# COMMAND ----------

df_ad1=sqlContext.sql("select address from retta.dim_site_stg")

# COMMAND ----------


apr={{'lat': '38.8976633', 'lng': '-77.0365739', 'loc_key': '2628393'}}
df_ad1 = df_ad1.withColumn('lat', F.get_json_object(apr, "$.lat"))\
        .withColumn('lng', F.get_json_object(apr, "$.lng"))\
        .withColumn('loc_key', F.get_json_object(apr, "$.loc_key"))

display(df_ad1)

# COMMAND ----------


