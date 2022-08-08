# Databricks notebook source
df_site=spark.sql('select * from retta.dim_site_stg')
display(df_site)


# COMMAND ----------

import pyspark.sql.functions as F


# COMMAND ----------

import requests
from pyspark.sql.functions import udf

def rest_req_call(base_url,param):
    response = requests.get(base_url,param)
    print(response.json())
    return response.json()

def accu_loc_key(lat, lng):
    API_KEY='8DUFt2aT98g8GAPZmTfIZ8srP9Ezj6Gy'
    base_url='http://dataservice.accuweather.com/locations/v1/cities/geoposition/search'
    param = {
            'apikey':API_KEY,
            'q':lat+','+lng
    }
    loc_key = rest_req_call(base_url,param)['Key']
    return str(loc_key)
    

@udf("string")
def fetch_lat_lng(address):
    base_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
    API_Key = 'AIzaSyAW3VqCFbrUItN1-DNppVKW1-gTfZ1e2gw'
    param = {'address':address,
             'key':API_Key}
    resp =  rest_req_call(base_url,param)
    lat = str(resp['results'][0]['geometry']['location']['lat'])
    lng = str(resp['results'][0]['geometry']['location']['lng'])
    loc_key = accu_loc_key(lat,lng)
    print(lat,lng,loc_key)
    out = {'lat' : lat,
    'lng' : lng,
     'loc_key' : loc_key     }
    return str(out)




# COMMAND ----------

 import pyspark.sql.functions as F

df_site_stg=spark.sql("select a.* from retta.dim_site_stg a left join retta.dim_site_tgt_stg b  on a.site_id = b.site_id and trim(b.lat) = ''")
df = df_site_stg.withColumn('api_resp', fetch_lat_lng(df_site_stg.address))
df = df.withColumn('lat', F.get_json_object(df['api_resp'], "$.lat"))\
        .withColumn('lng', F.get_json_object(df['api_resp'], "$.lng"))\
        .withColumn('loc_key', F.get_json_object(df['api_resp'], "$.loc_key"))\
        .drop('api_resp')
display(df)

# COMMAND ----------

dim_site_tgt_stg='/mnt/silver/dim_site_tgt_stg'
df.write.mode("append").format("delta").option('path', dim_site_tgt_stg).saveAsTable('retta.dim_site_tgt_stg')

# COMMAND ----------

weather_loc_ds_path='/mnt/bronze/sharepoint/config/weather_loc_ds.csv'
weather_loc='/mnt/bronze/sharepoint/config/site_loc.csv'
df.select('site_id', 'loc_key').write.format('csv').mode('overwrite').option('path', weather_loc_ds_path).option('header',True).save()
[dbutils.fs.mv(items[0], weather_loc) for items in dbutils.fs.ls(weather_loc_ds_path) if (items[0].endswith('csv'))]
