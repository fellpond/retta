# Databricks notebook source
deltaTable.alias("events").merge(
    source = updatesDF.alias("updates"),
    condition = "events.eventId = updates.eventId"
  ).whenMatchedUpdate(set =
    {
      "data": "updates.data",
      "count": "events.count + 1"
    }
  ).whenNotMatchedInsert(values =
    {
      "date": "updates.date",
      "eventId": "updates.eventId",
      "data": "updates.data",
      "count": "1"
    }
  ).execute()

# COMMAND ----------

#This file imports JSON data from url and load and saves the data in a .csv file.

#Importing Libraries
from re import L
import requests
import numpy as np
import pandas as pd
import json

#function definition
def Load_write_csv(url,filename):
    
    '''
    This function takes url and filename as arguments and prints the first 2 lines of the csv file created from it.

    Aruguments are:
    url= The https link
    filename=Name of the final file
    
    '''

    #Loading json data from url
    result = requests.get(url)
    
    print(result.json())

    #print(type(result.text))
    #print(type(result.json()))

    #Create and Open new json file and dump the loaded data into it
    with open(f'{filename}.json', 'w') as json_file:
        json.dump(result.json(), json_file)

    #Opening Json file
    f=open(f'{filename}.json')

    #Reading json data into dataframe
    df = pd.read_json (f)

    #Save dataframe into a .csv file
    df.to_csv (f'{filename}.csv', index = None)

    #Printing first 2 lines of the file
    print(df.head(2))

    #End of function

#Variable declaration
url1 = "https://cat-fact.herokuapp.com/facts/"  #Provided url
File_name = "/dbfs/mnt/bronze/energy_dly_mnthly/temp/Cat_Data_facts"  #Final File name

#function calling
Load_write_csv(url1,File_name)




# COMMAND ----------

from common.common_functions import *

# COMMAND ----------

for item in dbutils.fs.ls('/mnt/bronze/energy_dly_mnthly/archive'):
    if (item[1].find('mnthly') != -1):
        dbutils.fs.cp(item[0], '/mnt/bronze/energy_dly_mnthly/input/')

# COMMAND ----------

'energy_dly_mnthly20220721'.find('tet')
