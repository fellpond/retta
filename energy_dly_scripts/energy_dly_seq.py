# Databricks notebook source
#Mount ADLS
dbutils.notebook.run('./Mount_ADlS', 6000)

#Parse raw data 
dbutils.notebook.run('./enrgyDly_parser_adf', 6000)

#Process transformed data
dbutils.notebook.run('./energy_dly_xfm', 6000)

#Archive input files --parameterize it later
dbutils.notebook.run('./archive_src', 6000, {'input_dir': '/mnt/energy_dly_in/input/', 'archive_dir':'/mnt/energy_dly_in/Archive/'})

# COMMAND ----------

dbutils.fs.ls('/user/hive/warehouse/energy_daily/')

# COMMAND ----------

dbutils.notebook.run('./Mount_ADlS', 6000)


# COMMAND ----------

dbutils.fs.ls('/Repos')
