# Databricks notebook source
import common.common_functions as cf
#config=cf.load_config('/dbfs/mnt/bronze/energy_hrly/config/energy_hrly_config.json')

#Mount ADLS
dbutils.notebook.run('../common/mount_adls', 6000)

#Parse raw data 
dbutils.notebook.run('./energy_hourly_parser', 6000)

 

#Archive input files --parameterize it later
#cf.archiveProcess(dbutils, config['input'],config['archive'])
dbutils.notebook.run('./archive_src', 6000, {'input_dir': '/mnt/energy_dly_in/input/', 'archive_dir':'/mnt/energy_dly_in/Archive/'})
