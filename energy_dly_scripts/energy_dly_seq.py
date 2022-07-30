# Databricks notebook source
# MAGIC %md
# MAGIC ##Sequencer notebook - This notebook calls dependent notebooks

# COMMAND ----------

import common.common_functions as cf

#/load daily config file
config=cf.load_config('/dbfs/mnt/bronze/energy_dly_mnthly/config/energy_dly_config.json')

#Mount ADLS
dbutils.notebook.run('../common/mount_adls', 6000)

#Parse raw data 
dbutils.notebook.run('./enrgyDly_parser_adf', 6000, {'input_path': config['input'], 'file_name':config['file_name'] ,'daily_stg_path':config['energy_dly_stg'], 'mnthly_stg_path':config['energy_mnthly_stg'] })

#Process daily transformed data
dbutils.notebook.run('./energy_dly_xfm', 6000, {'energy_dly_tgt_stg_path': config['energy_dly_tgt_stg'], 'energy_dly_ref_path':config['energy_dly_ref']})

#Process monthly transformed data try multi-threading for parallelism
dbutils.notebook.run('../energy_mnthly_scripts/energy_mnthly_xfm', 6000, {'energy_mnthly_tgt_stg_path': config['energy_mnthly_tgt_stg_path'], 'energy_mnthly_ref_path':config['energy_mnthly_ref_path']})

#Archive input files 
cf.archiveProcess(dbutils, config['input'],config['archive'])
