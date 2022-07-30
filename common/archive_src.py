# Databricks notebook source
dbutils.widgets.text('input_dir', defaultValue = "")
dbutils.widgets.text('archive_dir',  defaultValue = "")

# COMMAND ----------

def archiveProcess(inpath, archive):
    fileList = dbutils.fs.ls(inpath)
    for i in fileList:
        dbutils.fs.mv(i[0], archive )
        
archiveProcess(dbutils.widgets.get('input_dir'), dbutils.widgets.get('archive_dir'))
