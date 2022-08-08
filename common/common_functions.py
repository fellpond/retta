import json
import pyspark.sql.functions as F

def copy_all_files_from_folder(source, target):
    for files in dbutils.fs.ls(source):
        dbutils.fs.cp(files[0].split(':')[1], target)

def load_config(path):
    return json.load(open(path,'r'))


def readJson(spark,path):
    try:
        df = spark.read.json(path)
        return df
    except:
        raise Exception("No file present")


def df_column_rename(df,metadata):
    for k,v in metadata.items():
        df=df.withColumnRenamed(k,v)
    return df
    
def archiveProcess(dbutils, inpath, archive):
    fileList = dbutils.fs.ls(inpath)
    for i in fileList:
        dbutils.fs.mv(i[0], archive )
        
def check_for_json_value(da_fram):
    for co in da_fram.columns:
        da_fram=getdata(da_fram,co)
    return da_fram

def getdata(dat_frm, col_name):
    key_config=load_config('/dbfs/mnt/bronze/sharepoint/config/input_columns_jsonkey.json')  
    if(col_name in key_config.keys()):
        key= key_config[col_name]
    else:
        key='Value'
    js=dat_frm.select(F.col(f'{col_name}')).collect()[0][0]
    if(js==None):
        js=dat_frm.select(F.col(f'{col_name}')).collect()[1][0]    
    if key in js:
        dat_frm =dat_frm.withColumn(f'{col_name}',F.get_json_object(F.col(f"{col_name}"),f"$.{key}"))  
    return dat_frm
