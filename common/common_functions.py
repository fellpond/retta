import json

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