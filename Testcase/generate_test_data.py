# Databricks notebook source
import os
import json
path=r'C:\Users\Prayank\Documents\Relia\Energy Data\test\\'
d = {}
l = os.listdir(path)
print(l)
for mon, item in enumerate(l):
    tmp = json.load(open(path+'\\' + item))
    print(str(tmp['data'][0]['consumption_stats'].keys()) + '         ' + item)
    
    for n in range(len(tmp['data'])):
        if (tmp['data'][n]['consumption_stats'] != {}):
            tmp['data'][n]['consumption_stats']['energy']['month']['last'] = '20220' + str(mon+1)
            tmp['data'][n]['consumption_stats']['energy']['month']['sum'] = 10000 + mon*10000
            tmp['data'][n]['consumption_stats']['energy']['month']['first'] = '202001'
    with open(path + 'Monthly\\' + item, "w+") as outfile:
        json.dump(tmp, outfile)
    

# COMMAND ----------

def gen_energy_test_data(ref, outpath,start_yr, num_years):
    d = {}
    file_name='energy_dly_'     
    tmp = json.load(open(ref))
    yr = start_yr
    
    for year in range(num_years):
        for mon in range(1,13):
            for n in range(len(tmp['data'])):
                if (tmp['data'][n]['consumption_stats'] != {}):
                    tmp['data'][n]['consumption_stats']['energy']['month']['last'] = str(yr) + str(mon)
                    tmp['data'][n]['consumption_stats']['energy']['month']['sum'] = 10000 + mon*10000
                    tmp['data'][n]['consumption_stats']['energy']['month']['first'] = str(yr).concat('01')
        with open(outpath + item, "w+") as outfile:
            json.dump(tmp, outfile+file_name+str(yr)+str(mon))
    
