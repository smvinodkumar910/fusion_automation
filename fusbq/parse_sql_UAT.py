import os
import glob
import re
import pandas as pd

path =r'C:\GitRepos\analytics-finance-gcp\fusion\bigquery\uat_deployment\views\\'

files = [path+a for a in  os.listdir(path)]

all_gl_tables =[]

for file in files:
    script_name = file.replace(path,'')
    
    with open(file,'r') as f:
        print('processing :'+script_name)
        sql = f.read()
        #sql = sql.replace('BQ_DM_UAT.','BQ_DM_GL.').replace('bq_dm_uat.','BQ_DM_GL.')
        #sql = sql.replace('BQ_ANALYTICS_UAT.','BQ_ANALYTICS_GL.').replace('bq_analytics_uat.','BQ_ANALYTICS_GL.')
        sql = sql.replace('BQ_STG.','BQ_STG_GL.').replace('bq_stg_uat.','BQ_STG_GL.')
    
    with open(file,'w') as f:
        f.write(sql)

    
        
    
    

#print(all_gl_tables)
#print(path+'ar.CSV')
#pd.DataFrame(all_gl_tables).drop_duplicates().to_csv(path+'ar.CSV')



    
