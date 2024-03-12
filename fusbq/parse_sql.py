import os
import glob
import re
import pandas as pd

path ='C:\\Users\\VinodKumarM\\Desktop\\Routines\\ar\\'

files = [path+a for a in  os.listdir(path)]

all_gl_tables =[]

for file in files:
    script_name = file.replace(path,'')
    with open(file,'r') as f:
        sql = f.read()
    matches = []
    matches.extend(re.findall('(BQ_DM).([A-Za-z_]*)',sql.upper()))
    matches.extend(re.findall('(BQ_ANALYTICS).([A-Za-z_]*)',sql.upper()))
    
    for m in matches:
        tables = dict()
        tables['script_name']=script_name
        tables['schema']=m[0]
        tables['table']=m[1]
        all_gl_tables.append(tables)
    
    

#print(all_gl_tables)
print(path+'ar.CSV')
pd.DataFrame(all_gl_tables).drop_duplicates().to_csv(path+'ar.CSV')



    
