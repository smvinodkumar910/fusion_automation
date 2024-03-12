from google.cloud import bigquery
import os
import sqlparse

data_project_name = 'helix-data-sit'
path = 'C:\\Users\\VinodKumarM\\Desktop\\Routines\\'

client = bigquery.Client(data_project_name)
sql = """select specific_catalog, specific_schema, specific_name, ddl from  
      helix-data-sit.helix_fin_tax_dw.INFORMATION_SCHEMA.ROUTINES where 1=1
       AND specific_name in ('SP_PLP1_DW_TAX_AP_AR_FTR_DATA',
'SP_PLP2_DW_TAX_AP_AR_FTR_DATA'

    )"""

print(sql)

df = client.query(sql).to_dataframe()
dicts = df.to_dict(orient='records')

for a in dicts:
    ddl = a['ddl']
    #ddl = ddl.replace('`','')
    ddl = ddl.replace('helix-data-sit','project_name')
    ddl = ddl.replace('CREATE PROCEDURE','CREATE OR REPLACE PROCEDURE')
    #new_proc_name = 'SP_'+a['specific_name'][4:]
    #ddl = ddl.replace(a['specific_name'],new_proc_name)
    with open(os.path.join(path, a['specific_name'] + '.sql'), 'w') as file:
        #file.write(sqlparse.format(ddl, reindent=True, keyword_case='upper'))
        file.write(ddl)
        
