
from google.cloud import bigquery
import os
import sqlparse
from fusbq.path_config import DataSets
pc = DataSets('common')
import re


data_project_name = 'helix-data-sit'
path = 'C:\\Users\\VinodKumarM\\Desktop\\TABLE_DDLS\\'

module_list=['FA']

client = bigquery.Client(data_project_name)

for module in module_list:
    pc = DataSets(module)
    #dataset_list = [pc.STG_SCHEMA, pc.HIST_SCHEMA, pc.RAW_SCHEMA]
    dataset_list = ["BQ_DM"]
    #print(dataset_list)
    for dataset_name in dataset_list:
        sql = """ select table_name, ddl from helix-data-sit.{dataset}.INFORMATION_SCHEMA.TABLES where 1=1 AND table_name IN 
        ('DM_GL_PRODUCT',
'DM_GL_COSTCENTER',
'DM_GL_ACCOUNT',
'DM_EXCHANGE_RATE_DR'
)
        """.format(dataset = dataset_name)

        print(sql)

        df = client.query(sql).to_dataframe()
        dicts = df.to_dict(orient='records')

        for a in dicts:
            ddl = a['ddl']
            ddl = ddl.replace('helix-data-sit','project_name')
            if re.search('\[([^\]]*)\]',ddl)==None:
                ddl = ddl.replace(';','\nOPTIONS (labels=[label_1]);')
            else:
                ddl = re.sub('\[([^\]]*)\]','[label_1]',ddl)
            ddl = ddl.replace('CREATE TABLE','CREATE TABLE IF NOT EXISTS')
            ddl = ddl.replace('.BQ_STG.','.BQ_STG_GL.')
            ddl = ddl.replace('.BQ_DM.','.BQ_DM_GL.')
            os.makedirs(os.path.join(path, module), exist_ok=True)
            with open(os.path.join(path, module, a['table_name'] + '.sql'), 'w') as file:
                file.write(ddl)
