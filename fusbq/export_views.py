
from google.cloud import bigquery
import os
import sqlparse
from fusbq.path_config import DataSets
pc = DataSets('common')
import re
import glob


data_project_name = 'helix-data-sit'
path = 'C:\\Users\\VinodKumarM\\Desktop\\VIEWS\\'

module_list=['TAX']

client = bigquery.Client(data_project_name)

for module in module_list:
    #pc = DataSets(module)
    #dataset_list = [pc.STG_SCHEMA, pc.HIST_SCHEMA, pc.RAW_SCHEMA, pc.DW_SCHEMA]
    dataset_list = ["helix_fin_tax_dm"]
    #print(dataset_list)
    for dataset_name in dataset_list:
        sql = " select table_name, ddl from helix-data-sit.{dataset}.INFORMATION_SCHEMA.TABLES where 1=1  ".format(dataset = dataset_name)

        print(sql)

        df = client.query(sql).to_dataframe()
        dicts = df.to_dict(orient='records')

        for a in dicts:
            ddl = a['ddl']
            ddl = ddl.replace('helix-data-sit','project_name')
            viewname = re.findall("\`(.*?)\`",ddl)[0]
            if re.search('\[([^\]]*)\]',ddl)==None:
                ddl = ddl.replace('`'+viewname+'`','`'+viewname+'` OPTIONS (labels=[label_1]) ')
            else:
                ddl = re.sub('\[([^\]]*)\]','[label_1]',ddl)
            
            ddl = ddl.replace('CREATE VIEW','CREATE OR REPLACE VIEW')
            os.makedirs(os.path.join(path, module), exist_ok=True)
            with open(os.path.join(path, module, a['table_name'] + '.sql'), 'w') as file:
                file.write(ddl)
'''
def create_merge_script(ddlroot: str):
    filenames = glob.glob(os.path.join(ddlroot, '*.sql'))
    #print(filenames)
    for file in filenames[2:3]:
        print(file)
        with open(file,'r') as sql:
            sqlstr = sql.read()
            print(sqlstr)
        


create_merge_script("C:\\Users\\VinodKumarM\\Desktop\\VIEWS\\MS\\")
'''