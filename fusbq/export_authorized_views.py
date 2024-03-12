
from google.cloud import bigquery
import os
import sqlparse
from fusbq.path_config import DataSets
pc = DataSets('common')
import re
import glob


data_project_name = 'data-uat'
path = 'C:\\Users\\VinodKumarM\\Desktop\\VIEWS\\'

module_list=['ZX']

client = bigquery.Client(data_project_name)

for module in module_list:
    pc = DataSets(module)
    dataset_list = ['helix_ucm','helix_siebel','helix_singleview','helix_caplogix','helix_salesforce','helix_hyperion','helix_ibxmaster','helix_idq']
    #dataset_list = ["helix_fin_gbcc_raw_vw"]
    #print(dataset_list)
    for dataset_name in dataset_list:
        sql = " select table_name, ddl from data-uat.{dataset}.INFORMATION_SCHEMA.TABLES where 1=1 ".format(dataset = dataset_name)

        print(sql)

        df = client.query(sql).to_dataframe()
        dicts = df.to_dict(orient='records')

        for a in dicts:
            ddl = a['ddl']
            ddl = ddl.replace('data-uat','data-prod-209710')
            #ddl = ddl.replace('cloud-dw-poc','data-uat')
            viewname = re.findall("\`(.*?)\`",ddl)[0]
            #ddl = ddl.replace('`'+viewname+'`','`'+viewname+'` OPTIONS (labels=[label_1]) ')
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