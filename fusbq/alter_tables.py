from google.cloud import bigquery
import os
import sqlparse

data_project_name = 'helix-data-dev'
path = r'C:\Users\VinodKumarM\OneDrive - EQUINIX\Projects\fusion_converstion\gbcc_dw_ddl_changes\ALTER_TABLE_2.sql'

client = bigquery.Client(data_project_name)
with  open(path,'+r') as file:
    alterscripts = file.readlines()
    print(len(alterscripts))

for a in alterscripts:
    job = client.query(a) 
    
    print(a +":completed")

