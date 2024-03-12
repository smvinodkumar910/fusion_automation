from google.cloud import bigquery
from fusbq.path_config import DataSets as pc
pc = pc('common')
import pandas as pd
import json
import os
from google.cloud import bigquery


client = bigquery.Client(pc.DATA_PROJECT_NAME)


print(pc.ROOT_DIR)


def bq_table_exists(table_id: str):
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except Exception as e:
        print("Table {} is not found.".format(table_id))
        return False
        

for filename in os.listdir(os.path.join(pc.ROOT_DIR,'helix_automation_output','ADHOC_JSON')):
    table_name = filename.split('.')[0]
    table_id = "{}.{}.{}".format(pc.DATA_PROJECT_NAME, pc.RAW_SCHEMA, table_name)
    if bq_table_exists(table_id):
        continue
    else:
        schema = client.schema_from_json(os.path.join(pc.ROOT_DIR,'helix_automation_output','ADHOC_JSON',filename))
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)   
        print("Table {} Created.".format(table_id))     
        

        