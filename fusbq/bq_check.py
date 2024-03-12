from google.cloud import bigquery
import pandas as pd
import json
import os
from google.cloud import bigquery
from fusbq.bq_ddl_automate.gcp_bq_table_create import create_procs
from fusbq.path_config import DataSets
pc = DataSets('common')


#client = bigquery.Client('helix-data-dev')

#table = client.get_table('helix-data-dev.helix_fin_ms_dw.DW_MS_EQX_POE_POF_MAPPING_BIP')
#print(table.schema)



# function to return date which 2 days prior to the last_execution date
def get_last_run_time_for_dag(PROJECT='project_name',
                              DATASET_NAME='dataset_name',
                              table='table',
                              dag_name='dag_name'):
    from google.cloud import bigquery
    from datetime import datetime, timezone, timedelta

    client = bigquery.Client(project=PROJECT)
    query_string = 'select CAST({column_name} AS DATE)-1 from \
            {project_id}.{dataset_id}.{table_name} \
                where batch_name={where_clause}'.format(
            column_name='tgt_update_dt',
            project_id=PROJECT,
            dataset_id=DATASET_NAME,
            table_name=table,
            where_clause=dag_name)
    print(query_string)
    job = client.query(query_string)
    
    print(job.result().total_rows)
    if job.result().total_rows>0:
        for row in job.result():
            return (row[0].strftime("%Y-%m-%d"))
    else:
        return "1900-01-01"

date_val = get_last_run_time_for_dag(PROJECT='helix-data-uat',
                                            DATASET_NAME='BQ_CTL_METADATA',
                                            table='BQ_CTL_BATCH_MASTER',
                                            dag_name='"fusion_finance_analytics_without_java"'
                                            )
print(date_val)

        