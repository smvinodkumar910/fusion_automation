"""
############################################################################################################################
#EQUINIX CORPORATION - All Rights Reserved.
#---------------------------------------------------------------------------------------------------------------------------
#
#Script Name  : Fusion_PVO_to_Bq_ddl.py
#Purpose      : To create BQ DDL Scripts based on Fusion PVO metadata
#Version      : 1.0
#---------------------------------------------------------------------------------------------------------------------------
#Date            Updated By                Comments
#---------------------------------------------------------------------------------------------------------------------------
#19-Oct-2022     Vinodkumar Madhavan       Initial Creation
############################################################################################################################
"""

import os
import sys
import traceback

import pandas as pd
import requests
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from requests.auth import HTTPBasicAuth
from fusbq.bq_ddl_automate.ddl_creation import create_bq_ddl
import fusbq.path_config as pc
from airflow.utils.dates import days_ago
from airflow import models
from google.cloud import storage, bigquery
from google.cloud import secretmanager
from fusbq.bq_ddl_automate.gcp_bq_table_create import create_table_bq, create_procs, create_table_api

base_excel_path = pc.BASE_PVOS_PATH
target_ddl_file_path = os.path.join(pc.ROOT_DIR, pc.DDL_TGT_PATH)
gcs_target_ddl_file_path = pc.DDL_TGT_PATH
bicc_file_path = pc.BICC_FILE_PATH
project_name = pc.PROJECT_NAME
bucket_name = pc.BUCKET_NAME
meta_mode = 'api'
meta_path = ""
temp_path = pc.TEMP_PATH
meta_dataset_name = pc.META_DATASET_NAME
data_project_name = pc.DATA_PROJECT_NAME

api_url = pc.REST_API_URL
api_user = pc.REST_API_USER
api_secret = pc.REST_API_SECRET

# Creates target directory if not exists
os.makedirs(target_ddl_file_path, exist_ok=True)


def get_meta_data_api(data_store_name: str, table_name: str):
    client = secretmanager.SecretManagerServiceClient()
    name = 'projects/{}/secrets/{}/versions/latest'.format(project_name, api_secret)
    response = client.access_secret_version(name=name)
    password = response.payload.data.decode('UTF-8')
    response = requests.get(api_url + '/meta/datastores/' + data_store_name, auth=HTTPBasicAuth(api_user, password))
    responsejson = response.json()
    pvocollist = responsejson['columns']
    df = pd.DataFrame(pvocollist)
    coldf = df.rename(columns={'dataType': 'type'})
    coldf['data_store_name'] = data_store_name
    coldf['table_name'] = table_name
    return coldf


def get_meta_data_csv(datastore_name: str, table_name: str):
    file_name = datastore_name.lower().replace('.', '_')
    file_name = 'file_' + file_name + '.csv'

    metadf = pd.read_csv(pc.pvo_meta_path + file_name, index_col=False)

    # file_path = os.path.join(meta_path, file_name)
    # print('PROCESSING :' + file_path)
    # metadf = pd.read_csv(file_path, index_col=False)
    metadf['data_store_name'] = datastore_name
    metadf['table_name'] = table_name
    metadf = metadf.rename(columns={'Column Name': 'name', 'Data Type': 'type', 'Primary Key': 'isPrimaryKey',
                                    'Precision': 'precision', 'Scale': 'scale', 'Incremental Key': 'isLastUpdateDate'})
    metadf['isPrimaryKey'] = metadf['isPrimaryKey'].map({'Yes': True, 'No': False})
    metadf['isLastUpdateDate'] = metadf['isLastUpdateDate'].map({'Yes': True, 'No': False})
    return metadf


def task_success(context):
    """task success steps"""
    query_stmt = """update {}.{}.FUSION_TO_BQ_PVO_DTL set RUN_DDL_FLAG=false
     where RUN_DDL_FLAG=true """.format(data_project_name, meta_dataset_name)

    print("Query for task_success" + query_stmt)
    bq_client = bigquery.Client(project=pc.DATA_PROJECT_NAME)
    bq_client.query(query_stmt).result()


@task(task_id="DDL_CREATE")
def ddl_create():
    try:
        client = bigquery.Client(data_project_name)
        sql = 'select TABLE_ID,SOURCE_TABLE_NAME,DATASTORE_NAME,STG_TABLE_NAME,' \
              'RAW_TABLE_NAME, DW_TABLE_NAME,HIST_TABLE_NAME ,RAW_SIL_PROC, DW_SIL_PROC,RUN_DDL_FLAG,MODULE , ' \
              'STG_DEL_TABLE_NAME' \ 
              'from {}.{}.FUSION_TO_BQ_PVO_DTL ' \
              'WHERE RUN_DDL_FLAG=true ORDER BY TABLE_ID ASC '.format(data_project_name, meta_dataset_name)

        pvodf = client.query(sql).to_dataframe()
        pvodict = pvodf.to_dict('records')

        # bicc_blob = bucket.get_blob(bicc_file_path)
        # bicc_target_path = os.path.join(pc.ROOT_DIR, 'BICC_FSCM_Database_Mapping_with_ViewObjects.csv')
        # bicc_blob.download_to_filename(bicc_target_path)
        datadictionarydf = pd.read_csv(bicc_file_path)

        datastores = pvodf['DATASTORE_NAME'].dropna().to_list()
        print(datastores)

        datadictionarydf = datadictionarydf[datadictionarydf['View Object'].isin(datastores)]

        for a in pvodict:
            print('Started Processing ' + a['SOURCE_TABLE_NAME'])
            # Loop thru dict get metadata from Rest API and generate ddl
            if meta_mode == 'api':
                metadf = get_meta_data_api(a['DATASTORE_NAME'], a['SOURCE_TABLE_NAME'])
            # Loop thru dict get metadata from CSV file and generate ddl
            elif meta_mode == 'csv':
                metadf = get_meta_data_csv(a['DATASTORE_NAME'], a['SOURCE_TABLE_NAME'])

            # Join BICC file & meta data to get raw_table_column names
            merged_df = pd.merge(metadf, datadictionarydf, how='left',
                                 left_on=['data_store_name', 'name'],
                                 right_on=['View Object', 'View Object Attribute'])
            merged_df = merged_df.rename(columns={'Database Column': 'raw_column_name'})
            merged_df = merged_df.loc[:,
                        ['name', 'type', 'isPrimaryKey', 'isLastUpdateDate', 'precision', 'scale', 'table_name',
                         'raw_column_name']]
            # merged_df.to_csv(os.path.join(target_ddl_file_path, 'merged_df.csv'))

            # Call the create_bq_ddl method to create ddl scripts
            create_bq_ddl(merged_df, a['SOURCE_TABLE_NAME'], a['STG_TABLE_NAME'], a['RAW_TABLE_NAME'],
                          a['DW_TABLE_NAME'], a['HIST_TABLE_NAME'],a['STG_DEL_TABLE_NAME'],
                          target_ddl_file_path)

            print('DDL Creation completed for ' + a['SOURCE_TABLE_NAME'])

    except Exception as e:
        print(type(e))  # the exception instance
        print(e.args)  # arguments stored in .args
        traceback.print_exc()
        sys.exit(1)


@task(task_id='create_stg_tables')
def create_stg_tables(table_type: str):
    from google.cloud import storage
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=temp_path + table_type + '/')
    for a in blob_list:
        # blob = bucket.get_blob(a.name)
        # script = blob.download_as_string()
        # Creating stg table
        # create_table_bq(str(script, 'UTF-8'))
        path = a.name.split('/')
        table_name = path[-1][0:-4]
        create_table_api(table_name, table_type)


@task(task_id='create_hist_tables')
def create_hist_tables(table_type: str):
    from google.cloud import storage
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=temp_path + table_type + '/')
    for a in blob_list:
        # blob = bucket.get_blob(a.name)
        # script = blob.download_as_string()
        # Creating stg table
        # create_table_bq(str(script, 'UTF-8'))
        path = a.name.split('/')
        table_name = path[-1][0:-4]
        create_table_api(table_name, table_type)


@task(task_id='create_raw_tables')
def create_raw_tables(table_type: str):
    from google.cloud import storage
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=temp_path + table_type + '/')
    for a in blob_list:
        # blob = bucket.get_blob(a.name)
        # script = blob.download_as_string()
        # Creating stg table
        # create_table_bq(str(script, 'UTF-8'))
        path = a.name.split('/')
        table_name = path[-1][0:-4]
        create_table_api(table_name, table_type)

'''
@task(task_id='create_raw_sil_scripts')
def create_raw_sil_scripts():
    from google.cloud import storage
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=temp_path + 'RAW_PROCs/')
    for a in blob_list:
        blob = bucket.get_blob(a.name)
        script = blob.download_as_string()
        print(a.name)
        # Creating RAW procs
        create_procs(str(script, 'UTF-8'))
'''

@task(task_id='create_stg_del_tables')
def create_stg_del_tables():
    from google.cloud import storage
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=temp_path + 'STG_DEL/')
    for a in blob_list:
        blob = bucket.get_blob(a.name)
        script = blob.download_as_string()
        # Creating stg del tables
        create_table_bq(str(script, 'UTF-8'))


@task(task_id='create_raw_del_plp_scripts')
def create_raw_del_plp_scripts():
    from google.cloud import storage
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=temp_path + 'RAW_DEL_PLP_PROCs/')
    for a in blob_list:
        blob = bucket.get_blob(a.name)
        script = blob.download_as_string()
        # Creating RAW procs
        create_procs(str(script, 'UTF-8'))


@task(task_id='prep_step')
def delete_temp_gcs():
    client = storage.Client(project_name)
    bucket = client.get_bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=temp_path)
    for a in blob_list:
        blob = bucket.get_blob(a.name)
        blob.delete()


move_ddl_files = GCSToGCSOperator(
    task_id="move_ddl_files",
    source_bucket=bucket_name,
    source_object=temp_path,
    destination_bucket=bucket_name,
    destination_object=gcs_target_ddl_file_path
)

tables_type = ['STG', 'RAW']

with models.DAG(
        "fusion_automation",
        schedule_interval=None,
        start_date=days_ago(1)
) as dag:
    DDL_CREATE = ddl_create()
    create_stg_tables = create_stg_tables('STG')
    create_raw_tables = create_raw_tables('RAW')
    create_hist_tables = create_hist_tables('HIST')
    #create_raw_sil_scripts = create_raw_sil_scripts()
    create_stg_del_tables = create_stg_del_tables()
    create_raw_del_plp_scripts = create_raw_del_plp_scripts()
    prep_step = delete_temp_gcs()

    tables_creation_check = EmptyOperator(
        task_id="tables_creation_check",
        trigger_rule='all_success',
        dag=dag)

    post_job_step = EmptyOperator(
        task_id="post_job_step",
        trigger_rule='all_success',
        on_success_callback=task_success,
        dag=dag)

prep_step >> DDL_CREATE >> [create_stg_tables, create_raw_tables, create_hist_tables,
                            create_stg_del_tables] >> tables_creation_check \
>> create_raw_del_plp_scripts >> post_job_step >> move_ddl_files

